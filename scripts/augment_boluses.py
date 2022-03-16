'''Checks for boluses and/or non_boluses if hr and invasive map are available during observation period'''

import pandas as pd
import numpy as np
import os
import math
import sys
import datetime
import pdb
import argparse
import pytz
import glob

from pyspark.sql import functions as sf

sys.path.append('/cluster/home/faltysm/source/2020_VolumeChallenge/common')
from spark_common import get_spark_session
from spark_common import get_included_subset


def get_stay_information(patientid, df_stay_data):
    #reference point for relative dataframe
    stay = df_stay_data[df_stay_data.patientid==patientid].iloc[0]
    rel_referencepoint = stay.rel_referencepoint
    lenght_of_stay = stay.lenght_of_stay
    lenght_of_stay_period_count = math.ceil(lenght_of_stay / TIME_PERIOD_LENGHT)

    return rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count


def process_patient(patientid, df_stay_data, df_bolus, df_monvals):
    print (patientid)
    rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count = get_stay_information(patientid, df_stay_data)
    screening_time = datetime.timedelta(0,SCREENING_TIME)
    
    df_boluses = pd.DataFrame()
    for index, bolus in df_bolus[df_bolus.patientid == patientid].iterrows():
        bolus_start = rel_referencepoint + datetime.timedelta(0, int(bolus.startindex * TIME_PERIOD_LENGHT))
        bolus_end = bolus_start + datetime.timedelta(0,int(bolus.lenght * TIME_PERIOD_LENGHT))
        
        start_mon_time = bolus_start - screening_time
        end_mon_time = bolus_end + screening_time
        
        df_pat_monvar = df_monvals.loc[df_monvals.PatientID == patientid, ["Datetime", "vm1", "vm5"]].sort_values('Datetime')
        df_pat_monvar_bolus_period = df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime <= end_mon_time)]
        
        for variableid in mon_value_ids:
            bolus[f"{variableid}_criterion"] = True
            max_gap_internal = df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period[variableid] > 0, "Datetime"].diff().max().seconds
            starting_gap = (df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period[variableid] > 0, "Datetime"].min() - start_mon_time).seconds
            ending_gap = (end_mon_time - df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period[variableid] > 0, "Datetime"].max()).seconds
            max_gap = np.max([max_gap_internal, starting_gap, ending_gap])
            if max_gap >= TIME_PERIOD_LENGHT or max_gap_internal == np.datetime64('NaT') or math.isnan(max_gap_internal):
                bolus[f"{variableid}_criterion"] = False
            
        df_boluses = df_boluses.append(bolus)

    return df_boluses

def process_batch(batchid, patientid_start, patientid_stop):
    print (f'Processing batch id: {batchid}: {patientid_start}-{patientid_stop}')
    #open spark session
    spark = get_spark_session(8, 1024, 64)
    
    cand_file=glob.glob(os.path.join(HIRID2_META_PATH,"reduced_fmat_{}_*.h5".format(batchid)))[0]
    
    df_bolus = spark.read.parquet(ISOBOLUS_PATH).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_stay_data = spark.read.parquet(PATIENT_STAY_PATH).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_monvals = pd.read_hdf(os.path.join(HIRID2_META_PATH, cand_file), columns=['PatientID', 'Datetime'] + mon_value_ids, where="PatientID in [" + ','.join(map(str, df_bolus.patientid.unique())) + "]")
    
    df_outcomes = pd.DataFrame()
    for patientid in df_bolus.patientid.unique():
        boluses_pat = process_patient(patientid, df_stay_data, df_bolus, df_monvals)
        if len(boluses_pat)>0:
            df_outcomes = df_outcomes.append(boluses_pat, ignore_index=True)
    
    return df_outcomes

if __name__=="__main__": 
    parser=argparse.ArgumentParser()
    # CONSTANTS 
    PATIENT_STAY_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay/2020-11-12"
    ISOBOLUS_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/bolus_isolated/2020-12-05"
    HIRID2_META_PATH="/cluster/work/grlab/clinical/hirid2/research/3_merged/v8/reduced"
    HIRID2_PATH="/cluster/work/grlab/clinical/hirid2/pg_db_export"
    ID_PATH = '/cluster/work/grlab/clinical/hirid2/research/misc_derived/id_lists/PID_all_vars_chunking_100.csv'
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
 
    TIME_PERIOD_LENGHT = 300
    SCREENING_TIME = 3600

    # Input paths
    parser.add_argument("--bolus_path", default=ISOBOLUS_PATH, help="Path to boluses")
    parser.add_argument("--batchid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--batchid_stop", help="Last patient id to be processed [including]", type=int)
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")

    args=parser.parse_args()
    ISOBOLUS_PATH = args.bolus_path

    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_AUGMENT_BOLUSES_{}_{}.stdout".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_AUGMENT_BOLUSES_{}_{}.stderr".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')

    mon_value_ids = ["vm1", "vm5"]
    spark = get_spark_session(8, 1024, 64)

    df_batches = pd.read_csv(ID_PATH)
    for batchid in range(args.batchid_start,args.batchid_stop+1):
        df_outcomes = pd.DataFrame() 
        pids_batch = df_batches[df_batches.BatchID == batchid].PatientID
        df_pat_outcomes = process_batch(batchid, pids_batch.min(), pids_batch.max())
        df_outcomes = df_outcomes.append(df_pat_outcomes)

        output_dir = os.path.join(ISOBOLUS_PATH, 'augmented', datetime.datetime.today().strftime('%Y-%m-%d'))
        if not os.path.exists(os.path.join(ISOBOLUS_PATH, 'augmented')): 
            os.makedirs(os.path.join(ISOBOLUS_PATH, 'augmented'))
        if not os.path.exists(output_dir): 
            os.makedirs(output_dir)

        df_outcomes.to_parquet(os.path.join(output_dir, "bolus_augmented_{}.parquet".format(batchid)))
    
    print ("success")

