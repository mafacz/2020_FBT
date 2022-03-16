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

def process_batch(batchid):
    print (batchid)
    df_batches = pd.read_csv(ID_PATH)
    cand_file=glob.glob(os.path.join(HIRID2_META_PATH,"reduced_fmat_{}_*.h5".format(batchid)))[0]
    pids_batch = df_batches[df_batches.BatchID == batchid].PatientID.tolist()
    pids = spark.read.parquet(BOLUS_PATH).where(sf.col("patientid").isin(pids_batch)).select("patientid").distinct().toPandas().patientid.values.tolist()
    df_bolus = spark.read.parquet(BOLUS_PATH).where(sf.col("patientid").isin(pids)).toPandas()
    df_monvals = pd.read_hdf(os.path.join(HIRID2_META_PATH, cand_file), columns=['PatientID', 'Datetime'] + MON_VALUE_IDS, where="PatientID in [" + ','.join(map(str, pids)) + "]")
    df_stay_data = spark.read.parquet(PATIENT_STAY_PATH).where(sf.col("patientid").isin(pids)).toPandas()

    for idx, bolus in df_bolus.iterrows():
        stay = df_stay_data[df_stay_data.patientid == bolus.patientid]
        rel_referencepoint = stay.iloc[0].rel_referencepoint

        bolus_start = rel_referencepoint + datetime.timedelta(0, int(bolus.startindex * TIME_PERIOD_LENGHT))
        bolus_end = bolus_start + datetime.timedelta(0,int(bolus.lenght * TIME_PERIOD_LENGHT))

        #check for betablocker usage
        screening_time = datetime.timedelta(0,60*24*60)
        start_mon_time = bolus_start - screening_time
        end_mon_time = bolus_end + datetime.timedelta(0,60*60)

        df_pat_monvar = df_monvals.loc[df_monvals.PatientID == bolus.patientid, ["Datetime", "pm51"]].sort_values('Datetime')
        df_pat_monvar_bolus_period = df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime <= end_mon_time)]

        df_bolus.loc[idx, "dm_betablocker"] = df_pat_monvar_bolus_period.pm51.any()
        
    #save result   
    if not os.path.exists(output_dir): 
        os.makedirs(output_dir)

    df_bolus.to_parquet(os.path.join(output_dir, "bolus_augmented_{}.parquet".format(batchid)))


if __name__=="__main__": 
    parser=argparse.ArgumentParser()
    # CONSTANTS 
    spark = get_spark_session(8, 1024, 64)

    PATIENT_STAY_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay/2020-11-12"
    BOLUS_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/psm_cardiac/2020-12-30"
    HIRID2_META_PATH="/cluster/work/grlab/clinical/hirid2/research/3_merged/v8/reduced"
    ID_PATH = '/cluster/work/grlab/clinical/hirid2/research/misc_derived/id_lists/PID_all_vars_chunking_100.csv'
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
    TIME_PERIOD_LENGHT=5
    MON_VALUE_IDS = ["pm51"]

    output_dir = os.path.join(BOLUS_PATH, 'augmented', datetime.datetime.today().strftime('%Y-%m-%d'))

    # Input paths
    parser.add_argument("--batchid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--batchid_stop", help="Last patient id to be processed [including]", type=int)
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")

    args=parser.parse_args()

    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_AUGMENT_BOLUSES_PPSM_{}_{}.stdout".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_AUGMENT_BOLUSES_PPSM_{}_{}.stderr".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')

    df_batches = pd.read_csv(ID_PATH)
    for batchid in range(args.batchid_start,args.batchid_stop+1):
        process_batch(batchid)
    
    print ("success")



