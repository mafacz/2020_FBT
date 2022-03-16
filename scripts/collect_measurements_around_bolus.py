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
from sklearn.linear_model import LinearRegression

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

    return rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count, stay.age_at_admission, stay.sex, stay.emergency, stay.height, stay.adm_diag, stay.adm_codeid, stay.apacheII, stay.sepsis


def process_patient(patientid, df_stay_data, df_bolus, df_monvals, result_columns):
    print (patientid)
    rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count, age, sex, emergency, height, adm_diag, adm_codeid, apacheII, sepsis = get_stay_information(patientid, df_stay_data)
    
    df_outcomes = pd.DataFrame(columns = result_columns)
    for index, bolus in df_bolus[df_bolus.patientid == patientid].iterrows():
        bolus_start = rel_referencepoint + datetime.timedelta(0, int(bolus.startindex * TIME_PERIOD_LENGHT))
        bolus_end = bolus_start + datetime.timedelta(0,int(bolus.lenght * TIME_PERIOD_LENGHT))
        
        df_pat_monvar = df_monvals[df_monvals.PatientID == patientid]
        
        results = [patientid, bolus.bolusid]
        #all monitored values
        for variableid in mon_value_ids:
            baseline_time = datetime.timedelta(0,VARIABLE_PERIODS_BASELINE[variableid])
            start_mon_time = bolus_start - baseline_time

            #calculate baseline as avg of all values in baseline time
            baseline = np.nanmean(df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start)][variableid])
            results.append(baseline)

            #calculate trend for trend variables
            if variableid in TREND_VARS:                
                x = (df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start) & (df_pat_monvar[variableid].notnull())]['Datetime']-start_mon_time).to_numpy() / (10**9) #10^9 converts it to seconds
                y = df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start) & (df_pat_monvar[variableid].notnull())][variableid].to_numpy()
                
                if len(x) > 0:
                    reg = LinearRegression().fit(x.reshape(-1, 1), y)
                    trend = reg.coef_[0]
                else:
                    print (f'trend error patientid:{patientid}')
                    trend = 0
                results.append(trend)

            #calculate outcomes for each period up to 1h post bolus end
            period_lenght = VARIABLE_PERIODS_OUTCOMES[variableid]
            for i in range(0, OUTCOME_OBSERVATION_TIME, period_lenght):
                period_start = bolus_end + datetime.timedelta(0,i)
                period_end = bolus_end + datetime.timedelta(0,i + period_lenght)
                period = np.nanmean(df_pat_monvar[(df_pat_monvar.Datetime >= period_start) & (df_pat_monvar.Datetime < period_end)][variableid])
                results.append(period)

        #all last values in intervall
        for variableid in last_values:
            baseline_time = datetime.timedelta(0,VARIABLE_PERIODS_BASELINE[variableid])
            start_mon_time = bolus_start - baseline_time

            #calculate baseline as avg of all values in baseline time
            if not df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start) & (df_pat_monvar[variableid].notna())].empty:
                baseline=df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start) & (df_pat_monvar[variableid].notna())].sort_values(by=['Datetime'], ascending=False).iloc[0][variableid]
                results.append(baseline)
            else:
                results.append(np.nan)

            #calculate outcomes for each period up to 1h post bolus end if outcome variable
            if variableid in VARIABLE_PERIODS_OUTCOMES:
                period_lenght = VARIABLE_PERIODS_OUTCOMES[variableid]
                for i in range(0, OUTCOME_OBSERVATION_TIME, period_lenght):
                    period_start = bolus_end + datetime.timedelta(0,i)
                    period_end = bolus_end + datetime.timedelta(0,i + period_lenght)
                    period = np.nanmean(df_pat_monvar[(df_pat_monvar.Datetime >= period_start) & (df_pat_monvar.Datetime < period_end)][variableid])
                    results.append(period)

        #all pharma values - special is 0 if nan
        for variableid in pharma_ids:
            baseline_time = datetime.timedelta(0,VARIABLE_PERIODS_BASELINE[variableid])
            start_mon_time = bolus_start - baseline_time

            #calculate baseline as avg of all values in baseline time
            baseline = np.nanmean(df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < bolus_start)][variableid])
            if baseline != np.nan:
                results.append(baseline)
            else:
                results.append(0)

            #calculate outcomes for each period up to 1h post bolus end
            period_lenght = VARIABLE_PERIODS_OUTCOMES[variableid]
            for i in range(0, OUTCOME_OBSERVATION_TIME, period_lenght):
                period_start = bolus_end + datetime.timedelta(0,i)
                period_end = bolus_end + datetime.timedelta(0,i + period_lenght)
                period = np.nanmean(df_pat_monvar[(df_pat_monvar.Datetime >= period_start) & (df_pat_monvar.Datetime < period_end)][variableid])
                if period != np.nan:
                    results.append(period)
                else:
                    results.append(0)

        #all static values
        results.append(age)
        results.append(sex)
        results.append(emergency)
        results.append(height)
        results.append(adm_diag)
        results.append(adm_codeid)
        results.append(apacheII)
        results.append(sepsis)

        #determin if patient is ventilated
        start_mon_time = bolus_start - datetime.timedelta(0,OUTCOME_OBSERVATION_TIME)
        end_mon_time = bolus_end + datetime.timedelta(0,OUTCOME_OBSERVATION_TIME)
        df_pat_monvar_bolus_period = df_pat_monvar[(df_pat_monvar.Datetime >= start_mon_time) & (df_pat_monvar.Datetime < end_mon_time)]

        etco2_meas = df_pat_monvar_bolus_period["vm21"]
        vent_group_meas = df_pat_monvar_bolus_period["vm306"]

        #check if patient has invasive ventilation settings on ventilator
        non_invasive_vent = False
        if 4 in vent_group_meas or 5 in vent_group_meas or 6 in vent_group_meas:
            non_invasive_vent = True

        if (etco2_meas>0.5).any() and non_invasive_vent==False:
            #patient is ventilated at some point during the period, estimate if for full period
            max_gap_internal = df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period["vm21"] > 0, "Datetime"].diff().max().seconds
            starting_gap = (df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period["vm21"] > 0, "Datetime"].min() - start_mon_time).seconds
            ending_gap = (end_mon_time - df_pat_monvar_bolus_period.loc[df_pat_monvar_bolus_period["vm21"] > 0, "Datetime"].max()).seconds
            max_gap = np.max([max_gap_internal, starting_gap, ending_gap])
            if max_gap >= 1800:
                results.append(2) #partial ventilation
            else:
                results.append(1) #full period
        else:
            results.append(0) #no invasive ventilation

        df_outcomes = df_outcomes.append(pd.Series(results, index=result_columns), ignore_index=True)
     
    return df_outcomes

def filter_mon_vals(df_monvals):
    #deletes all bloodpressures where syst,dia > 160 and all ~equal and within 30sek
    for idx, row in df_monvals[df_monvals.vm4 > 160].iterrows():
        period_end = row.Datetime + datetime.timedelta(0,30)
        period_start = row.Datetime - datetime.timedelta(0,30)
        patient_rows = df_monvals[df_monvals.PatientID == row.PatientID]
        rows_in_prox = patient_rows[(patient_rows.Datetime >= period_start) & (patient_rows.Datetime <= period_end)]
        if abs(rows_in_prox.vm3.max() - rows_in_prox.vm4.max()) < 20:
            for idx_del, row in rows_in_prox.iterrows():
                df_monvals.loc[idx_del, "vm4"] = np.NaN
                df_monvals.loc[idx_del, "vm3"] = np.NaN
                df_monvals.loc[idx_del, "vm5"] = np.NaN
    
    #delete urin out of bound readings 
    df_monvals.loc[df_monvals.vm24 > 2000] = np.NaN
    df_monvals.loc[df_monvals.pm39 > 250] = np.NaN
    df_monvals.loc[df_monvals.pm40 > 2000] = np.NaN
    df_monvals.loc[df_monvals.pm41 > 1] = np.NaN
    df_monvals.loc[df_monvals.pm42 > 0.075] = np.NaN
    df_monvals.loc[df_monvals.pm43 > 0.5] = np.NaN
    df_monvals.loc[df_monvals.pm44 > 7] = np.NaN
    df_monvals.loc[df_monvals.pm45 > 0.05] = np.NaN

    return df_monvals


def process_batch(batchid, patientid_start, patientid_stop):
    print (f'Processing batch id: {batchid}: {patientid_start}-{patientid_stop}')
    #open spark session
    spark = get_spark_session(8, 1024, 64)
    
    df_bolus = spark.read.parquet(ISOBOLUS_PATH).where('vm1_criterion = 1 and vm5_criterion = 1').where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    cand_file_mon=glob.glob(os.path.join(HIRID2_META_PATH,"reduced_fmat_{}_*.h5".format(batchid)))[0]
    df_monvals = pd.read_hdf(os.path.join(HIRID2_META_PATH, cand_file_mon), columns=['PatientID', 'Datetime'] + all_ids, where="PatientID in [" + ','.join(map(str, df_bolus.patientid.unique())) + "]")
    df_stay_data = spark.read.parquet(PATIENT_STAY_PATH).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    
    #simple outlier filtering
    df_monvals = filter_mon_vals(df_monvals)
 
    #generate columns names
    result_columns = ['patientid', 'bolusid']
    for variableid in mon_value_ids + last_values + pharma_ids + ['age', 'sex', 'emergency', 'height', 'adm_diag', 'adm_codeid', 'apacheII', 'sepsis', 'ventilated']:
        result_columns.append(variableid + "_baseline")
        if variableid in TREND_VARS:
            result_columns.append(variableid + "_trend")
        if variableid in VARIABLE_PERIODS_OUTCOMES:
            period_lenght = VARIABLE_PERIODS_OUTCOMES[variableid]
            for i in range(0, OUTCOME_OBSERVATION_TIME, period_lenght):
                result_columns.append(variableid + "_" + str(i))

    #process data
    df_outcomes = pd.DataFrame(columns = result_columns)
    for patientid in df_bolus.patientid.unique():
        outcome_pat = process_patient(patientid, df_stay_data, df_bolus, df_monvals, result_columns)
        if len(outcome_pat)>0:
            df_outcomes = df_outcomes.append(outcome_pat, ignore_index=True)
    
    return df_outcomes
 
if __name__=="__main__": 
    parser=argparse.ArgumentParser()
    # CONSTANTS
    PATIENT_STAY_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay/2020-11-12"
    ISOBOLUS_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/bolus_isolated/2020-12-04/augmented/2020-12-05"
    HIRID2_META_PATH="/cluster/work/grlab/clinical/hirid2/research/3_merged/v8/reduced"
    HIRID2_PATH="/cluster/work/grlab/clinical/hirid2/pg_db_export"
    ID_PATH = '/cluster/work/grlab/clinical/hirid2/research/misc_derived/id_lists/PID_all_vars_chunking_100.csv'
    OUTPUT_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/measurements_around_bolus"
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"

    # Input paths
    parser.add_argument("--batchid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--batchid_stop", help="Last patient id to be processed [including]", type=int)
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")
    parser.add_argument("--bolus_path", default=ISOBOLUS_PATH, help="Path to boluses")
    parser.add_argument("--output_path", default=OUTPUT_DIR, help="Path to output")

    args=parser.parse_args()
    ISOBOLUS_PATH = args.bolus_path
    OUTPUT_DIR = args.output_path

    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_MEASURE_{}_{}.stdout".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_MEASURE_{}_{}.stderr".format(datetime.datetime.today().strftime('%Y-%m-%d'), args.batchid_start,args.batchid_stop)),'w')

    TIME_PERIOD_LENGHT = 300
    OUTCOME_OBSERVATION_TIME = 3600

    mon_value_ids = ['vm1', 'vm2', 'vm3', 'vm4', 'vm5', 'vm9', 'vm12', 'vm13', 'vm14', 'vm15', 'vm20', 'vm22', 'vm24', 'vm263']
    last_values = ['vm136', 'vm132', 'vm134', 'vm174', 'vm142', 'vm131','vm176', 'vm161', 'vm138', 'vm140', 'vm141']
    pharma_ids = ['pm39', 'pm40', 'pm41', 'pm42', 'pm43', 'pm44', 'pm45', 'pm46']
    vent_ids = ['vm21', 'vm306']
    all_ids = mon_value_ids + last_values + pharma_ids + vent_ids

    TREND_VARS = ['vm1', 'vm5']

    VARIABLE_PERIODS_BASELINE = {
        'vm1': 1800,
        'vm2': 3600,
        'vm3': 1800,
        'vm4': 1800,
        'vm5': 1800,
        'vm9': 1800,
        'vm12': 3600,
        'vm13': 1800,
        'vm15': 1800,
        'vm22': 1800,
        'vm24': 3600,
        'pm39': 1800,
        'pm40': 1800,
        'pm41': 1800,
        'pm42': 1800,
        'pm43': 1800,
        'pm44': 1800,
        'pm45': 1800,
        'pm46': 1800,
        'vm136': 3600,
        'vm138': 3600,
        'vm140': 3600,
        'vm132': 3600,
        'vm134': 3600,
        'vm174': 3600,
        'vm263': 3600,
        'vm14': 1800,
        'vm20': 1800,
        'vm142': 3600,
        'vm141': 3600,
        'vm131': 17280000,
        'vm176': 172800,
        'vm161': 172800
    }

    VARIABLE_PERIODS_OUTCOMES = {
        'vm1': 300,
        'vm2': 3600,
        'vm3': 300,
        'vm4': 300,
        'vm5': 300,
        'vm9': 300,
        'vm12': 3600,
        'vm13': 300,
        'vm15': 300,
        'vm22': 300,
        'vm24': 3600,
        'pm39': 300,
        'pm40': 300,
        'pm41': 300,
        'pm42': 300,
        'pm43': 300,
        'pm44': 300,
        'pm45': 300,
        'pm46': 300,
        'vm136': 3600,
        'vm140': 3600,
        'vm138': 3600,
        'vm132': 3600,
        'vm134': 3600,
        'vm174': 3600,
        'vm263': 3600,
        'vm14': 300,
        'vm20': 300,
        'vm142': 3600,
        'vm141': 3600,
        'vm183': 86400
    }
 
    spark = get_spark_session(8, 1024, 64)
    df_batches = pd.read_csv(ID_PATH)

    for batchid in range(args.batchid_start,args.batchid_stop+1):
        df_outcomes = pd.DataFrame()
        pids_batch = df_batches[df_batches.BatchID == batchid].PatientID
        df_pat_outcomes = process_batch(batchid, pids_batch.min(), pids_batch.max())
        df_outcomes = df_outcomes.append(df_pat_outcomes)

        output_dir = os.path.join(OUTPUT_DIR, datetime.datetime.today().strftime('%Y-%m-%d'))
        if not os.path.exists(output_dir): 
            os.makedirs(output_dir)

        df_outcomes.to_parquet(os.path.join(output_dir, "bolus_measurements_{}.parquet".format(batchid)))

    print ("success")

