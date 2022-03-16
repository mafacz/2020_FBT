'''Collects and saves general information about the patient stays'''

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import pdb
import argparse

from pyspark.sql import functions as sf

sys.path.append('/cluster/home/faltysm/source/2020_VolumeChallenge/common')
from spark_common import get_spark_session

def get_apache_metavariable(patientid, df_apache_groups, df_patitem, df_code):
    groupids = np.concatenate((df_apache_groups['II'].to_numpy(), df_apache_groups['IV'].to_numpy())).tolist()
    merged = pd.merge(df_patitem[df_patitem.patientid == patientid], df_code, on='codeid')
    results = merged[merged['groupid'].isin(groupids)]
    if results.empty:
        return 18, 0
    else:
        groupid = results.iloc[0].groupid
        metaid = df_apache_groups.loc[(df_apache_groups['II'] == groupid) | (df_apache_groups['IV'] == groupid), 'metavariable'].iloc[0]
        codeid = results.iloc[0].codeid
        return metaid, codeid

def get_sepis_label(patientid, df_patitem):
    return bool(df_patitem[(df_patitem.patientid == patientid) & (df_patitem.codeid == 8900)].count().codeid > 0)

def get_observed_value(df_observedrec, patientid, variableid):
    result = df_observedrec[(df_observedrec.patientid==patientid)&(df_observedrec.variableid==variableid)]
    if result.empty:
        return np.nan
    return result.iloc[0].value

def process_stay_information(patientid, birthyear, sex, df_monvals, df_pharmarec, df_seq, df_patitem, df_code, df_apache_groups, df_dervals, df_observedrec):
    #request data
    df_mon_rec = df_monvals[df_monvals.patientid==patientid]
    
    if not df_mon_rec.empty:
        #calculate admission information, beginn and end with first/last HR measurement
        admission = df_mon_rec.loc[df_mon_rec.variableid == 200]['datetime'].min()
        discharge = df_mon_rec.loc[df_mon_rec.variableid == 200]['datetime'].max()
        age_at_admission = admission.year - birthyear

        #reference point for relative dataframe = first HR measurement
        rel_referencepoint = admission
        lenght_of_stay = (discharge-admission).total_seconds()
        
        #get exlusion criterion data
        df_volume = df_pharmarec[df_pharmarec.patientid==patientid]
        df_ecmo = df_seq[df_seq.patientid==patientid]
        
        #check exclusion criteria, 1=criterion ok, 0=criterion bad -> exclusion
        invasive_bp_measurement_criterion = not df_mon_rec.loc[df_mon_rec.variableid == 110].empty
        age_criterion = age_at_admission >= 18
        study_drug_criterion = df_volume.empty
        ecmo_criterion = df_ecmo.empty

        #add admission diagnosis and sepsis
        adm_diag, adm_codeid = get_apache_metavariable(patientid, df_apache_groups, df_patitem, df_code)
        sepsis = get_sepis_label(patientid, df_patitem)
        
        #det emergency
        emergency_int = get_observed_value(df_observedrec, patientid, 14000140)
        emergency = False
        if emergency_int ==1:
            emergency = True

        #get height/weight
        height = get_observed_value(df_observedrec, patientid, 10000450)
        weight = get_observed_value(df_observedrec, patientid, 10000400)

        #get icu outcome
        outcome_death = get_observed_value(df_observedrec, patientid, 14000100)
        outcome_death = 1 if outcome_death == 4.0 else 0

        #get apache score
        apache_score = np.nan
        apache_scores = df_dervals[(df_dervals.patientid == patientid) & (df_dervals.variableid==30000140)].sort_values(by='datetime')
        if not apache_scores.empty:
            apache_score = apache_scores.iloc[0]['value']
        
        return [patientid, rel_referencepoint, lenght_of_stay, age_at_admission, sex, True, invasive_bp_measurement_criterion, age_criterion,study_drug_criterion, ecmo_criterion, adm_diag, adm_codeid, emergency, height, apache_score, sepsis, weight, outcome_death]
    else:
        return [patientid, datetime.now(), 0, 0, sex, False, True, True,True, True, 18, 0 , False,0,np.nan, False, np.nan, 0]

'''wraper loops through a set of patients'''
def process_batch(patientid_start, patientid_stop):
    OUTPUT_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay"
    mon_value_ids=[200,110]
    volume_pharma_ids=[1000752]
    observ_value_ids=[10000450,14000140,10000400,14000100]

    #open spark sessions
    spark = get_spark_session(8, 1024, 64)
    output_dir = os.path.join(OUTPUT_DIR, datetime.today().strftime('%Y-%m-%d'))

    df_stay_table = pd.read_hdf(STATIC_PATH, where='PatientID >= ' + str(patientid_start) + ' and PatientID <= ' + str(patientid_stop))
    df_monvals = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_monvals')).where(sf.col('variableid').isin(mon_value_ids)).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_dervals = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_dervals')).where(sf.col('variableid').isin([30000140])).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_pharmarec = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_pharmarec')).where(sf.col('pharmaid').isin(volume_pharma_ids)).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_seq = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_patcareseqs')).where("(lower(name) like '%impella%' or lower(name) like '%ecmo%') and lower(name) not like '%wunde%' and lower(name) not like '%ex%' and lower(name) not like '%naht%' and lower(name) not like '%blut%'").where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_patitem = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_codeditem')).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_code = spark.read.parquet(os.path.join(HIRID2_PATH, 's_coderef')).toPandas()
    df_apache_groups = pd.read_csv('/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/misc/apache_group.csv')
    df_observedrec = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_observrec')).where(sf.col('variableid').isin(observ_value_ids)).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()

    df_stay_info = pd.DataFrame(columns=['patientid', 'rel_referencepoint', 'lenght_of_stay', 'age_at_admission', 'sex', 'invasive_hr_measurement_criterion', 'invasive_bp_measurement_criterion', 'age_criterion','study_drug_criterion', 'ecmo_criterion', 'adm_diag', 'adm_codeid', 'emergency', 'height', 'apacheII', 'sepsis', 'weight', 'outcome_death'])
    for idx, row in df_stay_table.iterrows():
        print(row.PatientID)
        df_stay_info = df_stay_info.append(pd.Series(process_stay_information(row.PatientID, row.birthYear, row.Sex, df_monvals, df_pharmarec, df_seq, df_patitem, df_code, df_apache_groups, df_dervals, df_observedrec), index=df_stay_info.columns), ignore_index=True)
    
    if not os.path.exists(output_dir): 
        os.makedirs(output_dir)

    if not df_stay_info.empty:
        df_stay_info.to_parquet(os.path.join(OUTPUT_DIR, datetime.today().strftime('%Y-%m-%d'), "patientstay_{}_{}.parquet".format(patientid_start, patientid_stop)))
  
if __name__=="__main__":
    parser=argparse.ArgumentParser()

    # CONSTANTS
    HIRID2_PATH="/cluster/work/grlab/clinical/hirid2/pg_db_export/"
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
    STATIC_PATH='/cluster/work/grlab/clinical/hirid2/research/1a_hdf5_clean/v8/static.h5'
 
    # Input paths
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")
    parser.add_argument("--patientid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--patientid_stop", help="Last patient id to be processed [including]", type=int)

    args=parser.parse_args()

    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_STAYINFO_{}_{}.stdout".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_STAYINFO_{}_{}.stderr".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')

    process_batch(args.patientid_start, args.patientid_stop)

    print ("success")