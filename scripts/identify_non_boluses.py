'''Over a full patient stay, identify random regions not interfering with any fluid bolus'''

import pandas as pd
import numpy as np
import os
import math
import sys
from datetime import datetime
import pdb
import argparse
import random
import pickle

from pyspark.sql import functions as sf

sys.path.append('/cluster/home/faltysm/source/2020_VolumeChallenge/common')
from spark_common import get_spark_session

def get_stay_information(patientid, df_stay_data):
    #reference point for relative dataframe
    print(patientid)
    stay = df_stay_data[df_stay_data.patientid==patientid].iloc[0]
    rel_referencepoint = stay.rel_referencepoint
    lenght_of_stay = stay.lenght_of_stay
    lenght_of_stay_period_count = math.ceil(lenght_of_stay / TIME_PERIOD_LENGHT)

    return rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count

def calculate_previous_fluid(rastered_ringer, start_of_bolus, duration_hours):
    duration_timeslices = int(duration_hours * 3600 / TIME_PERIOD_LENGHT)
    previous_fluid = 0
    start_of_fluid_period = max(0, start_of_bolus - duration_timeslices)
    if start_of_bolus > 0:
        previous_fluid = np.sum(rastered_ringer[start_of_fluid_period:start_of_bolus])
    
    return previous_fluid

def process_patient(patientid, iso_boluses, boluses, df_stay_data):
    rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count = get_stay_information(patientid, df_stay_data)

    #generate a array over full stay where bolusregions timepoints are marked with 1
    bolus_array = np.zeros(lenght_of_stay_period_count)
    for index, bolus in boluses.iterrows():
        bolus_array[int(bolus.startindex):int(bolus.endindex+1)] = 1

    df_patient_non_boluses = pd.DataFrame(columns=['patientid', 'bolusid', 'startindex', 'endindex', 'lenght', 'fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])
    rastered_ringer = pickle.load(open(os.path.join(VOLUME_PATH, f"{int(patientid)}.pickle"), "rb"))

    non_bolus_id = 0
    for idx in range(0, NON_BOLUS_MULTIPLICATION_FACTOR):
        for index, bolus in iso_boluses.iterrows():
            #generate a array over full stay where 1 if possible beginn of an isolated non_bolus with lengt of reference bolus
            nonbolus_possible_start_array = np.zeros(lenght_of_stay_period_count)    
            for i in range(int(ISOLATED_BOLUS_START_MARGIN/TIME_PERIOD_LENGHT), lenght_of_stay_period_count-int(bolus.lenght)-int(ISOLATED_BOLUS_END_MARGIN/TIME_PERIOD_LENGHT)):
                if np.sum(bolus_array[i-int(ISOLATED_BOLUS_START_MARGIN/TIME_PERIOD_LENGHT): i + int(bolus.lenght+ISOLATED_BOLUS_END_MARGIN/TIME_PERIOD_LENGHT)]) == 0:
                    nonbolus_possible_start_array[i] = 1
            
            #if no possible starts, exit loop for this patient
            if np.sum(nonbolus_possible_start_array) <= len(df_patient_non_boluses.index):
                print ("no possible non-bolus available")
                break

            #select a non bolus which was not choosen yet
            sucessfull_choice = False
            while not sucessfull_choice:
                start_of_bolus = np.random.choice(np.nonzero(nonbolus_possible_start_array)[0],1)[0]
                if start_of_bolus not in df_patient_non_boluses.startindex.unique():
                    sucessfull_choice = True
                else:
                    print ("non-bolus start already taken")
            
            end_of_bolus = int(start_of_bolus + bolus.lenght - 1)
            non_bolus_id+=1
            previous_total_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 8640)
            previous_2h_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 2)
            previous_1h_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 1)
            previous_30min_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 0.5)
            non_bolus = pd.Series([bolus.patientid, non_bolus_id, start_of_bolus, end_of_bolus, bolus.lenght, bolus.fluidamount, previous_total_fluid, previous_30min_fluid, previous_1h_fluid, previous_2h_fluid], index=['patientid', 'bolusid', 'startindex', 'endindex','lenght','fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])
            
            df_patient_non_boluses = df_patient_non_boluses.append(non_bolus, ignore_index=True)

    return df_patient_non_boluses

'''wraper loops through a set of patients'''
def process_batch(patientid_start, patientid_stop):
    #open spark session
    spark = get_spark_session(8, 1024, 64)
    df_isobolus = spark.read.parquet(ISOBOLUS_PATH).where('vm1_criterion = 1 and vm5_criterion = 1').where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_bolus = spark.read.parquet(BOLUS_PATH).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()
    df_stay_data = spark.read.parquet(PATIENT_STAY_PATH).where('patientid >= ' + str(patientid_start) + ' and patientid <= ' + str(patientid_stop)).toPandas()

    df_batch_non_boluses = pd.DataFrame(columns=['patientid', 'bolusid', 'startindex', 'endindex', 'lenght', 'fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])
    for patientid in df_isobolus.patientid.unique():
        df_isobolus_pat = df_isobolus[df_isobolus.patientid == patientid]
        df_bolus_pat = df_bolus[df_bolus.patientid == patientid]
        pat_boluses = process_patient(patientid, df_isobolus_pat, df_bolus_pat, df_stay_data) 
         
        df_batch_non_boluses = df_batch_non_boluses.append(pat_boluses, ignore_index=True)

    output_dir= os.path.join(OUTPUT_DIR, 'non_bolus', datetime.today().strftime('%Y-%m-%d'))

    if not os.path.exists(output_dir): 
        os.makedirs(output_dir)

    if len(df_batch_non_boluses)>0:
        df_batch_non_boluses.to_parquet(os.path.join(output_dir, "non_bolus_{}_{}.parquet".format(patientid_start, patientid_stop)))
 
if __name__=="__main__": 
    parser=argparse.ArgumentParser()
    # CONSTANTS 
    HIRID2_PATH="/cluster/work/grlab/clinical/hirid2/pg_db_export/"
    PATIENT_STAY_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay/2020-11-12"
    ISOBOLUS_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/bolus_isolated/2020-12-05/augmented/2020-12-05"
    BOLUS_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/bolus/2020-12-05"
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
    VOLUME_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/volume/2020-12-05"
    OUTPUT_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed"
    
    # Script arguments default values
    TIME_PERIOD_LENGHT = 300
    BOLUS_LIMIT = 250
    BOLUS_WINDOW = 1800
    ISOLATED_BOLUS_END_MARGIN = 3600
    ISOLATED_BOLUS_START_MARGIN = 3600
    NON_BOLUS_MULTIPLICATION_FACTOR = 2

    # Input paths
    parser.add_argument("--time_period_lenght", default=TIME_PERIOD_LENGHT, help="The time series is rastered into these 'block' of times [seconds]")
    parser.add_argument("--bolus_limit", default=BOLUS_LIMIT, help="Fluid amounts >= this value will be considered a bolus [ml]")
    parser.add_argument("--bolus_window", default=BOLUS_WINDOW, help="Time period to consider as bolus [sec]")
    parser.add_argument("--isolated_bolus_end_margin", default=ISOLATED_BOLUS_END_MARGIN, help="Period after the end of a bolus, with no other bolus [sec]")
    parser.add_argument("--isolated_bolus_start_margin", default=ISOLATED_BOLUS_START_MARGIN, help="Period before the start of a bolus, with no other bolus [sec]")
    parser.add_argument("--patientid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--patientid_stop", help="Last patient id to be processed [including]", type=int)
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")
    
    args=parser.parse_args()

    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_NONBOLUSDETECTION_{}_{}.stdout".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_NONBOLUSDETECTION_{}_{}.stderr".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')

    process_batch(args.patientid_start, args.patientid_stop)

    print ("success")