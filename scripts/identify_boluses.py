'''Processess all infusions on a patient basis into one fluid flow and from there define bolus and isolated bolus regions'''

import pandas as pd
import numpy as np
import os
import math
import sys
from datetime import datetime
import pdb
import argparse
import pickle

from pyspark.sql import functions as sf

sys.path.append('/cluster/home/faltysm/source/2020_VolumeChallenge/common')
from spark_common import get_spark_session
from spark_common import get_included_subset

def get_patient_volrec(patientid, df_pharmarec, volume_pharma_ids=[1000030,1000739,1000866]):
    df_volumes = df_pharmarec.where(sf.col('pharmaid').isin(volume_pharma_ids))
    df_volumes = df_volumes.where('patientid=' + str(patientid))
    return df_volumes

def get_stay_information(patientid, df_stay_data):
    #reference point for relative dataframe
    stay = df_stay_data.where(f'patientid={patientid}').toPandas()
    rel_referencepoint = stay.rel_referencepoint[0]
    lenght_of_stay = stay.lenght_of_stay[0]
    lenght_of_stay_period_count = math.ceil(lenght_of_stay / TIME_PERIOD_LENGHT)

    return rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count

def get_rastered_infusion(df_infusions, infusionid, rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count):
    infusions = df_infusions.where('infusionid=' + str(infusionid)).sort('datetime').select('datetime', 'givendose', 'cumuldose', 'rate', 'status','pharmaid').toPandas()

    infusion_periods = []
    for i in range(0, infusions.shape[0]-1): #-1: last row marks only the end
        row_start = int((infusions.iloc[i,0] - rel_referencepoint).total_seconds())
        if row_start < 0:
            row_start = 0
            print ("rowstart")
        row_end = int((infusions.iloc[i+1,0] - rel_referencepoint).total_seconds())-1  #ends one second before the start of the next interval
        if row_end < 0:
            print ("rowend before entry")  #currently these are not added see below
        if row_end > lenght_of_stay:
            row_end = lenght_of_stay
            print ("rowend")
        given_dose = infusions.iloc[i+1,1]
        period_lenght = (row_end - row_start)
        if given_dose > 0 and given_dose <= 1000 and row_end>0 and period_lenght>0:
            rate_sec = given_dose/period_lenght
            infusion_periods.append([row_start, row_end, given_dose, period_lenght, rate_sec])
    
    rastered_infusion_periods = []
    for infusion_period in infusion_periods:
        starting_edge, trailing_edge = [], []

        first_time_intervall = int(infusion_period[0] / TIME_PERIOD_LENGHT)    #partial intervall inculded
        period_lenght_count = math.ceil(infusion_period[3] / TIME_PERIOD_LENGHT)
        leading_zeros = [0] * first_time_intervall
        if period_lenght_count>1:
            if (infusion_period[0] % TIME_PERIOD_LENGHT) != 0:
                starting_edge = [(TIME_PERIOD_LENGHT - (infusion_period[0] % TIME_PERIOD_LENGHT)) * infusion_period[4]] #partial time intervall with infusion
            if (infusion_period[1] % TIME_PERIOD_LENGHT) != 0:
                trailing_edge = [(infusion_period[1] % TIME_PERIOD_LENGHT) * infusion_period[4]]
        else:
            starting_edge = [infusion_period[3] * infusion_period[4]]  #period shorter than one time intervall
        main_block_lenght = period_lenght_count - len(starting_edge) - len(trailing_edge)
        main_block = [infusion_period[4]*TIME_PERIOD_LENGHT] * main_block_lenght
        trailing_zeros = [0] * (lenght_of_stay_period_count - len(leading_zeros) - period_lenght_count)
        rastered_infusion_period = leading_zeros + starting_edge + main_block + trailing_edge + trailing_zeros
        rastered_infusion_periods.append(np.array(rastered_infusion_period))
        
    rastered_infusion = np.add.reduce(rastered_infusion_periods)
    infusion_source = infusions.iloc[0,5]
    return rastered_infusion, infusion_source 

def enlarge_bolus(rastered_ringer, bolus, start_of_bolus, end_of_bolus):
    #look for bolus enlargment direction
    for j in range(1,int(BOLUS_WINDOW / TIME_PERIOD_LENGHT)):
        for k in range(0,j+1):
            tmp_start_of_bolus = max(start_of_bolus - (j - k),0)
            tmp_end_of_bolus = min(end_of_bolus + k, len(bolus)-1)
            
            tmp_total_fluid = np.sum(rastered_ringer[tmp_start_of_bolus:tmp_end_of_bolus+1])
            if round(tmp_total_fluid) >= BOLUS_LIMIT:
                if k>0:
                    bolus[min(end_of_bolus + 1, len(bolus)-1)] = True
                    print (f"Bolus enlarged - end; current end: {end_of_bolus} new end:{end_of_bolus+1}")
                else:
                    bolus[max(start_of_bolus - 1,0)] = True
                    print (f"Bolus enlarged - beginning; current start: {start_of_bolus} new start:{start_of_bolus-1}")
                return
        if j==int(BOLUS_WINDOW / TIME_PERIOD_LENGHT)-1:
            raise ValueError("ERROR NO SOLUTION FOUND TO BOLUS ENLARGMENT")

def process_rastered_infusions_into_boluses(rastered_ringer): 
    #define bolus region by calculating bolus regions by cummulative sum
    cumsum_array = np.cumsum(rastered_ringer)
    avg_volume_h = cumsum_array[-1] / len(cumsum_array) * 3600 / TIME_PERIOD_LENGHT
    bolus_region = np.zeros(len(rastered_ringer), dtype=bool)
    for i in range(0, len(rastered_ringer)):
        upperindex = min(i+5, len(rastered_ringer)-1)
        previous_cumsum_value = 0    #this step is required for the first value of the stay
        if i > 0:
            previous_cumsum_value = cumsum_array[i-1]
        if cumsum_array[upperindex] - previous_cumsum_value >= 250:  #the i-th value contains the fluid given in the i-th period -> minus value at i-1
            bolus_region[i:upperindex+1] = True
    
    #bolus where current rate > desired average rate
    bolus = (rastered_ringer > BOLUS_LIMIT/BOLUS_WINDOW*TIME_PERIOD_LENGHT)&bolus_region

    #bolus growth if current "bolus" selection is not large enough.
    check_completed = False
    while check_completed == False:
        start_of_bolus = 0
        check_completed = True
        for i in range(1, len(bolus)):
            if bolus[i] == True and bolus[i-1] == False:
                start_of_bolus = i
            if bolus[i] == True and (i == (len(bolus) - 1) or bolus[i+1] == False):
                end_of_bolus = i
                total_fluid = np.sum(rastered_ringer[start_of_bolus:end_of_bolus+1])
                if round(total_fluid) < BOLUS_LIMIT:
                    print (f"small bolus: {total_fluid} start at {start_of_bolus} end at {end_of_bolus}")
                    check_completed = False
                    enlarge_bolus(rastered_ringer, bolus, start_of_bolus, end_of_bolus)
                    break
    
    #close 5min gaps
    if CLOSE_GAP:
        for i in range(len(bolus)):
            if i < len(bolus)-2 and bolus[i] == True and bolus[i+1] == False and bolus[i+2] == True:
                #close 5min gap
                print("gab closed")
                bolus[i+1] = True
    
    window_count_end_margin = int(ISOLATED_BOLUS_END_MARGIN / TIME_PERIOD_LENGHT)
    window_count_start_margin = int(ISOLATED_BOLUS_START_MARGIN / TIME_PERIOD_LENGHT)
    isolated_bolus = bolus.copy() 

    for i in range(len(bolus)):
        #last window of bolus, check if next x are all false, whereas x depends on endmargin and window size
        if i != len(bolus)-1 and bolus[i] == True and bolus[i+1] == False:
            for j in range(min(window_count_end_margin,len(bolus)-i-1)):
                if bolus[i+1+j] == True:  #an other bolus exists
                    #delete current bolus
                    k = i
                    print ("post condition: bolus deleted at " + str(k))
                    while bolus[k] == True and k >= 0:
                        isolated_bolus[k] = False
                        k -= 1

        #first window of a bolus, check if x before are false
        if i != 0 and bolus[i] == True and bolus[i-1] == False:
            for j in range(min(window_count_start_margin,i)):
                if bolus[i-1-j] == True:  #an other bolus exists
                    #delete current bolus
                    k = i
                    print ("prä condition: bolus deleted at " + str(k))
                    while k < len(bolus) and bolus[k] == True:
                        isolated_bolus[k] = False 
                        k += 1
                        
    #delete boluses reaching into first 1h
    for j in range(min(window_count_start_margin,len(isolated_bolus))):
        k=0
        while isolated_bolus[(j+k)] == True:
            print ("start bolus")
            isolated_bolus[(j+k)] = False
            if j+k+1 < len(isolated_bolus):
                k += 1

    #delete boluses reaching into last 1h
    for j in range(min(window_count_end_margin+1,len(isolated_bolus)-1)):
        k=0
        while isolated_bolus[-(j+k)] == True:
            print ("end bolus")
            isolated_bolus[-(j+k)] = False
            k += 1

    return bolus_region, bolus, isolated_bolus, avg_volume_h

def calculate_previous_fluid(rastered_ringer, start_of_bolus, duration_hours):
    duration_timeslices = int(duration_hours * 3600 / TIME_PERIOD_LENGHT)
    previous_fluid = 0
    start_of_fluid_period = max(0, start_of_bolus - duration_timeslices)
    if start_of_bolus > 0:
        previous_fluid = np.sum(rastered_ringer[start_of_fluid_period:start_of_bolus])
    
    return previous_fluid

def detect_boluses_and_describe(patientid, bolus_collection, rastered_ringer):
    #result stores
    df_boluses = pd.DataFrame(columns=['patientid', 'bolusid', 'startindex', 'endindex','lenght','fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])

    start_of_bolus = 0
    bolus_count = 0
    #search for boluses
    for i in range(0, len(bolus_collection)):
        if i > 0:
            if bolus_collection[i] == True and bolus_collection[i-1] == False:
                start_of_bolus = i
        else:
            if bolus_collection[i] == True:
                start_of_bolus = i
        if bolus_collection[i] == True and (i == len(bolus_collection)-1 or bolus_collection[i+1] == False):
            end_of_bolus = i
            bolus_count += 1
            bolus_lenght = end_of_bolus-start_of_bolus+1
            total_fluid = round(np.sum(rastered_ringer[start_of_bolus:end_of_bolus+1]))
            previous_total_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 8640)
            previous_2h_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 2)
            previous_1h_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 1)
            previous_30min_fluid = calculate_previous_fluid(rastered_ringer, start_of_bolus, 0.5)
            df_boluses=df_boluses.append(pd.Series([patientid, bolus_count, start_of_bolus, end_of_bolus,bolus_lenght, total_fluid, previous_total_fluid, previous_30min_fluid, previous_1h_fluid, previous_2h_fluid], index=df_boluses.columns), ignore_index=True)
    
    return df_boluses

def process_patient(patientid, df_stay_data, df_pharmarec):
    print (patientid)
    rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count = get_stay_information(patientid, df_stay_data)
    
    #get infusion data
    df_infusions = get_patient_volrec(patientid, df_pharmarec)

    #process infusions of patient to rastered infusions
    rastered_infusions = []
    infusion_sources = []
    for infusionid in [int(row.infusionid) for row in df_infusions.select('infusionid').distinct().sort('infusionid').collect()]:
        rastered_infusion, infusion_source = get_rastered_infusion(df_infusions, infusionid, rel_referencepoint, lenght_of_stay, lenght_of_stay_period_count)

        if isinstance(rastered_infusion, np.ndarray):
            rastered_infusions.append(rastered_infusion)
            infusion_sources.append(infusion_source)
        else:
            print ("DEBUG: " + str(infusionid))

    #bolus calculations 
    bolus_region = []
    bolus = []
    isolated_bolus = []
    rastered_ringer = []
    avg_volume_h = 0
    if len(rastered_infusions)>0:
        rastered_ringer = np.add.reduce(rastered_infusions)
        bolus_region, bolus, isolated_bolus, avg_volume_h = process_rastered_infusions_into_boluses(rastered_ringer)
                                
    #bolus detection and description
    df_boluses = detect_boluses_and_describe(patientid, bolus, rastered_ringer)
    df_isolated_boluses = detect_boluses_and_describe(patientid, isolated_bolus, rastered_ringer)
            
    return rastered_infusions, rastered_ringer, infusion_sources, bolus_region, bolus, isolated_bolus, df_boluses, df_isolated_boluses, avg_volume_h

'''wraper loops through a set of patients'''
def process_batch(args):
    #open spark session
    spark = get_spark_session(8, 1024, 64)
    df_pharmarec = spark.read.parquet(os.path.join(HIRID2_PATH, 'p_pharmarec')).where('patientid >= ' + str(args.patientid_start) + ' and patientid <= ' + str(args.patientid_stop)).cache()
    #get only patients/stays satisfying study criteria
    df_stay_data = get_included_subset(spark.read.parquet(PATIENT_STAY_PATH).where('patientid >= ' + str(args.patientid_start) + ' and patientid <= ' + str(args.patientid_stop))).cache()

    df_batch_boluses = pd.DataFrame(columns=['patientid', 'bolusid', 'startindex', 'endindex','lenght', 'fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])
    df_batch_isolated_boluses = pd.DataFrame(columns=['patientid', 'bolusid', 'startindex', 'endindex','lenght', 'fluidamount', 'previous_fluid_total', 'previous_fluid_30min', 'previous_fluid_1h', 'previous_fluid_2h'])
    for patientid in df_stay_data.select('patientid').distinct().toPandas().iloc[:,0]:
        rastered_infusions, rastered_ringer, infusion_sources, bolus_region, bolus, isolated_bolus, df_boluses, df_isolated_boluses, avg_volume_h = process_patient(patientid, df_stay_data, df_pharmarec)
         
        df_batch_boluses = df_batch_boluses.append(df_boluses, ignore_index=True)
        df_batch_isolated_boluses = df_batch_isolated_boluses.append(df_isolated_boluses, ignore_index=True)

        #saving rastered ringer as a pickle per patient
        output_dir_volume= os.path.join(OUTPUT_DIR, 'volume', datetime.today().strftime('%Y-%m-%d'))
        if not os.path.exists(output_dir_volume): 
            os.makedirs(output_dir_volume)
        pickle.dump(rastered_ringer, open(os.path.join(output_dir_volume, f"{patientid}.pickle"), "wb"))

    output_dir_bolus = os.path.join(OUTPUT_DIR, 'bolus', datetime.today().strftime('%Y-%m-%d'))  
    output_dir_bolus_isolated = os.path.join(OUTPUT_DIR, 'bolus_isolated', datetime.today().strftime('%Y-%m-%d'))

    if not os.path.exists(output_dir_bolus): 
        os.makedirs(output_dir_bolus)

    if not df_batch_boluses.empty:
        df_batch_boluses.to_parquet(os.path.join(output_dir_bolus, "bolus_{}_{}.parquet".format(args.patientid_start, args.patientid_stop)))

    if not os.path.exists(output_dir_bolus_isolated): 
        os.makedirs(output_dir_bolus_isolated)

    if not df_batch_isolated_boluses.empty:
        df_batch_isolated_boluses.to_parquet(os.path.join(output_dir_bolus_isolated, "bolus_isolated_{}_{}.parquet".format(args.patientid_start, args.patientid_stop)))
    
if __name__=="__main__": 
    parser=argparse.ArgumentParser()
    # CONSTANTS
    HIRID2_PATH="/cluster/work/grlab/clinical/hirid2/pg_db_export/"
    PATIENT_STAY_PATH="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/patient_stay/2020-11-12"
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
    OUTPUT_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge"
    
    # Script arguments default values
    TIME_PERIOD_LENGHT = 300
    BOLUS_LIMIT = 250
    BOLUS_WINDOW = 1800
    ISOLATED_BOLUS_END_MARGIN = 3600
    ISOLATED_BOLUS_START_MARGIN = 3600
    CLOSE_GAP = False

    # Input paths
    parser.add_argument("--time_period_lenght", default=TIME_PERIOD_LENGHT, help="The time series is rastered into these 'block' of times [seconds]")
    parser.add_argument("--bolus_limit", default=BOLUS_LIMIT, help="Fluid amounts >= this value will be considered a bolus [ml]")
    parser.add_argument("--bolus_window", default=BOLUS_WINDOW, help="Time period to consider as bolus [sec]")
    parser.add_argument("--isolated_bolus_end_margin", default=ISOLATED_BOLUS_END_MARGIN, help="Period after the end of a bolus, with no other bolus [sec]")
    parser.add_argument("--isolated_bolus_start_margin", default=ISOLATED_BOLUS_START_MARGIN, help="Period before the start of a bolus, with no other bolus [sec]")
    parser.add_argument("--patientid_start", help="First patient id to be processed", type=int)
    parser.add_argument("--patientid_stop", help="Last patient id to be processed [including]", type=int)
    parser.add_argument("--run_mode", default="INTERACTIVE", help="Should job be run in batch or interactive mode")
    parser.add_argument('--close_gap', default=False, action='store_true')
    parser.add_argument("--output_path", default=OUTPUT_DIR, help="Path to store results")

    args=parser.parse_args()
    CLOSE_GAP = args.close_gap
    OUTPUT_DIR = args.output_path
    assert(args.run_mode in ["CLUSTER", "INTERACTIVE"]) 

    if args.run_mode=="CLUSTER":
        sys.stdout=open(os.path.join(LOG_DIR,"{}_BOLUSDETECTION_{}_{}.stdout".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')
        sys.stderr=open(os.path.join(LOG_DIR,"{}_BOLUSDETECTION_{}_{}.stderr".format(datetime.today().strftime('%Y-%m-%d'), args.patientid_start,args.patientid_stop)),'w')

    process_batch(args)

    print ("success")