import pandas as pd
import numpy as np
import os

from pyspark import SparkConf
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower

VARIABLES_CLEAR_NAMES = {
  "vm1": "HR",
  "vm2": "Temperature (central)",
  "vm3": "ABPs",
  "vm4": "ABPd",
  "vm5": "MAP",
  "vm9": "ABPm",
  "vm12": "PCWP",
  "vm13": "CO",
  "vm14": "SvO2",
  "vm15": "CVP",
  "vm24": "Urine/h"
}

def get_spark_session(cores, memory_per_executor, max_result_size):
    driver_mem = cores * memory_per_executor + 2000 # + some driver overhead
    
    cfg = (SparkConf().set("spark.driver.memory", "{}m".format(driver_mem)).
            set("spark.executor.memory", "{}m".format(memory_per_executor)).
            set("spark.master", "local[{}]".format(cores)).
            set("spark.sql.execution.arrow.enabled", True).
           set("spark.driver.maxResultSize", "{}g".format(driver_mem)).
           set("spark.sql.session.timeZone", "UTC")
          )
    
    return (SparkSession.
             builder.
             config(conf=cfg).
             getOrCreate())

def get_included_subset(df_patientstay):
    return df_patientstay.where('invasive_hr_measurement_criterion=True and invasive_bp_measurement_criterion=True and age_criterion=True and study_drug_criterion=True and ecmo_criterion=True')