import subprocess
import os
import os.path
import sys
import argparse

step_size = 2499
for batch_idx in range(0,115000, step_size+1):
    LOG_DIR="/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/logs"
    job_name="boluses_{}".format(batch_idx)
    mem_in_mbytes = 2000
    n_cpu_cores = 6
    n_compute_hours = 12

    compute_script_path="/cluster/home/faltysm/source/2020_VolumeChallenge/scripts/identify_non_boluses.py"

    log_result_file=os.path.join(LOG_DIR, "{}_RESULT.txt".format(job_name))

    subprocess.call(["source activate ds_p38_base"],shell=True)

    cmd_line=" ".join(["bsub", "-R", "rusage[mem={}]".format(mem_in_mbytes), "-G ms_raets", "-n", "{}".format(n_cpu_cores), "-r", "-W", "{}:00".format(n_compute_hours),                                  
                                   "-J","{}".format(job_name), "-o", log_result_file, "python", compute_script_path, "--run_mode CLUSTER",
                                   "--patientid_start {}".format(batch_idx), 
                                   "--patientid_stop {}".format(batch_idx+step_size)])
     
    print (cmd_line)
    subprocess.call([cmd_line], shell=True)