# 2020_FBT

## Scripts
**identify_patientstays:** Collect general information about a patient stay including exclusion criteria  
**identify_boluses:** Processess all infusions on a patient basis into one fluid flow and from there define bolus and isolated bolus regions  
**identify_non_boluses:** Over a full patient stay, identify random regions not interfering with any fluid bolus  
**augment_boluses:** Checks for boluses and/or non_boluses if hr and invasive map are available during observation period  
**collect_measurements_around_bolus:** collects measurements for the full oberservation period, for each intervall and for all the variables of interest.  
**PSM:** R script performing the propensity score matching
**augment_boluses_post_psm:** Augmentention of the selected boluses with any desired information (currently use of betablockers)

## Run order
1) python ~/source/2020_VolumeChallenge/cluster/run_patient_stay.py
2) python ~/source/2020_VolumeChallenge/cluster/run_identify_boluses_closegap.py
3) python ~/source/2020_VolumeChallenge/cluster/run_augment_boluses.py
4) python ~/source/2020_VolumeChallenge/cluster/run_identify_non_boluses.py
   python ~/source/2020_VolumeChallenge/cluster/run_collect_measurements_around_bolus.py
5) python ~/source/2020_VolumeChallenge/cluster/run_augment_non_boluses.py
6) python ~/source/2020_VolumeChallenge/cluster/run_collect_measurements_around_non_bolus.py
7) PSM
8) python ~/source/2020_VolumeChallenge/cluster/run_augment_boluses_post_psm.py

# Notebooks
**stay_and_bolus_inclusion_statistics** Number of bolus and patient stays included and excluded (Fig. 1)
**population_statistics** Patient characteristics, overall and per group (Table 1)
**PSM_matching_quality** Matching quality plot (Fig. 2)
**effect_estimation** Effect of bolus vs control (Fig. 3, Supp Table 1)
**bolus_volume_visualisation** Visualization of volume application to patient (Supp Fig. 1)
**Bolus_Size_statistics_and_effect** Bolus size statistics and effect estimation of bolus size, as well as responders (Supp Fig. 2, Supp Fig. 3)
**Bolus_reasons** Bolus reasons in the subcohort (Supp Supp Table 3)
