# Physiological changes after fluid bolus therapy in cardiac surgery patients: a propensity score matched case-control study

## Key elements
**scripts/notebooks/p_effect_estimation.ipynb:** Contains the main statistical analysis and plots regarding the effects of fluid bolus therapy<br>
**scripts/PSM.R:** Key script executing the Propensity Score Matching<br>
**script/notebooks** Contains the other python notebooks required to generate the individual figures and tables. See Notebooks below for details.<br>

## Scripts
**identify_patientstays:** Collect general information about a patient stay including exclusion criteria  <br>
**identify_boluses:** Processess all infusions on a patient basis into one fluid flow and from there define bolus and isolated bolus regions  <br>
**identify_non_boluses:** Over a full patient stay, identify random regions not interfering with any fluid bolus  <br>
**augment_boluses:** Checks for boluses and/or non_boluses if hr and invasive map are available during observation period  <br>
**collect_measurements_around_bolus:** collects measurements for the full oberservation period, for each intervall and for all the variables of interest.  <br>
**PSM:** R script performing the propensity score matching<br>
**augment_boluses_post_psm:** Augmentention of the selected boluses with any desired information (currently use of betablockers)<br>

## Run order
1) python ~/source/2020_VolumeChallenge/cluster/run_patient_stay.py<br>
2) python ~/source/2020_VolumeChallenge/cluster/run_identify_boluses_closegap.py<br>
3) python ~/source/2020_VolumeChallenge/cluster/run_augment_boluses.py<br>
4) python ~/source/2020_VolumeChallenge/cluster/run_identify_non_boluses.py<br>
   python ~/source/2020_VolumeChallenge/cluster/run_collect_measurements_around_bolus.py<br>
5) python ~/source/2020_VolumeChallenge/cluster/run_augment_non_boluses.py<br>
6) python ~/source/2020_VolumeChallenge/cluster/run_collect_measurements_around_non_bolus.py<br>
7) PSM<br>
8) python ~/source/2020_VolumeChallenge/cluster/run_augment_boluses_post_psm.py<br>

## Notebooks
**stay_and_bolus_inclusion_statistics** Number of bolus and patient stays included and excluded (Fig. 1)<br>
**population_statistics** Patient characteristics, overall and per group (Table 1)<br>
**PSM_matching_quality** Matching quality plot (Fig. 2)<br>
**effect_estimation** Effect of bolus vs control (Fig. 3, Supp Table 1)<br>
**bolus_volume_visualisation** Visualization of volume application to patient (Supp Fig. 1)<br>
**Bolus_Size_statistics_and_effect** Bolus size statistics and effect estimation of bolus size, as well as responders (Supp Fig. 2, Supp Fig. 3)<br>
**Bolus_reasons** Bolus reasons in the subcohort (Supp Supp Table 3)<br>
