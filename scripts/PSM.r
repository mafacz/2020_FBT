library(MatchIt)
library(dplyr)
library(tidyr)
library(sparklyr)
library(ggplot2)
library(ggridges)
library(gridExtra)
library(effsize)

config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "10G"
config$`sparklyr.shell.executor-memory` <- "10G"
sc <- spark_connect(master = "local", config = config)

#functions
set_pharma_to_zero <- function(df, variableid)
{
    v_name = paste(variableid, "_baseline", sep = "")
    df[is.na(df[,v_name]),v_name] <- 0
    
    for (t in seq(0,3300,300)) {
        v_name = paste(variableid, toString(t), sep = "_")
        df[is.na(df[,v_name]),v_name] <- 0
    }
    return (df)
}

process_missingness_pattern <- function(row) {
    df_ana_filtered <- df_ana

    #pattern: 1=value missing, 0=value not missing
    pattern_columns_with_missing_values = col_with_na[selected_missingness_patterns[row,col_with_na] == 1]
    pattern_columns_with_all_values = col_with_na[selected_missingness_patterns[row,col_with_na] == 0]
    pattern_column_set = c('startindex', 'previous_fluid_30min', 'previous_fluid_2h', pattern_columns_with_all_values, col_without_na[grepl('*_baseline',col_without_na)], col_without_na[grepl('*_trend',col_without_na)])

    #filter out rows which have measurements where they should not have
    for (col in pattern_columns_with_missing_values) {
        df_ana_filtered <- df_ana_filtered[is.na(df_ana_filtered[,col]),]
    }

    #filter out rows which do not have measurements
    df_ana_filtered <- df_ana_filtered %>%  # MatchIt does not allow missing values
      select(patientid, bolusid, bolus, one_of(pattern_column_set), one_of(col_without_na)) %>%
      na.omit()
    
    #match
    fmla <- as.formula(paste("bolus ~ ", paste(pattern_column_set, collapse= "+")))
    mod_match <- matchit(fmla,
                         method = "nearest", data = df_ana_filtered, distance="logit", caliper=.2)
    dta_m <- match.data(mod_match)
    
    return (list(fmla, dta_m, mod_match))
}

#constants
first_bolus_only <- FALSE
group_size_cuttoff = 100
#read args
args = commandArgs(trailingOnly=TRUE)

# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("Please supply study type", call.=FALSE)
} else {
    if (!(args[1] %in% c('cardiac')))
        stop("unknown subgroup")
}
subgroup <- args[1]

if (subgroup == "cardiac") {
    output_folder <- "/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/psm_cardiac"
    print ("Processing cardiac psm")
}

#load data
spark_tbl_handle <- spark_read_parquet(sc, "bolus", "/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/measurements_around_bolus/2020-12-05")
df_bolus <- collect(spark_tbl_handle)
spark_tbl_handle <- spark_read_parquet(sc, "nobolus", "/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/measurements_around_non_bolus/2020-12-05")
df_no_bolus <- collect(spark_tbl_handle)
spark_tbl_handle <- spark_read_parquet(sc, "bolus_info", "/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/bolus_isolated/2020-12-05/augmented/2020-12-05")
df_bolus_base_info <- collect(spark_tbl_handle)
spark_tbl_handle <- spark_read_parquet(sc, "no_bolus_info", "/cluster/work/grlab/clinical/hirid2/research/faltysm/volume_challenge/gap_closed/non_bolus/2020-12-05/augmented/2020-12-05")
df_no_bolus_base_info <- collect(spark_tbl_handle)

#merge bolus and additional bolus info
df_bolus_augm <- merge(df_bolus,df_bolus_base_info,by=c("patientid","bolusid"))
df_no_bolus_augm <- merge(df_no_bolus,df_no_bolus_base_info,by=c("patientid","bolusid"))

#subset patients to study group (general inclusion criteria are enforced in identify boluses)
if (subgroup == "cardiac") {
    df_bolus_augm <- df_bolus_augm %>% filter(df_bolus_augm$adm_codeid_baseline %in% c(8699,8700,8701,9085,9086,9087,9088,9089))
    df_no_bolus_augm <- df_no_bolus_augm %>% filter(df_no_bolus_augm$adm_codeid_baseline %in% c(8699,8700,8701,9085,9086,9087,9088,9089))
}

if (first_bolus_only) {
    include_bolus = c(df_bolus_augm[,'bolusid'] == 1 & df_bolus_augm[,'lenght'] <= 6) #only look at first and short boluses
} else {
    include_bolus = c(df_bolus_augm[,'lenght'] <= 6) #only look at short boluses
}
include_non_bolus = c(df_no_bolus_augm[,'lenght'] <= 6) #only look at short non boluses

#set all pharma variables to zero
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm39')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm40')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm41')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm42')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm43')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm44')
df_bolus_augm = set_pharma_to_zero(df_bolus_augm, 'pm45')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm39')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm40')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm41')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm42')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm43')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm44')
df_no_bolus_augm = set_pharma_to_zero(df_no_bolus_augm, 'pm45')

#combine to one dataset
df_bolus_augm$bolus <- 1
df_no_bolus_augm$bolus <- 0
df_ana <- rbind(df_bolus_augm[include_bolus,], df_no_bolus_augm[include_non_bolus,])

#convert categorical variables to factors
df_ana$adm_diag_baseline <- as.factor(df_ana$adm_diag_baseline)
df_ana$sex_baseline = as.factor(df_ana$sex_baseline)
df_ana$ventilated_baseline = as.factor(df_ana$ventilated_baseline)

#select only columns of interest for matching
base_var <- c('bolus', 'patientid', 'bolusid')
binary_cov <- c('emergency_baseline')
factor_cov <- c('adm_diag_baseline', 'sex_baseline', 'ventilated_baseline')
numeric_cov <- c('startindex', 'age_baseline', 'apacheII_baseline', 'vm1_baseline', 'vm3_baseline', 'vm5_baseline', 'vm5_trend', 'vm1_trend', 'pm39_baseline', 'pm40_baseline', 'pm41_baseline', 'pm42_baseline', 'pm43_baseline', 'pm44_baseline', 'pm45_baseline', 'vm9_baseline', 'vm12_baseline', 'vm13_baseline', 'vm14_baseline', 'vm15_baseline', 'vm24_baseline', 'vm136_baseline', 'previous_fluid_30min', 'previous_fluid_2h')

#matching variable special rules for certain studies
if (subgroup == "cardiac") {
    factor_cov <- c('sex_baseline', 'ventilated_baseline')
}

cov <- c(binary_cov, numeric_cov, factor_cov)

df_ana <- df_ana %>%  # MatchIt does not allow missing values
  select(one_of(base_var), one_of(cov))

###find patterns of missing values
#find columns with na values
col_with_na_all = colnames(df_ana)[colSums(is.na(df_ana)) > 0]
col_without_na = colnames(df_ana)[colSums(is.na(df_ana)) == 0]
col_with_na = col_with_na_all[grepl('*_baseline',col_with_na_all)]

#find all patterns of missingness in the bolus data
n = length(col_with_na)
missingness_pattern = expand.grid(rep(list(0:1), n))
colnames(missingness_pattern) <- col_with_na

for(i in 1:nrow(missingness_pattern)){
    df_ana_filtered <- df_ana[df_ana$bolus==1,]
    for (j in 1:length(col_with_na)) {
        df_ana_filtered <- df_ana_filtered[is.na(df_ana_filtered[,col_with_na[j]]) == as.logical(missingness_pattern[i,j]),]
    }
    missingness_pattern[i,'count'] <- nrow(df_ana_filtered)
}
existing_missingness_patterns = missingness_pattern %>% filter(count>0)
selected_missingness_patterns = existing_missingness_patterns %>% filter(count>=group_size_cuttoff)

print (paste0("Amount of possible patterns: ", 2^n))
print (paste0("Amount of existing patterns: ", nrow(existing_missingness_patterns)))
print (paste0("Lost patients in to small groups: ", sum((existing_missingness_patterns %>% filter(count<=group_size_cuttoff))$count)))
print (selected_missingness_patterns)

#matched for each pattern of missingness and merge to df_total
all_columns = c(base_var, cov, 'distance', 'weights')
df_total = as.data.frame(matrix(numeric(),nrow = 0, ncol = length(all_columns)))
colnames(df_total) = all_columns
df_total$adm_diag_baseline <- as.factor(df_total$adm_diag_baseline)
df_total$sex_baseline = as.factor(df_total$sex_baseline)
df_total$ventilated_baseline = as.factor(df_total$ventilated_baseline)

for (i in 1:nrow(selected_missingness_patterns)) {
    results = process_missingness_pattern(i)
    df_total = bind_rows(df_total, results[[2]])
}

#merge with all data from bolus and non bolus
df_m_key_columns = df_total %>%
    select(patientid, bolus, bolusid)
df_total_anal <- merge(df_m_key_columns, rbind(df_bolus_augm[include_bolus,], df_no_bolus_augm[include_non_bolus,]),by=c("patientid", "bolusid", "bolus"))

#save it
df <- copy_to(sc, df_total_anal, overwrite = TRUE)
spark_write_parquet(df, file.path(output_folder, Sys.Date()))

#disconnect spark
spark_disconnect(sc)
