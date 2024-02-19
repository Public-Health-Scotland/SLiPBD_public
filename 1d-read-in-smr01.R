##################################################
#Read in smr01
#This script reads in smr01, identifies episodes related to miscarriage or ectopic pregnancy
# and flags and retains these stays
####################################################

#source("SLiPBD process/00-setup.R")

data_smr01_temp_1 <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(smr01_sql_vars ,
      "WHERE SMR.DISCHARGE_DATE >= TO_DATE('", cohort_start_date, "', 'yyyy-mm-dd')
    AND (SUBSTR(MAIN_CONDITION, 1, 2) = 'O0' 
    OR SUBSTR(OTHER_CONDITION_1, 1, 2) = 'O0' 
    OR SUBSTR(OTHER_CONDITION_2, 1, 2) = 'O0' 
    OR SUBSTR(OTHER_CONDITION_3, 1, 2) = 'O0' 
    OR SUBSTR(OTHER_CONDITION_4, 1, 2) = 'O0' 
    OR SUBSTR(OTHER_CONDITION_5, 1, 2) = 'O0' )
    ORDER BY link_no, admission_date, discharge_date, admission, discharge, uri ASC")
  )
) %>%
  clean_names() %>%
  group_by(link_no, cis_marker) %>%
  mutate(admission_date = min(admission_date)) %>%
  mutate(discharge_date = max(discharge_date)) %>%
  ungroup() 

data_smr01_temp_2 <- data_smr01_temp_1 %>%
  pivot_longer(
    cols = main_condition:other_condition_5,
    names_to = "condition",
    values_to = "icd10"
  ) %>%
  filter(!is.na(icd10)) %>% # Remove any rows with no ICD10 code recorded, as they won't be required
  mutate(icd10_3 = str_sub(icd10, 1, 3)) %>% 
  rowwise() %>% 
  mutate(miscarriage = if_else(icd10 %in% icd10_miscarriage | icd10_3 %in% icd10_miscarriage, 1, 0)) %>% 
  mutate(ectopic_pregnancy = if_else(icd10 %in% icd10_ectopic | icd10_3 %in% icd10_ectopic, 1, 0)) %>% 
  mutate(molar_pregnancy = if_else(icd10 %in% icd10_molar | icd10_3 %in% icd10_molar, 1, 0)) %>% 
  group_by(link_no, cis_marker) %>%
  mutate(
    # If a miscarriage, molar pregnancy or ectopic pregnancy occurs anywhere in the stay, flag every episode as such
    miscarriage = max(miscarriage),
    ectopic_pregnancy = max(ectopic_pregnancy),
    molar_pregnancy = max(molar_pregnancy)
  ) %>%
  filter(miscarriage == 1 |
           ectopic_pregnancy == 1 |
           molar_pregnancy == 1) %>%
  mutate(
    ethnic_group = first(ethnic_group),
    # Some patients have multiple ethnic groups, postcodes etc recorded across different records. We need to choose one value and go with it, so choose the first observed value.
    dr_postcode = first(dr_postcode),
    hbres_currentdate = first(hbres_currentdate),
    hbtreat_currentdate = first(hbtreat_currentdate),
    location = first(location)
  ) %>%
  select(-c(condition, icd10_3)) %>%
  distinct() %>%
  mutate(condition = paste0("condition", row_number())) %>%
  ungroup() %>%
  pivot_wider(names_from = condition,
              values_from = icd10) 


##label outcomes and get rid of diagnosis fields
#add assumed gestation
data_smr01_temp_2 <- data_smr01_temp_2 %>%
  rename("mother_upi_number" = "upi_number") %>%
  mutate(outcome_type = case_when(ectopic_pregnancy == 1 ~ "Ectopic pregnancy",
                                  molar_pregnancy == 1 ~ "Molar pregnancy",
                                  miscarriage == 1 ~ "Miscarriage")) %>%
  mutate(gestation = case_when(outcome_type == "Molar pregnancy" | outcome_type == "Miscarriage"
                               ~ assumed_gestation_miscarriage,
                               outcome_type == "Ectopic pregnancy" 
                               ~ assumed_gestation_ectopic)) %>% 
  select(
    mother_upi_number,
    gestation,
    outcome_type,
    everything()
  ) %>%
  select(-c(miscarriage, molar_pregnancy, ectopic_pregnancy)) 



##moving window de-duplication on 83 days.
data_smr01_temp_2 <- moving_index_deduplication(data_smr01_temp_2, link_no, admission_date, dedupe_period)

##group by person
data_smr01_temp_2 <- data_smr01_temp_2 %>%
  group_by(link_no, event) %>%
  ###the order of the case-when ensures that the priority where there are multiple codes
  # indicating differnt outcomes within the same event is ectopic > molar > miscarraige
  mutate(outcome_type = case_when("Ectopic pregnancy" %in% outcome_type ~ "Ectopic pregnancy",
                                  "Molar pregnancy" %in% outcome_type ~ "Molar pregnancy",
                                  "Miscarriage" %in% outcome_type ~ "Miscarriage")) %>%
  mutate(
    cis_admission_date = min_(admission_date),
    cis_discharge_date = max_(discharge_date)
  ) %>%
  ungroup() %>%
  pivot_longer(col = starts_with("condition"),
               names_to = "condition",
               values_to = "icd10") %>%
  select(-c(admission_date, discharge_date, cis_marker, condition)) %>%
  filter(!is.na(icd10)) %>%
  distinct() %>%
  group_by(link_no, event) %>%
  mutate(condition = paste0("condition", row_number())) %>%
  mutate(
    ethnic_group = first_(ethnic_group), # Some patients have multiple ethnic groups, postcodes etc recorded across different records. We need to choose one value and go with it, so choose the first observed value.
    dr_postcode = first_(dr_postcode),
    hbres_currentdate = first_(hbres_currentdate),
    hbtreat_currentdate = first_(hbtreat_currentdate)
  ) %>%
  ungroup() %>%
  pivot_wider(names_from = condition,
              values_from = icd10) %>%
  mutate(mother_upi_number = chi_pad(mother_upi_number)) %>% 
  mutate(validity = chi_check(mother_upi_number)) %>% 
  mutate(mother_upi_number = case_when(validity == "Valid CHI" ~ mother_upi_number,
                                       T ~ NA_character_)) %>%
  select(-validity) %>% 
    ##reduce to one row per event 
  group_by(link_no, event) %>%
  slice(1) %>%  
  ungroup()

data_smr01_temp_2 <- data_smr01_temp_2 %>%
  mutate(mother_upi_number = case_when(
    # Create dummy UPI numbers for mothers who have no UPI recorded
    is.na(mother_upi_number) ~ paste0("43", str_pad(
      string = row_number(),
      width = 8,
      side = "left",
      pad = "0"
    )),
    T ~ mother_upi_number
  ))  %>%
  mutate(estimated_conception_date = cis_admission_date - (weeks(gestation) - weeks(2) )) %>%
  select(
    mother_upi_number,
    outcome_type,
    estimated_conception_date,
    gestation,
    everything()
  )

##final tidy p before saving
data_smr01  <-  data_smr01_temp_2   %>%
  select(-contains("condition")) %>%   ##remove icd10 codes as we do not need individual codes for further analyses
  rename_with( ~ paste0("smr01_", .)) %>% #prefix names
  mutate(smr01 = T) %>%
  rowwise() %>% mutate(event_id = UUIDgenerate()) %>% ungroup() # add uniqe event IDs


saveRDS(data_smr01, paste0(folder_temp_data, "smr01.rds"))

rm(data_smr01, data_smr01_temp_1, data_smr01_temp_2)
###############################

smr01 <- readRDS( paste0(folder_temp_data, "smr01.rds"))
df <- smr01
ds_name <- "SMR01"

n_male <- df  %>% filter(substr(smr01_mother_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_sex_from_chi = phsmethods::sex_from_chi(smr01_mother_upi_number)) %>%
  filter(mother_sex_from_chi==1) %>% count()
n_male$dataset <- "SMR01"
n_male$value <- "chi_implies_male"

n_missing_invalid_upi <- df %>%
  mutate(validity = chi_check(smr01_mother_upi_number)) %>% filter(validity !="Valid CHI") %>% count()
n_missing_invalid_upi$dataset <- "SMR01"
n_missing_invalid_upi$value <- "mother_chi_missing_invalid"


 df <- df %>%
  mutate(validity = chi_check(smr01_mother_upi_number)) %>% 
  filter(substr(smr01_mother_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_dob_fr_chi = phsmethods::dob_from_chi(smr01_mother_upi_number,
                                                      min_date= as.Date("1930-01-01"), max_date=Sys.Date())) %>%
  mutate(mother_age = floor(decimal_date(smr01_cis_admission_date) - decimal_date(mother_dob_fr_chi )))
n_bad_dob_fr_chi<-  df %>%  mutate(high_low_age = ifelse(mother_age %in% feasible_age, 0,1)) %>% 
  filter(high_low_age==1 & !is.na(mother_age)) %>% count()

n_bad_dob_fr_chi$dataset <- "SMR01"
n_bad_dob_fr_chi$value <- "chi_implies_age_outside_range"


n_unmatch_dob <- df %>% mutate(dob_mismatch = ifelse(smr01_dob !=mother_dob_fr_chi, 1,0)) %>% 
  filter(dob_mismatch==1 ) %>% count()

n_unmatch_dob$dataset <- ds_name
n_unmatch_dob$value <- "chi_dob_and_given_dob_do_not_match"

df <- rbind(n_bad_dob_fr_chi, n_male, n_missing_invalid_upi ,n_unmatch_dob) %>% 
  select(dataset, value, n) %>%
  group_by(dataset, value) %>% summarise(n=sum(n))
saveRDS(df,paste0(folder_temp_data,"checks/smr01_checks.rds") )


rm(df, smr01)
gc()