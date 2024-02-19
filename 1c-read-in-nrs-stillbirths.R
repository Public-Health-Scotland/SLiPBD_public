##################################################
######### 1c. Read in NRS Still Births ------------
# read in and clean data
# does not link multiples or check for duplicates, this comes later
##################################################

#source("SLiPBD process/00-setup.R")


# read in NRS stillbirths
data_nrs_still_births <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(
      nrssb_sql_vars ,
    "WHERE DATE_OF_BIRTH >= TO_DATE('", cohort_start_date, "', 'yyyy-mm-dd')")
  )) %>%
  distinct() %>%
  clean_names() %>%
  mutate(triplicate_id = paste0("S",substr(year_of_registration,3,4),
                                registration_district, 
                                str_pad(entry_number, width = 4, pad="0")))

                        
#data cleaning ####
#remove those with DOB invalid 
#check mother chi numbers and create dummy chi for any missing
#recode missing or unfeasible gestation
#recode n births form 0 to 1 for singletons

data_nrs_stillbirths <- data_nrs_still_births %>% 
  filter(year_of_registration >=year(cohort_start_date)) %>% 
  filter(date_of_birth < Sys.Date()) %>% ##removes one odd one which has dob on it recorded as yr 9999
  mutate(mother_upi_number = chi_pad(mother_upi_number)) %>% 
  mutate(validity = chi_check(mother_upi_number)) %>% 
  mutate(mother_upi_number = case_when(validity == "Valid CHI" ~ mother_upi_number,
                                       T ~ NA_character_)) %>%
  select(-validity) %>% 
  mutate(mother_upi_number = case_when(
    is.na(mother_upi_number) ~ paste0("42", str_pad(
      string = row_number(),
      width = 8,
      side = "left",
      pad = "0"
    )),
    T ~ mother_upi_number
  )) %>%
  #stillbirths should not have a chi but some have been given one in error, these need to be replaced as may have eg a siblings chi attributed
  mutate(child_upi_number = paste0("54", str_pad(  # dummy child upi number 
      string = row_number(),
      width = 8,
      side = "left",
      pad = "0" ))
  ) %>%  
  #recode terminations based on COD
  mutate(
    termination = case_when(
      primary_cause_of_death == "P964" ~ T,
      secondary_cause_of_death_0 == "P964" ~ T,
      secondary_cause_of_death_1 == "P964" ~ T,
      secondary_cause_of_death_2 == "P964" ~ T,
      secondary_cause_of_death_3 == "P964" ~ T,
      T ~ F
    )
  ) %>%
  mutate(stillbirth = case_when(termination == T ~ F,
                                T ~ T)) %>%
  mutate(
    duration_of_pregnancy = case_when(duration_of_pregnancy == 99 ~ NA_real_,
                                      T ~ duration_of_pregnancy)
  ) %>%
  mutate(duration_of_pregnancy = if_else(duration_of_pregnancy %in% feasible_gestation_sb, duration_of_pregnancy, NA_real_)) %>% 
  mutate(assumed_gestation = if_else(is.na(duration_of_pregnancy), 1, 0)) %>% 
  mutate(duration_of_pregnancy = case_when(is.na(duration_of_pregnancy) ~ assumed_gestation_stillbirth,
                                           T ~ duration_of_pregnancy)) %>%
  mutate(estimated_date_of_conception = date_of_birth - (weeks(duration_of_pregnancy) - weeks(2))) %>%
  mutate(outcome_type = case_when(
    termination == T &  stillbirth == T ~ "Stillbirth/Termination", 
    termination == T ~ "Termination",
    stillbirth == T ~ "Stillbirth"
  )) %>%
  mutate(total_births_live_and_still = case_when(is.na(total_births_live_and_still) ~ 1,
                                                 total_births_live_and_still == 0 ~ 1,
                                                 T ~ total_births_live_and_still)) %>%
  select(
    mother_upi_number,
    date_of_birth,
    duration_of_pregnancy,
    estimated_date_of_conception,
    termination,
    stillbirth,
    everything()
  ) %>% 
  mutate(sex = case_when(sex == "1" ~ "M", sex == "2" ~ "F", T ~ NA_character_)) %>%
  rename_with( ~ paste0("nrssb_", .)) %>%
  replace_with_na_at(.vars = c("nrssb_sex"),
                     condition = ~.x == "9") %>% 
  ##create uuid
  rowwise() %>% mutate(event_id = UUIDgenerate()) 


saveRDS(data_nrs_stillbirths, paste0(folder_temp_data, "NRS_sb.rds"))

rm(data_nrs_stillbirths, data_nrs_still_births_raw)
##checks for N with male upi, unrealistic age/dob

data_nrs_stillbirths <- readRDS( paste0(folder_temp_data, "NRS_sb.rds"))

n_sb_male <- data_nrs_stillbirths %>% filter(substr(nrssb_mother_upi_number,1,2) !="46") %>% # remove dummy upi 1st
  mutate(mother_sex_from_chi = phsmethods::sex_from_chi(nrssb_mother_upi_number)) %>%
  filter(mother_sex_from_chi==1) %>% count()
n_sb_male$dataset <- "NRS_sb"
n_sb_male$value <- "chi_implies_male"
n_sb_bad_dob_fr_chi <- data_nrs_stillbirths %>% filter(substr(nrssb_mother_upi_number,1,2) !="46") %>% # remove dummy upi 1st
  mutate(mother_dob_fr_chi = phsmethods::dob_from_chi(nrssb_mother_upi_number,
         min_date= as.Date("1940-01-01"), max_date=Sys.Date())) %>%
  mutate(mother_age = floor(decimal_date(nrssb_date_of_birth) - decimal_date(mother_dob_fr_chi ))) %>%
  mutate(high_low_age = ifelse(mother_age %in% feasible_age, 0,1)) %>% 
filter(high_low_age==1) %>% count()
n_sb_bad_dob_fr_chi$dataset <- "NRS_sb"
n_sb_bad_dob_fr_chi$value <- "chi_implies_age_outside_range"

n_missing_invalid_upi <- data_nrs_stillbirths  %>%
  mutate(validity = chi_check(nrssb_mother_upi_number)) %>% filter(validity !="Valid CHI") %>% count()
n_missing_invalid_upi$dataset <- "NRS_sb"
n_missing_invalid_upi$value <- "mother_chi_missing_invalid"


df <- rbind(n_sb_bad_dob_fr_chi, n_sb_male, n_missing_invalid_upi) %>% select(dataset, value, n) %>% 
  group_by(dataset, value) %>% summarise(n=sum(n))


saveRDS(df,paste0(folder_temp_data,"checks/nrssb_checks.rds") )
