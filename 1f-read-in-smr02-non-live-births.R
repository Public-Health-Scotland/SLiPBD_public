###########################
##SMR02 - non-live births##
##########################

#start with the file from script 1b
smr02_data <- readRDS(paste0(folder_temp_data, "smr02_data.rds")) %>%
  filter(smr02_outcome_type !="Live birth" & smr02_outcome_type !="Stillbirth")

saveRDS(smr02_data,  paste0(folder_temp_data, "smr02_nonlive.rds"))

##QA checks on chi

df <- smr02_data
ds_name <- "smr02_nonlive"

n_male <- df %>% filter(substr(smr02_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_sex_from_chi = phsmethods::sex_from_chi(smr02_upi_number)) %>%
  filter(mother_sex_from_chi==1) %>% count()
n_male$dataset <- ds_name
n_male$value <- "chi_implies_male"

n_missing_invalid_upi <- df %>%
  mutate(validity = chi_check(smr02_upi_number)) %>% filter(validity !="Valid CHI") %>% count()
n_missing_invalid_upi$dataset <- ds_name
n_missing_invalid_upi$value <- "mother_chi_missing_invalid"


df <- df %>%
  mutate(validity = chi_check(smr02_upi_number)) %>% 
  filter(substr(smr02_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_dob_fr_chi = phsmethods::dob_from_chi(smr02_upi_number,
                                                      min_date= as.Date("1930-01-01"), max_date=Sys.Date())) %>%
  mutate(mother_age = floor(decimal_date(smr02_date_of_delivery) - decimal_date(mother_dob_fr_chi ))) 

table(df$mother_age, useNA="always")

n_bad_dob_fr_chi <-  df %>% mutate(high_low_age = ifelse(mother_age %in% feasible_age, 0,1)) %>% 
  filter(high_low_age==1 & !is.na(mother_age)) %>% count()

n_bad_dob_fr_chi$dataset <- ds_name
n_bad_dob_fr_chi$value <- "chi_implies_age_outside_range"

n_unmatch_dob<- df %>% mutate(dob_mismatch = ifelse(smr02_dob !=mother_dob_fr_chi, 1,0)) %>% 
  filter(dob_mismatch==1 ) %>% count()

n_unmatch_dob$dataset <- ds_name
n_unmatch_dob$value <- "chi_dob_and_given_dob_do_not_match"

df <- rbind(n_bad_dob_fr_chi, n_male, n_missing_invalid_upi,n_unmatch_dob ) %>% 
  select(dataset, value, n) %>%
  group_by(dataset, value) %>% summarise(n=sum(n))

saveRDS(df,paste0(folder_temp_data,"checks/smr02_nonlive_checks.rds") )
rm(df)

gc()

