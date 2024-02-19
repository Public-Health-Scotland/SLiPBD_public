
##################################################
######### 1a. Read in NRS Live Births ------------
##################################################

#pick up chili file of updated chi
chili_chi <- readRDS(paste0(nrs_chi_path, "Updated_CHI_file.rds"))

chili_chi <- chili_chi %>% 
  mutate(chili_mother_upi = chi_pad(mother_chi),
         chili_baby_upi = chi_pad(baby_chi))

chili_chi <- chili_chi %>% select(triplicate_id, chili_mother_upi, chili_baby_upi)
# read in NRS live births
nrs_live_births <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(
      nrslb_sql_vars, cohort_start_date, "', 'yyyy-mm-dd')")
  )) %>%
  distinct() %>%
  clean_names() %>%
  mutate(triplicate_id = paste0("B",substr(year_of_registration,3,4), registration_district, 
                                                        str_pad(entry_number, width = 4, pad="0")))


##Join CHILI information####
nrs_live_births <- left_join(nrs_live_births, chili_chi)
##write out comparison file 
chi_checks_baby <- nrs_live_births %>% filter(!is.na(chili_baby_upi)) %>%
  mutate(different_baby_upi  = case_when(is.na(child_upi_number) ~ "No baby CHI in NRS",
                                           chili_baby_upi != child_upi_number ~"Different baby chi to NRS",
                                           chili_baby_upi == child_upi_number~ "Same baby chi as NRS")) %>%
  mutate(year_birth = year(date_of_birth), month_birth = month(date_of_birth)) %>%
  group_by(year_birth, month_birth, different_baby_upi) %>%
  summarise(n_records = n())

chi_checks_mother <- nrs_live_births %>% filter(!is.na(chili_mother_upi)) %>%
  mutate(different_mother_upi  = case_when(is.na(mother_upi_number) ~ "No mother CHI in NRS",
                                         chili_mother_upi != mother_upi_number ~"Different mother chi to NRS",
                                         chili_mother_upi ==  mother_upi_number ~ "Same mother chi as NRS")) %>%
  mutate(year_birth = year(date_of_birth), month_birth = month(date_of_birth)) %>%
  group_by(year_birth, month_birth, different_mother_upi) %>%
  summarise(n_records = n())


saveRDS(chi_checks_baby, paste0(folder_data_path, "CHILI/", "chi_comparisons_babies",Sys.Date(),  ".rds"))
saveRDS(chi_checks_mother, paste0(folder_data_path, "CHILI/", "chi_comparisons_mothers",Sys.Date(),  ".rds"))
######

#Replace chi with NRS chi where applicable
nrs_live_births <- nrs_live_births %>%
  mutate(mother_upi_number =  case_when(!is.na(chili_mother_upi) ~ chili_mother_upi, 
                                        T~mother_upi_number)) %>%
  mutate(child_upi_number = case_when(!is.na(chili_baby_upi) ~ chili_baby_upi, 
                                      T~ child_upi_number)) %>%
  select(-c(chili_mother_upi, chili_baby_upi))
   

#Clean nrs live births####
nrs_live_births <- nrs_live_births %>%
  ###
  ### Deal with missing upi numbers
  mutate(mother_upi_number = chi_pad(mother_upi_number)) %>%
  mutate(validity = chi_check(mother_upi_number)) %>%
  mutate(mother_upi_number = if_else(validity == "Valid CHI", mother_upi_number, NA_character_)) %>%
  mutate(mother_upi_number = if_else(is.na(mother_upi_number),
                                     paste0("40",str_pad(string = row_number(),
                                                         width = 8,
                                                         side = "left",
                                                         pad = "0")),
                                     mother_upi_number)
         ) %>%
  mutate(child_upi_number = chi_pad(child_upi_number)) %>%
  mutate(validity = chi_check(child_upi_number)) %>%
  mutate(child_upi_number = if_else(validity == "Valid CHI", child_upi_number, NA_character_)) %>%
  mutate(child_upi_number = if_else(is.na(child_upi_number), paste0("50", str_pad(string = row_number(),
                                                                                  width = 8,
                                                                                  side = "left",
                                                                                  pad = "0")),
                                    child_upi_number))


###deduplicating
duplicate_baby_chi <- nrs_live_births %>% filter(duplicated(child_upi_number)) %>%
  pull(., child_upi_number)

data_nrs_live_births <- nrs_live_births %>%
  mutate(duplicate_baby_upi_flag = case_when(child_upi_number %in% duplicate_baby_chi ~ T,
                                             T ~ F)) %>%
  mutate(child_upi_number = case_when(duplicate_baby_upi_flag==T ~ 
                                        paste0("51",str_pad(string = row_number(),
                                                            width = 8,
                                                            side = "left",
                                                            pad = "0")),
                                      T~  child_upi_number)) %>%
   mutate(sex = case_when(sex == "1" ~ "M", sex == "2" ~ "F", T ~ NA_character_)) %>%
  mutate(outcome_type = "Live birth") %>%
  mutate(total_births_live_and_still = case_when(is.na(total_births_live_and_still) ~ 1,
                                                 total_births_live_and_still == 0 ~ 1,
                                                 T ~ total_births_live_and_still)) %>%
  mutate(estimated_gestation = ifelse(total_births_live_and_still==1,
                                      assumed_gestation_live_birth_single, assumed_gestation_live_birth_multiple)) %>%
           mutate(estimated_conception_date = date_of_birth - (weeks(estimated_gestation) - weeks(2) )) %>%
            select(-validity) %>%
  rename_with( ~ paste0("nrslb_", .)) %>%
  rowwise() %>% mutate(event_id = UUIDgenerate()) %>% ungroup()


write_rds(data_nrs_live_births, paste0(folder_temp_data, "nrs_live_births.rds"), compress = "gz")


rm(nrs_live_births, duplicate_baby_chi, data_nrs_live_births)
gc()
