# read in smr02 data
smr02_data <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(
      smr02_sql_vars, 
    "WHERE (SMR.ADMISSION_DATE >= TO_DATE('", cohort_start_date, "', 'yyyy-mm-dd')) AND CONDITION_ON_DISCHARGE IN (2, 3)")
  )) %>%
  # Make sure numeric fields are stored as numerics
  mutate(across(c(CONDITION_ON_DISCHARGE,
                  TYPE_OF_ABORTION, DIABETES, BOOKING_SMOKING_HISTORY,
                  NUM_OF_BIRTHS_THIS_PREGNANCY, ESTIMATED_GESTATION, INDUCTION_OF_LABOUR,
                  OUTCOME_OF_PREGNANCY_BABY_1, OUTCOME_OF_PREGNANCY_BABY_2, OUTCOME_OF_PREGNANCY_BABY_3,
                  BIRTHWEIGHT_BABY_1,BIRTHWEIGHT_BABY_2, BIRTHWEIGHT_BABY_3,
                  OFC_BABY_1, OFC_BABY_2, OFC_BABY_3), as.numeric)) %>%
  clean_names()

smr02_data <- smr02_data %>%
  rename_with(~ paste0("smr02_", .)) %>%
  ###
  ### Handle missing number of births
  mutate(smr02_num_of_outcomes_this_pregnancy = case_when(is.na(smr02_num_of_births_this_pregnancy) ~ 1,
                                                          T ~ smr02_num_of_births_this_pregnancy)) %>%
  distinct() %>%
  ### Deal with valid chis
  ### Replace invalid chis with dummy chi
  mutate(smr02_upi_number = chi_pad(smr02_upi_number)) %>%
  mutate(validity = chi_check(smr02_upi_number)) %>%
  mutate(smr02_upi_number = case_when(validity == "Valid CHI" ~ smr02_upi_number,
                                      T ~ NA_character_)) %>%
  mutate(smr02_upi_number = case_when(is.na(smr02_upi_number) ~ paste0("41", str_pad(string = row_number(),
                                                                                     width = 8,
                                                                                     side = "left",
                                                                                     pad = "0")),
                                      T ~ smr02_upi_number)
  ) %>%
  ### create number live and number still births
  mutate(lb_baby_1 = case_when(smr02_condition_on_discharge == 3 &
                                  smr02_outcome_of_pregnancy_baby_1 %in% c(1, 3, 4, 5) ~ T,
                               T~F),
         lb_baby_2 = case_when(smr02_condition_on_discharge == 3 &
                                 smr02_outcome_of_pregnancy_baby_2 %in% c(1, 3, 4, 5) ~ T,
                               T~F),
         lb_baby_3 = case_when(smr02_condition_on_discharge == 3 &
                                 smr02_outcome_of_pregnancy_baby_3 %in% c(1, 3, 4, 5) ~ T,
                               T~F),
         sb_baby_1 = case_when(smr02_condition_on_discharge == 3 &
                                    smr02_outcome_of_pregnancy_baby_1 == 2 ~ T,
                               T~F),
         sb_baby_2 = case_when(smr02_condition_on_discharge == 3 &
                                 smr02_outcome_of_pregnancy_baby_2 == 2 ~ T,
                               T~F),
         sb_baby_3 = case_when(smr02_condition_on_discharge == 3 &
                                 smr02_outcome_of_pregnancy_baby_3 == 2 ~ T,
                               T~F)) %>% 
  #rowwise() %>%
  mutate(total_stillbirths_this_pregnancy = rowSums(across(sb_baby_1 :sb_baby_3)), 
         total_livebirths_this_pregnancy =  rowSums(across(lb_baby_1 :lb_baby_3)) ) %>%
  select(-c(lb_baby_1, lb_baby_2, lb_baby_3, sb_baby_1, sb_baby_2, sb_baby_3)) %>%
  ### Create identifier for the pregnancy before splitting into one row per baby
  mutate(smr02_preg_id = paste0("smr02_", row_number())) %>%
  # create a flag for pregnancies where one fetus dies in utero (8 Abortion of a dead fetus of a multiple pregnancy ending before 24 wks of gestation in which the other babies are live born.)
  mutate(smr02_abortion_of_fetus_died_in_utero_flag = case_when(smr02_condition_on_discharge == 3 &
                                                                  smr02_outcome_of_pregnancy_baby_1 == 8 ~ T, 
                                                                smr02_condition_on_discharge == 3 &
                                                                  smr02_outcome_of_pregnancy_baby_2 == 8 ~ T, 
                                                                smr02_condition_on_discharge == 3 &
                                                                  smr02_outcome_of_pregnancy_baby_3 == 8 ~ T, 
                                                                T ~ F)) %>%
   ###
  ### want to have 1 baby per row so unite all columns associated with each baby
  unite(
    col = "baby1",
    #"smr02_baby_upi_number_1",
    "smr02_baby_chi_number_1",
    "smr02_presentation_at_delivery_b1",
    "smr02_outcome_of_pregnancy_baby_1",
    "smr02_birthweight_baby_1",
    "smr02_apgar_5_minutes_baby_1",
    "smr02_sex_baby_1",
    "smr02_mode_of_delivery_baby_1",
    "smr02_ofc_baby_1"
  ) %>%
  unite(
    col = "baby2",
    #"smr02_baby_upi_number_2",
    "smr02_baby_chi_number_2",
    "smr02_presentation_at_delivery_b2",
    "smr02_outcome_of_pregnancy_baby_2",
    "smr02_birthweight_baby_2",
    "smr02_apgar_5_minutes_baby_2",
    "smr02_sex_baby_2",
    "smr02_mode_of_delivery_baby_2",
    "smr02_ofc_baby_2"
  ) %>%
  unite(
    col = "baby3",
    #"smr02_baby_upi_number_3",
    "smr02_baby_chi_number_3",
    "smr02_presentation_at_delivery_b3",
    "smr02_outcome_of_pregnancy_baby_3",
    "smr02_birthweight_baby_3",
    "smr02_apgar_5_minutes_baby_3",
    "smr02_sex_baby_3",
    "smr02_mode_of_delivery_baby_3",
    "smr02_ofc_baby_3"
  ) %>%
  pivot_longer(c(baby1:baby3),
               names_to = "smr02_baby") %>%
  ###
  ### separate unite value back into separate columns
  separate(
    value,
    c(#"smr02_baby_upi_number",
      "smr02_baby_chi_number",
      "smr02_presentation_at_delivery",
      "smr02_outcome_of_pregnancy",
      "smr02_birthweight",
      "smr02_apgar_5_minutes",
      "smr02_sex",
      "smr02_mode_of_delivery",
      "smr02_ofc"
    )
  ) %>%

  ###
  ### Determine outcomes
  mutate(smr02_live_birth = if_else(smr02_condition_on_discharge == 3 &
                                      smr02_outcome_of_pregnancy %in% c(1, 3, 4, 5),
                                    T,
                                    F)
         ) %>%
  mutate(smr02_miscarriage = if_else(smr02_condition_on_discharge == 2 &
                                       smr02_type_of_abortion %in% c(1, 2, 8, 9) &
                                       smr02_baby == "baby1",
                                     T,
                                     F)
         ) %>%
  mutate(smr02_molar_pregnancy = if_else(smr02_condition_on_discharge == 2 &
                                           smr02_type_of_abortion == 3 &
                                           smr02_baby == "baby1",
                                         T,
                                         F)
         ) %>%
  mutate(smr02_ectopic_pregnancy = if_else(smr02_condition_on_discharge == 2 &
                                             smr02_type_of_abortion == 6 &
                                             smr02_baby == "baby1",
                                           T,
                                           F)
         ) %>%
  mutate(smr02_stillbirth = if_else(smr02_condition_on_discharge == 3 &
                                      smr02_outcome_of_pregnancy == 2,
                                    T,
                                    F)
         ) %>%
  mutate(smr02_termination = if_else(smr02_condition_on_discharge == 2 &
                                       smr02_type_of_abortion == 4 &
                                       smr02_baby == "baby1",
                                     T,
                                     F)
         ) %>%
  mutate(smr02_outcome_type = case_when(smr02_termination == T ~ "Termination",
                                        smr02_live_birth == T ~ "Live birth",
                                        smr02_stillbirth == T ~ "Stillbirth",
                                        smr02_ectopic_pregnancy == 1 ~ "Ectopic pregnancy",
                                        smr02_molar_pregnancy == 1 ~ "Molar pregnancy",
                                        smr02_miscarriage == 1 ~ "Miscarriage")
         ) %>%
  ###
  #### Replace NAs with R's NA type, and remove any heights and weights which are not feasible
  mutate(smr02_baby_chi_number = case_when(smr02_baby_chi_number != "NA" ~ smr02_baby_chi_number,
                                           T ~ NA_character_)
         ) %>%
  mutate(smr02_presentation_at_delivery = case_when(smr02_presentation_at_delivery != "NA" ~ smr02_presentation_at_delivery,
                                                    T ~ NA_character_)
         ) %>%
  mutate(smr02_outcome_of_pregnancy = case_when(smr02_outcome_of_pregnancy != "NA" ~ smr02_outcome_of_pregnancy,
                                                T ~ NA_character_)
         ) %>%
  mutate(smr02_birthweight = case_when(smr02_birthweight != "NA" ~ smr02_birthweight,
                                       T ~ NA_character_)
         ) %>%
  mutate(smr02_apgar_5_minutes = case_when(smr02_apgar_5_minutes != "NA" ~ smr02_apgar_5_minutes,
                                           T ~ NA_character_)
         ) %>%
  mutate(smr02_sex = case_when(smr02_sex != "NA" ~ smr02_sex,
                               T ~ NA_character_)
         ) %>%
  mutate(smr02_mode_of_delivery = case_when(smr02_mode_of_delivery != "NA" ~ smr02_mode_of_delivery,
                                            T ~ NA_character_)
         ) %>%
  mutate(smr02_height = if_else(smr02_height < 10 | smr02_height > 200,
                                as.numeric(NA),
                                smr02_height)
         ) %>%
  mutate(smr02_weight_of_mother = if_else(smr02_weight_of_mother < 10 | smr02_weight_of_mother > 600,
                                          as.numeric(NA),
                                          smr02_weight_of_mother)
         ) %>%
  mutate(smr02_birthweight = as.numeric(smr02_birthweight)) %>%
  mutate(smr02_birthweight = if_else(smr02_birthweight < 10 | smr02_birthweight > 9000,
                                     NA_real_,
                                     smr02_birthweight)
         ) %>%
  mutate(smr02_booking_smoking_history = case_when(smr02_booking_smoking_history == 9 ~ NA_real_,
                                              T ~ smr02_booking_smoking_history)
         ) %>%
  mutate(smr02_diabetes = case_when(smr02_diabetes == 9 ~ NA_real_,
                                    T ~ smr02_diabetes)
         ) %>%
  mutate(smr02_ofc = as.numeric(smr02_ofc))


smr02_data %<>%
  ###  rename Baby CHI to Baby UPI.
  rename(smr02_baby_upi_number = smr02_baby_chi_number) %>%
  ###
  ### Deal with invalid baby upis
  mutate(smr02_baby_upi_number = chi_pad(smr02_baby_upi_number)) %>%
  mutate(validity = chi_check(smr02_baby_upi_number)) %>%
  mutate(smr02_baby_upi_number = if_else(validity == "Valid CHI",
                                         smr02_baby_upi_number,
                                         NA_character_)) %>%
  select(-validity) %>%
  mutate(smr02_baby_upi_number = if_else(is.na(smr02_baby_upi_number),
                                         paste0("52",str_pad(string = row_number(),
                                                             width = 8,
                                                             side = "left",
                                                             pad = "0")),
                                         smr02_baby_upi_number)
         ) %>%
  ###
  ### Recode sex
  mutate(smr02_sex = case_when(smr02_sex == "1" ~ "M",
                               smr02_sex == "2" ~ "F",
                               T ~ NA_character_)
         )

  ### Deal with gestation
  ### Code if gestation is feasible or not for that outcome
  ### Feasible gestation is coded in setup
smr02_data %<>%
  mutate(smr02_feasible_gestation = case_when(smr02_outcome_type == "Live birth" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_lb) ~ T,
                                              smr02_outcome_type == "Stillbirth" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_sb) ~ T,
                                              (smr02_outcome_type == "Miscarriage" |
                                                 smr02_outcome_type == "Molar pregnancy" |
                                                 smr02_outcome_type == "Ectopic pregnancy") &
                                                smr02_estimated_gestation %in% c(feasible_gestation_miscarriage) ~ T,
                                              smr02_outcome_type == "Termination" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_termination) ~ T,
                                              T ~ F)
         ) %>%
  # create estimated gestation if estimated gestation is not real based on date of delivery/date of admission
  mutate(smr02_estimated_gestation = case_when(smr02_feasible_gestation == T ~ smr02_estimated_gestation,
                                               smr02_feasible_gestation == F &
                                                 !is.na(smr02_date_of_delivery) &
                                                 !is.na(smr02_date_of_lmp) ~ as.numeric(floor(difftime(smr02_date_of_delivery, smr02_date_of_lmp, units = "weeks"))),
                                               smr02_feasible_gestation == F &
                                                 is.na(smr02_date_of_delivery) &
                                                 !is.na(smr02_date_of_lmp) ~ as.numeric(floor(difftime(smr02_admission_date, smr02_date_of_lmp, units = "weeks"))))
         ) %>%
  mutate(smr02_feasible_gestation = case_when(smr02_outcome_type == "Live birth" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_lb) ~ T,
                                              smr02_outcome_type == "Stillbirth" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_sb) ~ T,
                                              (smr02_outcome_type == "Miscarriage" |
                                                 smr02_outcome_type == "Molar pregnancy" |
                                                 smr02_outcome_type == "Ectopic pregnancy") &
                                                smr02_estimated_gestation %in% c(feasible_gestation_miscarriage) ~ T,
                                              smr02_outcome_type == "Termination" &
                                                smr02_estimated_gestation %in% c(feasible_gestation_termination) ~ T,
                                              T ~ F)
         ) %>%
  mutate(smr02_estimated_gestation = case_when(smr02_feasible_gestation == T ~ smr02_estimated_gestation,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Live birth" & smr02_num_of_outcomes_this_pregnancy==1 ~ assumed_gestation_live_birth_single,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Live birth" & smr02_num_of_outcomes_this_pregnancy>1 ~ assumed_gestation_live_birth_multiple,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Termination" ~ assumed_gestation_smr02_termination,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Stillbirth" ~ assumed_gestation_stillbirth,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Miscarriage" ~ assumed_gestation_miscarriage,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Molar pregnancy" ~ assumed_gestation_miscarriage,
                                               smr02_feasible_gestation == F &
                                                 smr02_outcome_type == "Ectopic pregnancy"~ assumed_gestation_ectopic,
                                               T ~ smr02_estimated_gestation)
         ) %>%
  mutate(smr02_estimated_conception_date = case_when(smr02_live_birth == T ~
                                                       smr02_date_of_delivery - (weeks(smr02_estimated_gestation) - weeks(2)),
                                                     smr02_live_birth == F ~
                                                       smr02_admission_date - (weeks(smr02_estimated_gestation) - weeks(2)))
         ) %>%
  ###
  ### Assume pregnancy end date based on date of delivery. NAs coded as admission date
  mutate(smr02_pregnancy_end_date = case_when(!is.na(smr02_date_of_delivery) ~ smr02_date_of_delivery,
                                              T ~ smr02_admission_date)
         ) %>%
  ####Compute gestation at booking 
    #remove impossible booking dates
  mutate(smr02_date_of_booking =  case_when(smr02_date_of_booking <= as.Date("1999-03-31") ~ NA, 
                                            smr02_date_of_booking <= smr02_estimated_conception_date ~ NA,
                                            smr02_date_of_booking > smr02_pregnancy_end_date ~ NA,
                                            T~smr02_date_of_booking)) %>%
  mutate(smr02_gestation_at_booking = case_when(!is.na(smr02_date_of_booking) ~
                                                  as.numeric(floor(difftime(smr02_date_of_booking,smr02_estimated_conception_date, units="weeks")))+2 )) %>%
  mutate(smr02_gestation_at_booking= case_when(smr02_gestation_at_booking %in% feasible_gestation_booking ~ smr02_gestation_at_booking, 
                                               T~NA))


########## DEAL WITH DUPLICATE SMR02 EVENTS ##############
# If a baby UPI appears more than once, but there was only one birth that pregnancy, the case is likely a duplicate
# This can occur when more than one SMR02 record is generated during a birth due to the patient being transferred at some point
# Note - this does not deduplicate multiples
smr02_live_and_still_births_singletons <- smr02_data %>%
  filter(smr02_outcome_type %in% c("Live birth", "Stillbirth")) %>%
  filter(smr02_num_of_births_this_pregnancy == 1) %>%
  group_by(smr02_baby_upi_number) %>%
  filter(row_number() == 1) # remove duplicate upis for singletons

smr02_live_and_still_births_multiples <- smr02_data %>%
  filter(smr02_outcome_type %in% c("Live birth", "Stillbirth")) %>%
  filter(smr02_num_of_births_this_pregnancy > 1) %>%
  group_by(smr02_baby_upi_number, smr02_baby) %>% #11488
  filter(row_number() == 1) # remove duplicate upis due to transfers

data_smr02_livebirths_and_stillbirths <- bind_rows(smr02_live_and_still_births_singletons,
                                                   smr02_live_and_still_births_multiples)


data_smr02_miscarriage_ectopic_and_termination <- smr02_data %>%
  filter(smr02_outcome_type %in% c("Miscarriage", "Ectopic pregnancy", "Molar pregnancy", "Termination"))

data_smr02_miscarriage_ectopic_and_termination <-
  moving_index_deduplication(data_smr02_miscarriage_ectopic_and_termination, smr02_upi_number, smr02_admission_date, dedupe_period)

data_smr02_miscarriage_ectopic_and_termination <- data_smr02_miscarriage_ectopic_and_termination %>%
  group_by(smr02_upi_number, event) %>%
  mutate(revised_conception_date = min(smr02_estimated_conception_date)) %>%
  ungroup()

data_smr02_miscarriage_ectopic_and_termination <-
  moving_index_deduplication(data_smr02_miscarriage_ectopic_and_termination, smr02_upi_number, revised_conception_date, dedupe_period)

data_smr02_miscarriage_ectopic_and_termination_1 <- data_smr02_miscarriage_ectopic_and_termination %>%
  group_by(smr02_upi_number, event) %>%
  mutate(smr02_outcome_type = case_when("Termination" %in% smr02_outcome_type ~ "Termination",
                                        "Ectopic pregnancy" %in% smr02_outcome_type ~ "Ectopic pregnancy",
                                        "Molar pregnancy" %in% smr02_outcome_type ~ "Molar pregnancy",
                                        "Miscarriage" %in% smr02_outcome_type ~ "Miscarriage")) %>%
  mutate(across(!smr02_feasible_gestation & !smr02_estimated_gestation & !smr02_outcome_type,
                ~ first(.x))) %>%
  arrange(desc(smr02_feasible_gestation), .by_group = TRUE) %>%
  slice(1) %>%
  ungroup()

# bind all data frames together

data_smr02 <- bind_rows(data_smr02_livebirths_and_stillbirths, data_smr02_miscarriage_ectopic_and_termination_1) %>%
  select(
    smr02_baby_upi_number,
    #smr02_baby_chi_number,
    smr02_upi_number,
    smr02_outcome_type,
    smr02_baby,
    smr02_estimated_conception_date,
    smr02_estimated_gestation,
    everything()) %>%
  mutate(smr02_assumed_gestation = if_else(smr02_feasible_gestation == F, 1, 0)) %>%
  select(-c(smr02_feasible_gestation, event, revised_conception_date)) %>%
  arrange(smr02_upi_number, smr02_admission_date) %>%
  ungroup() %>%
  rowwise() %>% mutate(event_id = UUIDgenerate()) %>% ungroup()


##duplicate baby chi to dummy chi.
duplicate_baby_chi <- data_smr02 %>% filter(duplicated(smr02_baby_upi_number)) %>%
  pull(., smr02_baby_upi_number)

data_smr02 <- data_smr02 %>%
  mutate(smr02_baby_upi_number = case_when(smr02_baby_upi_number %in% duplicate_baby_chi ~
                                             paste0("53",str_pad(string = row_number(),
                                                                 width = 8,
                                                                 side = "left",
                                                                 pad = "0")),
                                          T~  smr02_baby_upi_number))


write_rds(data_smr02, paste0(folder_temp_data, "smr02_data.rds"), compress="gz")



rm(data_smr02, smr02_data,
   data_smr02_livebirths_and_stillbirths,
   data_smr02_miscarriage_ectopic_and_termination,
   data_smr02_miscarriage_ectopic_and_termination_1)



