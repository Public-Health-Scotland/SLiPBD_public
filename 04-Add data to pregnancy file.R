# Script 4 Add full data to pregnancy file -------------------------------------
log_print("Start script 4: Import source data to add details to pregnancy file")
# in this script we get our definitive values from the various datasets for each event 
# variables to get definitive values (see SLIPBD file layout)

births <- readRDS(paste0(folder_temp_data, "slipbd_births_updates.rds")) %>%  rename_with(~ paste0("births_", .)) # 
smr02_live_births <- readRDS( paste0(folder_temp_data, "smr02_data.rds")) %>%   
  filter(smr02_outcome_type == "Live birth" |smr02_outcome_type == "Stillbirth" )
tops <- readRDS(paste0(folder_temp_data, "tops_aas.rds"))
smr02 <- readRDS( paste0(folder_temp_data, "smr02_nonlive.rds"))
smr01 <- readRDS(paste0(folder_temp_data, "smr01.rds"))
an_booking <- readRDS( paste0(folder_temp_data, "antenatal_booking.rds"))

upis <- read_rds(paste0(folder_temp_data, "mother_upis_dob_from_CHI.rds"))

chi_postcode <-  readRDS(paste0(folder_documentation, "CHI_historic_bookingpostcode.rds"))#
chi_postcode <- chi_postcode %>% group_by(smr02_upi_number,smr02_date_of_booking) %>% slice(1) %>% select(-SERIAL)


# import file for baby ethnicity. 
names_files <- list.files(baby_ethnicity_path , pattern ="Births_Ethnicity" )

df <- data.frame()
for(i in 1:length(names_files)){
  df1 <- read.csv(paste0(baby_ethnicity_path, names_files[i]))
  df <- rbind(df, df1)
}
baby_ethnicity <- df  
baby_ethnicity <-baby_ethnicity %>% mutate(chi = phsmethods::chi_pad(as.character(chi))) %>% 
  mutate(chi_check = phsmethods::chi_check(as.character(chi)))

##always the same ethnicity if any present so just group by and take the first none NA value
baby_ethnicity <-baby_ethnicity %>% group_by(chi) %>%
  summarise(ethnicity = first_(ethnicity))

pregnancies1 <- read_rds(paste0(folder_temp_data, "script3_pregnancy_record.rds"))
##issue in script 3 is causing extra variables for linked record uuids to be created as lists
# rare and dont always appear as lists, so unlisting is conditional
if(is.list(pregnancies1$SMR02)){
  pregnancies1 <-pregnancies1 %>% mutate(SMR02 = unlist(purrr::map_depth(SMR02, 1, ~ifelse(is.null(.x), NA, .x) )))
}
if(is.list(pregnancies1$SMR01)){
  pregnancies1 <-pregnancies1 %>% mutate(SMR01 = unlist(purrr::map_depth(SMR01, 1, ~ifelse(is.null(.x), NA, .x) )))
}
if(is.list(pregnancies1$ToPs)){
  pregnancies1 <-pregnancies1 %>% mutate(ToPs = unlist(purrr::map_depth(ToPs, 1, ~ifelse(is.null(.x), NA, .x) )))
}
if(is.list(pregnancies1$slipbd_births_file)){
  pregnancies1 <-pregnancies1 %>% mutate(slipbd_births_file = unlist(purrr::map_depth(slipbd_births_file, 1, ~ifelse(is.null(.x), NA, .x) )))
}
##assign the variables to the correct field 
pregnancies1 <-pregnancies1 %>%
  dplyr::bind_rows(dplyr::tibble(SMR01 = character(), SMR02=character(),
                                 ToPs=character(), slipbd_births_file=character() )) %>%  #add vars in case they dont exisit in another iteration (as rare). This prevents the run failing in case these variables don't exist
  mutate(SMR02_1 = case_when(is.na(SMR02_1)~ SMR02, 
                             T~SMR02_1), 
         ToPs_1 = case_when(is.na(ToPs_1)~ ToPs, 
                            T~ToPs_1), 
         slipbd_births_file_1 = case_when(is.na(slipbd_births_file_1)~slipbd_births_file,
                                         T~slipbd_births_file_1)) %>%
  select(-SMR02, -ToPs, -slipbd_births_file, -SMR01)

log_print("Join details to pregnancy file")
# this makes a really wide file so we should have definitive values for every pregnancy
pregnancies1 <- pregnancies1 %>% 
  left_join(births, by= c("slipbd_births_file_1" = "births_baby_id")) %>%
  left_join(nrs_all_births, by=c( "births_nrs" = "nrs_event_id")) %>%
  left_join(tops, by=c("ToPs_1" = "aas_event_id")) %>%
  left_join(smr01 , by=c("SMR01_1" = "event_id")) %>%  
  left_join(an_booking, by=c("anbooking_1" = "anbooking_event_id")) 


# bind smr02 data separately depending on outcome to remove duplication of certain columns (.x and .y)
pregnancies_births <- pregnancies1 %>%
  filter(outcome1 == "Live birth" | outcome1 == "Stillbirth" ) %>%
  left_join(smr02_live_births, by=c("births_smr02_live_births" = "event_id" ))

pregnancies_other_outcomes <- pregnancies1 %>%
  filter(outcome1 != "Live birth" & outcome1 != "Stillbirth") %>%
  left_join(smr02, by=c("SMR02_1"="event_id"))

pregnancies1 <- pregnancies_births %>%
  bind_rows(pregnancies_other_outcomes) %>%
  arrange(mother_upi, pregnancy_start_date)


rm(tops,smr01, smr02_live_births, nrs_all_births)
gc()

# FIX ANTENATAL BOOKINGS -------------------------------------------------------
log_print("Fix antenatal booking issues")

# find overlapping pregnancies where we have an orphan antenatal booking record overlapping another pregnancy
overlapping_antenatal_bookings <- pregnancies1 %>%
  select(mother_upi, pregnancy_id, anbooking_1, outcome1, outcome2, pregnancy_start_date, pregnancy_end_date, anbooking_booking_date) %>%
  filter(anbooking_booking_date >= as.Date("2019-04-01")) %>%
  group_by(mother_upi) %>% 
  filter(n() > 1 & "Unknown" %in% outcome1) %>%
  arrange(mother_upi, pregnancy_end_date) %>%
  mutate(ab_overlap_flag = case_when(outcome1 == "Unknown" & 
                                       anbooking_booking_date >= lag(as.Date(pregnancy_start_date)) & 
                                       anbooking_booking_date <= lag(as.Date(pregnancy_end_date)) ~ 1, 
                                     T ~ 0)) %>% 
  filter(1 %in% ab_overlap_flag) %>% 
  mutate(preg_start_date_diff = difftime(pregnancy_start_date, lag(pregnancy_start_date), units = "days"), 
         diff_pregnancy = case_when(as.numeric(preg_start_date_diff) > 83 ~ 1, 
                                    T ~ 0)) %>%
  group_by(mother_upi, diff_pregnancy) %>%
  mutate(new_pregnancy_id = first_(pregnancy_id)) %>% 
  rename(old_pregnancy_id = pregnancy_id ) %>%
  filter(outcome1 == "Unknown" & diff_pregnancy == 0) %>%
  ungroup() %>%
  select(mother_upi, old_pregnancy_id, new_pregnancy_id, anbooking_1)

t <- overlapping_antenatal_bookings %>% group_by(mother_upi) %>% filter(n() > 1)

anbooking_cols <- colnames(an_booking) 
anbooking_cols <- anbooking_cols[-length(anbooking_cols)]

# remove the unknown pregnancies that have an antenatal booking during a pregnancy 
# select the antenatal booking record associated with the overlapping pregnancy as this is likely the right one 
# rejoin the antenatal booking records 
pregnancies1 <- pregnancies1 %>% 
  filter(!(pregnancy_id %in% overlapping_antenatal_bookings$old_pregnancy_id)) %>%
  left_join(., overlapping_antenatal_bookings, by = c("mother_upi", "pregnancy_id" = "new_pregnancy_id")) %>%
  select(-(anbooking_cols)) %>% 
  mutate(anbooking_1 = case_when(!is.na(anbooking_1.y) ~ anbooking_1.y, 
                                 T ~ anbooking_1.x)) %>%
  select(-c(old_pregnancy_id, anbooking_1.x, anbooking_1.y)) %>%
  left_join(an_booking, by=c("anbooking_1" = "anbooking_event_id"))

# deal with isolated bookings that have a pregnancy followed shortly after it 
log_print("Deal with presumed early losses")
pregnancies1 %<>% 
  #create a temporary end date for unknown outcomes, 
  #otherwise 2 consecutive unknown outcomes cannot be compared properly.
  mutate(pregnancy_end_date =
           case_when(outcome1 %in% c("Unknown", "Unknown - emigrated") ~ pregnancy_start_date + (38*7),
                     T~pregnancy_end_date)) %>%
  group_by(mother_upi) %>% 
  arrange(mother_upi, pregnancy_start_date) %>%
  mutate(event_date_difference = 
           case_when(outcome1 %in% c("Unknown", "Unknown - emigrated")  ~ 
                       difftime(lead(pregnancy_end_date), pregnancy_start_date, units = "days"))) %>%
  ungroup()


pregnancies1 <- pregnancies1 %>%
  mutate(week_diff = floor(event_date_difference/7)) %>%
  mutate(outcome1 = case_when(is.na(week_diff) ~ outcome1,
                              outcome1 == "Unknown" & week_diff < 40 ~ "Unknown - assumed early loss",
                              outcome1 == "Unknown" & week_diff >= 40 ~ outcome1,
                              T~outcome1)) %>%  select(-c(event_date_difference, week_diff))  %>%
  mutate(pregnancy_end_date = 
           case_when(outcome1 %in% c("Unknown", "Unknown - assumed early loss", "Unknown - emigrated")~NA,
                     T~pregnancy_end_date))


# ADD DEFINITIVE VALUES --------------------------------------------------------
log_print("Adding definitive values")
### Maternal Date of Birth ------------------------------------------------------
pregnancies1 <- left_join(pregnancies1 , chi_postcode)
pregnancies1 <- pregnancies1 %>%
  rename(chili_booking_postcode = POSTCODE ) %>% 
  mutate(chili_booking_postcode = format_postcode(chili_booking_postcode, "pc7") )

pregnancies4 <- pregnancies1 %>%
  mutate(chi_check = chi_check(mother_upi)) %>% 
  left_join(., upis, by = c("mother_upi" = "upi")) %>%
  mutate(x_mother_dob = coalesce(chi_dob, 
                                 smr02_dob, 
                                 smr01_dob, 
                                 aas_date_of_birth, 
                                 anbooking_mothers_dob
  ) )


gc()
##clean up duplicate outcomes
pregnancies4 <- pregnancies4 %>% mutate(outcome2 = case_when(outcome2==outcome1 ~ NA, T~outcome2))

#fix stillbirth mislabeling caused by live/still/termination outcomes on multiple pregnancies
pregnancies4 <- pregnancies4 %>%
  mutate(outcome2 = case_when(outcome1 =="Termination" & 
                                substr(nrs_triplicate_id,1,1)=="S" ~ "Stillbirth", 
                              T~ outcome2))%>%
  mutate(outcome1 = case_when(outcome1 !="Termination" & 
                                outcome1 !="Stillbirth" &
                                substr(nrs_triplicate_id,1,1)=="S" ~ "Stillbirth", 
                              T~ outcome1))
### Maternal Death During Pregnancy ----
pregnancies4 %<>% 
  rename(x_maternal_death_in_pregnancy = death_in_pregnancy) %>%
  mutate(x_maternal_death_in_pregnancy = case_when(is.na(x_maternal_death_in_pregnancy)~0,
                                                   T~x_maternal_death_in_pregnancy)) %>%
  rename(x_maternal_death_postpartum = death_post_pregnancy) %>% 
    mutate(x_maternal_death_postpartum = case_when(is.na(x_maternal_death_postpartum)~0,
                                                   T~x_maternal_death_postpartum)) %>%
  rename(x_maternal_date_of_death = DATE_OF_DEATH) %>%
  mutate(any_maternal_death = case_when(x_maternal_death_in_pregnancy==1 | x_maternal_death_postpartum ==1 ~1,T~0))

### Maternal Emigration during Pregnancy ----

pregnancies4 %<>% 
  rename(x_maternal_emigration_code = TRANSFER_OUT_CODE, 
         x_date_of_maternal_emigration = DATE_TRANSFER_OUT) %>%
  mutate(x_maternal_emigration_during_pregnancy = case_when(!is.na(x_date_of_maternal_emigration) ~ "Yes", 
                                                            T ~ "No"))

## Record of antenatal booking -------------------------------------------------
log_print("Adding definitive values : antenatal booking values")
pregnancies4 %<>%
  mutate(smr02_date_of_booking = case_when(smr02_date_of_booking < as.Date("1999-03-01")~ NA, # clean smr02 booking dates
                                                           T~smr02_date_of_booking)) %>%
  mutate(x_record_of_ab  = case_when(!is.na(anbooking_booking_date) ~ "Yes", 
                                    !is.na(smr02_date_of_booking) ~ "Yes", 
                                    T ~ "No"))

## Date of antenatal booking ---------------------------------------------------
# From SMR02 delivery record for women booking up to 31.3.2019,
#from ABC (or SMR02 if missing from ABC) for women booking from 1.4.2019
pregnancies4 %<>% 
  mutate(x_antenatal_booking_date = coalesce(anbooking_booking_date, 
                                             as.Date(smr02_date_of_booking)))%>% 
  mutate(x_antenatal_booking_date = as_date(x_antenatal_booking_date))

## Gestation at antenatal booking ----------------------------------------------
pregnancies4 %<>%   ####Compute gestation at booking 
#remove impossible booking dates (move to script 2 for monthly runs)
 mutate(smr02_date_of_booking =  case_when(smr02_date_of_booking <= as.Date("1999-03-31") ~ NA, 
                                            smr02_date_of_booking <= smr02_estimated_conception_date ~ NA,
                                            smr02_date_of_booking > smr02_pregnancy_end_date ~ NA,
                                            T~smr02_date_of_booking)) %>%
  mutate(smr02_gestation_at_booking = case_when(!is.na(smr02_date_of_booking) ~
                                                  as.numeric(floor(difftime(smr02_date_of_booking,smr02_estimated_conception_date, units="weeks")))+2 )) %>%
  mutate(smr02_gestation_at_booking= case_when(smr02_gestation_at_booking %in% feasible_gestation_booking ~ smr02_gestation_at_booking, 
                                              T~NA))

pregnancies4 %<>% 
  mutate(x_gestation_at_anbooking = coalesce(anbooking_gestation_at_booking, smr02_gestation_at_booking))



### Maternal postcode at booking -----------------------------------------------
log_print("Adding definitive values : postcode values")
# priority to an booking postcode.
# if using chi postcode check dates

pregnancies4 %<>% 
  mutate(x_postcode_at_booking = case_when(chi_current_postcode == anbooking_pc7 ~ chi_current_postcode, 
                                           chi_previous_postcode == anbooking_pc7 ~ chi_previous_postcode, 
                                           !is.na(smr02_date_of_booking) & smr02_date_of_booking <= as.Date("2019-03-31") ~ chili_booking_postcode, # to be added in the refresh when we have historic chili pc
                                           !is.na(anbooking_pc7) ~ anbooking_pc7, 
                                           #if no anbooking postcode then use definitive booking date plus chi postcode
                                           !is.na(x_antenatal_booking_date) & x_antenatal_booking_date >= as.Date(chi_date_postcode_change) ~ chi_current_postcode, 
                                           !is.na(x_antenatal_booking_date) & x_antenatal_booking_date <= as.Date(chi_date_postcode_change) ~ chi_previous_postcode, 
                                           T ~ NA)) %>% 
  mutate(x_postcode_at_booking = format_postcode(x_postcode_at_booking, format = "pc7"))

### Maternal postcode at end of pregnancy --------------------------------------

pregnancies4 %<>% 
  mutate(x_postcode_end_pregnancy = coalesce(nrs_postcode,
                                             smr02_dr_postcode,
                                             aas_postcode,
                                             smr01_postcode)) %>%
  mutate(x_postcode_end_pregnancy = format_postcode(x_postcode_end_pregnancy, format = "pc7"))


## Determine Pregnancy End Date ------------------------------------------------
log_print("Adding definitive values : pregnancy dates and gestations")
##impute end date for unknown outcomes,
pregnancies4 <- pregnancies4 %>%
  mutate(imputed_end_date = 
           case_when(outcome1== "Unknown" | outcome1=="Unknown - emigrated" ~ anbooking_estimated_conception_date + (38*7), 
                     outcome1=="Unknown - assumed early loss" & 
                       x_antenatal_booking_date > 
                          (anbooking_estimated_conception_date + (8*7)) ~ x_antenatal_booking_date,
                        x_antenatal_booking_date <=
                          (anbooking_estimated_conception_date + (8*7))~ 
                          anbooking_estimated_conception_date + (8*7))) %>%
  mutate(imputed_end_date = 
           case_when(outcome1=="Unknown - emigrated" & imputed_end_date > Sys.Date() ~ NA, 
                     T ~ imputed_end_date))


pregnancies4 <- pregnancies4 %>%
  mutate(x_pregnancy_end_date = coalesce(
      as.Date(nrs_date_of_birth),
      as.Date(smr02_pregnancy_end_date),
      as.Date(aas_date_of_termination),
      as.Date(smr01_cis_admission_date), 
      as.Date(x_maternal_date_of_death), 
      as.Date(imputed_end_date)
  )) %>%
  mutate(x_pregnancy_end_date = as_date(x_pregnancy_end_date)) # Remove any remaining datetime objects, converting to dates

## Determine gestation at outcome ----------------------------------------------

pregnancies4 %<>% 
  # calculates gestation at end of pregnancy from antenatal booking gestation variable
  mutate(an_calc_gestation = case_when(!is.na(anbooking_gestation_at_booking) & anbooking_assumed_gestation == FALSE ~ 
                                         as.numeric(floor(difftime(x_pregnancy_end_date, anbooking_booking_date, units="weeks")) 
                                                    + anbooking_gestation_at_booking))) 
  

# calculating gestation at outcome
# uses a hierarchy: 
# - if we have gestation of pregnancy in nrs/smr02/aas which has not been imputed then use that
# - if we don't, we use the gestation calculated from antenatal booking 
# (so long as it is within the feasible gestation range) 
# - if we don't have that we use the imputed gestation from nrs/smr02/aas/smr01
# for unknown pregnancies, we automatically set gestation to 40 weeks 
# if we have an unknown pregnancy - emigrated outcome, gestation is set to the
#time between pregnancy end date and today's date if less than 44 weeks - otherwise set to 44 weeks
# note we do not have gestation at outcome for any ongoing pregnancies as their
#is no outcome yet 
pregnancies4 %<>% 
  mutate(x_gestation_at_outcome = case_when(!is.na(nrs_duration_of_pregnancy) & nrs_assumed_gestation == 0
                                            ~ nrs_duration_of_pregnancy,
                                            !is.na(smr02_estimated_gestation) & smr02_assumed_gestation == FALSE
                                            ~ smr02_estimated_gestation,
                                            !is.na(aas_estimated_gestation) & aas_assumed_gestation == 0
                                            ~ aas_estimated_gestation,
                                            (outcome1 == "Live birth" | outcome2=="Live birth") & an_calc_gestation %in% feasible_gestation_lb
                                            ~ an_calc_gestation,
                                            (outcome1 == "Stillbirth"| outcome2=="Stillbirth") & an_calc_gestation %in% feasible_gestation_sb
                                            ~ an_calc_gestation,
                                            outcome1 == "Termination" & an_calc_gestation %in% feasible_gestation_termination
                                            ~ an_calc_gestation,
                                            (outcome1 == "Molar pregnancy" | outcome1 == "Ectopic pregnancy" | outcome1 == "Miscarriage") 
                                            & an_calc_gestation %in% feasible_gestation_miscarriage
                                            ~ an_calc_gestation,
                                            !is.na(nrs_duration_of_pregnancy) ~ nrs_duration_of_pregnancy,
                                            !is.na(smr02_estimated_gestation) ~ smr02_estimated_gestation,
                                            !is.na(aas_estimated_gestation) ~ aas_estimated_gestation,
                                            !is.na(smr01_gestation) ~ smr01_gestation, 
                                            outcome1 == "Maternal death" ~ as.numeric(floor(difftime(x_maternal_date_of_death, pregnancy_start_date, units = "weeks")))+2,
                                            outcome1 == "Unknown" ~ 40, 
                                            outcome1 == "Unknown - assumed early loss" ~ max_(x_gestation_at_anbooking,10), 
                                            outcome1 == "Unknown - emigrated" & as.numeric(difftime(Sys.Date(), pregnancy_start_date, units = "weeks"))+2 < 40 ~ floor(as.numeric(difftime(Sys.Date(), pregnancy_start_date, units = "weeks"))), 
                                            outcome1 == "Unknown - emigrated" & as.numeric(difftime(Sys.Date(), pregnancy_start_date, units = "weeks"))+2 >= 40 ~ 40))

## Determine gestation ascertainment -------------------------------------------
pregnancies4 %<>% 
  # is the gestation recorded in a dataset (only used when not assumed)
  mutate(x_recorded_gestation = case_when(!is.na(nrs_duration_of_pregnancy) & nrs_assumed_gestation == 0 |
                                            !is.na(smr02_estimated_gestation) & smr02_assumed_gestation == 0 |
                                            !is.na(aas_estimated_gestation) & aas_assumed_gestation == 0 ~ TRUE,
                                          T ~ FALSE)) %>% 
  # is the calculated gestation from antenatal booking feasible
  mutate(x_an_calc_gest = case_when(x_recorded_gestation == FALSE & 
                                      ((outcome1 == "Live birth" & an_calc_gestation %in% feasible_gestation_lb) |
                                         (outcome1 == "Stillbirth" & an_calc_gestation %in% feasible_gestation_sb) |
                                         (outcome1 == "Termination" & an_calc_gestation %in% feasible_gestation_termination) |
                                         ((outcome1 == "Molar pregnancy" | outcome1 == "Ectopic pregnancy" | outcome1 == "Miscarriage") 
                                          & an_calc_gestation %in% feasible_gestation_miscarriage)) ~ TRUE,
                                    T~ FALSE)) %>% 
  mutate(x_assumed_gestation = case_when(outcome1 != "Ongoing" & x_recorded_gestation == FALSE & x_an_calc_gest == FALSE
                                         ~ TRUE,
                                         T ~ FALSE)
  ) %>% 
  mutate(x_gestation_ascertainment = case_when(x_recorded_gestation == TRUE ~ "Gestation recorded on end of pregnancy record",
                                               x_an_calc_gest == TRUE ~ "Gestation calculated from gestation at booking",
                                               x_assumed_gestation == TRUE | str_starts(outcome1, "Unknown") ~ "Gestation imputed based on outcome of pregnancy",
                                               outcome1 == "Ongoing" ~ "Ongoing pregnancy")) %>% 
  select(-c(x_recorded_gestation, x_an_calc_gest, x_assumed_gestation, an_calc_gestation))

## Determine date of conception ------------------------------------------------

pregnancies4 %<>% 
 # rowwise() %>%
  mutate(x_est_conception_date = case_when(outcome1 == "Ongoing" 
                                           ~ as_date(anbooking_estimated_conception_date),
                                           outcome1 %in% c("Unknown", "Unknown - emigrated","Unknown - assumed early loss", "Maternal death")
                                           ~ as_date(anbooking_estimated_conception_date),
                                           outcome1 != "Ongoing" 
                                           ~ as_date(x_pregnancy_end_date - (weeks(x_gestation_at_outcome) - weeks(2))))) 

## Final redetermination of unknown assumed losses end dates to prevent overlapping pregnancies------------------------------------

pregnancies4 <- pregnancies4 %>% 
  group_by(mother_upi) %>%
  arrange(x_est_conception_date) %>%
  mutate(flag_overlap = case_when(pregnancy_id != lead(pregnancy_id )&
                                    x_pregnancy_end_date >= lead(x_est_conception_date) ~1, T~0)) %>%
  mutate(diffoverlap = case_when(flag_overlap==1 ~ x_pregnancy_end_date - lead(x_est_conception_date))) %>% 
  ungroup() 
table(pregnancies4$flag_overlap, pregnancies4$outcome1)
table(pregnancies4$x_gestation_at_outcome, pregnancies4$outcome1)

pregnancies4 <- pregnancies4  %>%
  group_by(mother_upi) %>%
  mutate(x_pregnancy_end_date  = case_when((outcome1 == "Unknown" | outcome1 == "Unknown - assumed early loss" ) &
                                             flag_overlap==1~ 
                                             lead(x_est_conception_date) -14,
  T~x_pregnancy_end_date)) %>%  
  mutate(x_gestation_at_outcome = case_when((outcome1 == "Unknown" | outcome1 == "Unknown - assumed early loss" ) &
                                              flag_overlap==1 & !is.na(x_pregnancy_end_date) ~ 
                                              floor(as.numeric(difftime(x_pregnancy_end_date, pregnancy_start_date, units = "weeks"))+2) , 
                                            T~ x_gestation_at_outcome )) %>%
  group_by(mother_upi) %>%
  arrange(x_est_conception_date) %>%
  mutate(flag_overlap = case_when(pregnancy_id != lead(pregnancy_id )&
                                                                  x_pregnancy_end_date >= lead(x_est_conception_date) ~1, T~0)) %>%
  mutate(diffoverlap = case_when(flag_overlap==1 ~ x_pregnancy_end_date - lead(x_est_conception_date))) %>% 
  ungroup() 
 
## Maternal Age at conception --------------------------------------------------

pregnancies4 %<>% 
  mutate(x_mother_age_at_conception = floor(as.numeric(difftime(x_est_conception_date, x_mother_dob, units="weeks"))
                                            / 52.25)) %>% 
  mutate(x_mother_age_at_conception = case_when(x_mother_age_at_conception %in% feasible_age ~ x_mother_age_at_conception,
                                                T ~ NA_real_))

## Maternal Age at end of pregnancy --------------------------------------------
pregnancies4 %<>% 
  mutate(x_mother_age_at_outcome = floor(as.numeric(difftime(x_pregnancy_end_date, x_mother_dob, units="weeks"))/ 52.25)) %>% 
  mutate(x_mother_age_at_outcome = case_when(x_mother_age_at_outcome %in% feasible_age ~ x_mother_age_at_outcome,
                                             T ~ NA_real_))

## Determine number of fetuses/babies in this pregnancy-------------------------
# number of fetuses equal the number of rows per pregnancy 
# number of births comes from nrs all births (combined nrs live births and nrs still births) and smr02 (live births) 
# if no birth - assume fetus is 1 
pregnancies4 %<>% 
  group_by(pregnancy_id) %>%
  mutate(x_fetuses_this_pregnancy = n()) %>% # 
  mutate(x_births_this_pregnancy = max_(c(nrs_total_births_live_and_still, smr02_num_of_outcomes_this_pregnancy))) %>%
  ungroup()


## Determining baby/fetus outcome ----------------------------------------------------
log_print("Adding definitive values : fetus outcomes")
pregnancies4 %<>% 
  rename(x_fetus_outcome_1 = outcome1, 
         x_fetus_outcome_2 = outcome2)

# remove dummy chis from stillbirths
# ensure all live births have a chi (if na, make it a dummy chi)
pregnancies4 %<>% 
  mutate(births_baby_upi = case_when(x_fetus_outcome_1 == "Stillbirth" | x_fetus_outcome_2 == "Stillbirth" ~ NA_character_, 
                                     (x_fetus_outcome_1 == "Live birth" | x_fetus_outcome_2 == "Live birth") & is.na(births_baby_upi) ~ 
                                       paste0("71", str_pad(string = row_number(), width = 8, side = "left", pad = "0")), 
                                     T ~ births_baby_upi))

## Determining additional baby/fetus outcome -----------------------------------

pregnancies4 %<>%
  group_by(pregnancy_id) %>%
  mutate(x_fetus_id = seq(1:n())) %>% ungroup()

pregnancies_multiples <- pregnancies4 %>% 
  select(pregnancy_id, mother_upi, x_pregnancy_end_date, x_gestation_at_outcome, x_fetus_outcome_1, 
         x_fetus_outcome_2, x_fetuses_this_pregnancy, x_births_this_pregnancy, x_fetus_id) %>%
  filter(x_fetuses_this_pregnancy > 1) %>%
  left_join(., 
            pregnancies4 %>% 
              select(pregnancy_id, mother_upi, x_pregnancy_end_date, x_gestation_at_outcome, x_fetus_outcome_1, 
                     x_fetus_outcome_2, x_fetuses_this_pregnancy, x_births_this_pregnancy, x_fetus_id) %>%
              filter(x_fetuses_this_pregnancy > 1) %>%
              group_by(pregnancy_id) %>% 
              pivot_wider(names_from = x_fetus_id, values_from = c(x_fetus_outcome_1, x_fetus_outcome_2), id_cols = pregnancy_id)) %>%
 dplyr::bind_rows(dplyr::tibble(x_fetus_outcome_1_4=character(),x_fetus_outcome_2_4=character() )) %>%  #adds baby 4 variables if dont exist
  mutate(additional_fetus_outcome1 = case_when(x_fetus_id == 1 ~ paste(x_fetus_outcome_1_2, x_fetus_outcome_1_3, x_fetus_outcome_1_4, sep = "/"), 
                                               x_fetus_id == 2 ~ paste(x_fetus_outcome_1_1, x_fetus_outcome_1_3, x_fetus_outcome_1_4, sep = "/"), 
                                               x_fetus_id == 3 ~ paste(x_fetus_outcome_1_1, x_fetus_outcome_1_2, x_fetus_outcome_1_4, sep = "/"), 
                                               x_fetus_id == 4 ~ paste(x_fetus_outcome_1_1, x_fetus_outcome_1_2, x_fetus_outcome_1_3, sep = "/")), 
         additional_fetus_outcome1 = gsub("/NA", "", additional_fetus_outcome1), 
         additional_fetus_outcome2 = case_when(x_fetus_id == 1 ~ paste(x_fetus_outcome_2_2, x_fetus_outcome_2_3, x_fetus_outcome_2_4, sep = "/"), 
                                               x_fetus_id == 2 ~ paste(x_fetus_outcome_2_1, x_fetus_outcome_2_3, x_fetus_outcome_2_4, sep = "/"), 
                                               x_fetus_id == 3 ~ paste(x_fetus_outcome_2_1, x_fetus_outcome_2_2, x_fetus_outcome_2_4, sep = "/"), 
                                               x_fetus_id == 4 ~ paste(x_fetus_outcome_1_1, x_fetus_outcome_2_2, x_fetus_outcome_2_3, sep = "/")), 
         additional_fetus_outcome2 = gsub("/NA", "", additional_fetus_outcome2), 
         additional_fetus_outcome2 = gsub("NA", NA, additional_fetus_outcome2)) 

pregnancies4 %<>% 
  left_join(., pregnancies_multiples)

## Determining pregnancy Outcome 
# will be the same as outcome1/outcome 2 for singleton pregnancies and
#multiples where all babies have the same outcome 

pregnancies4 %<>%
  mutate(x_pregnancy_outcome1 = case_when(x_fetuses_this_pregnancy == 1 ~ x_fetus_outcome_1, 
                                          x_fetuses_this_pregnancy == 2 & x_fetus_outcome_1_1 == x_fetus_outcome_1_2 ~ x_fetus_outcome_1_1, 
                                          x_fetuses_this_pregnancy == 3 & (x_fetus_outcome_1_1 == x_fetus_outcome_1_2 & 
                                                                             x_fetus_outcome_1_2 == x_fetus_outcome_1_3) ~ x_fetus_outcome_1_1, 
                                          x_fetuses_this_pregnancy == 4 & (x_fetus_outcome_1_1 == x_fetus_outcome_1_2 & 
                                                                             x_fetus_outcome_1_2 == x_fetus_outcome_1_3 & x_fetus_outcome_1_3 == x_fetus_outcome_1_4) ~ x_fetus_outcome_1_1, 
                                          T ~ "Discordant Outcomes"), 
         x_pregnancy_outcome2 = case_when(x_fetuses_this_pregnancy == 1 ~ x_fetus_outcome_2, 
                                          x_fetuses_this_pregnancy == 2 & x_fetus_outcome_2_1 == x_fetus_outcome_2_2 ~ x_fetus_outcome_2_1, 
                                          x_fetuses_this_pregnancy == 3 & (x_fetus_outcome_2_1 == x_fetus_outcome_2_2 & 
                                                                             x_fetus_outcome_2_2 == x_fetus_outcome_2_3) ~ x_fetus_outcome_2_1, 
                                          x_fetuses_this_pregnancy == 4 & (x_fetus_outcome_2_1 == x_fetus_outcome_2_2 & 
                                                                             x_fetus_outcome_2_2 == x_fetus_outcome_2_3 & x_fetus_outcome_2_3 == x_fetus_outcome_2_4) ~ x_fetus_outcome_2_1, 
                                          T ~ "Discordant Outcomes"))


## Defining maternal ethnicity -------------------------------------------------
# category relevant to date of event. 

pregnancies4 <- pregnancies4 %>%
  mutate(smr02_ethnic_group = if_else(smr02_ethnic_group == "98" | smr02_ethnic_group == "99", NA_character_, smr02_ethnic_group)) %>% 
  mutate(smr01_ethnic_group = if_else(smr01_ethnic_group == "98" | smr01_ethnic_group == "99", NA_character_, smr01_ethnic_group)) %>% 
  mutate(anbooking_ethnicity = if_else(anbooking_ethnicity == "98" | anbooking_ethnicity == "99", NA_character_, anbooking_ethnicity)) %>% 
  mutate(x_ethnicity_code = coalesce(smr02_ethnic_group, 
                                     smr01_ethnic_group,
                                     anbooking_ethnicity), 
         x_ethnicity_code = case_when(is.na(x_ethnicity_code) ~ "unknown", 
                                      T ~ x_ethnicity_code)) %>%
  mutate(mother_ethnicity = reporting_ethnicity(x_ethnicity_code)) 

## Previous pregnancies --------------------------------------------------------
# Total number of pregnancies prior to this current pregnancy regardless of 
# pregnancy outcome; multiple pregnancies count as one pregnancy.  
# Available on SMR02 and ToPSS records (and in future ANB)

pregnancies4 %<>% 
  mutate(smr02_total_previous_pregnancies = case_when(smr02_total_previous_pregnancies == 99 ~ NA_real_, 
                                                      T ~ smr02_total_previous_pregnancies), 
         smr02_previous_spontaneous_abortions = case_when(smr02_previous_spontaneous_abortions == 99 ~ NA_real_, 
                                                          T ~ smr02_previous_spontaneous_abortions), 
         smr02_previous_theraputic_abortions = case_when(smr02_previous_theraputic_abortions == 99 ~ NA_real_, 
                                                         T ~ smr02_previous_theraputic_abortions), 
         smr02_total_previous_pregnancies = case_when(smr02_total_previous_pregnancies < smr02_previous_spontaneous_abortions + smr02_previous_theraputic_abortions ~ 
                                                        smr02_total_previous_pregnancies + smr02_previous_spontaneous_abortions + smr02_previous_theraputic_abortions, 
                                                      T ~ smr02_total_previous_pregnancies), 
         aas_total_number_of_pregnancies = case_when(aas_total_number_of_pregnancies < aas_spontaneous_abortions + aas_therapeutic_abortions ~ 
                                                       aas_total_number_of_pregnancies + aas_spontaneous_abortions + aas_therapeutic_abortions, 
                                                     T ~ smr02_total_previous_pregnancies)) %>%
  mutate(x_previous_pregnancies = coalesce(smr02_total_previous_pregnancies, 
                                           aas_total_number_of_pregnancies)) %>% 
  group_by(pregnancy_id) %>%
  arrange(x_previous_pregnancies) %>% 
  mutate(x_previous_pregnancies = max_(x_previous_pregnancies)) 

## Previous deliveries ---------------------------------------------------------
# "Number of pregnancies prior to this current pregnancy that ended in delivery
# of a live and/or stillbirth; multiple pregnancies count as one pregnancy 
# Derived from previous pregnancies - (previous spontaneous abortions + previous therapeutic abortions)? 

pregnancies4 %<>%
  mutate(x_previous_deliveries = coalesce(smr02_total_previous_pregnancies - (smr02_previous_spontaneous_abortions + smr02_previous_theraputic_abortions), 
                                          aas_total_number_of_pregnancies - (aas_spontaneous_abortions + aas_therapeutic_abortions))) %>% ungroup()


## NHS Board of care at booking appointment ------------------------------------
log_print("Adding definitive values : location values")
pregnancies4 %<>% 
  mutate(x_nhs_board_at_booking = anbooking_hbt2019name)

## Location of care at end of pregnancy ----------------------------------------

pregnancies4 %<>% 
  mutate(x_location_at_end_of_pregnancy = coalesce(nrs_institution,
                                                   smr02_location,
                                                   aas_hospital,
                                                   smr01_location))

## NHS Board of care at end of pregnancy ---------------------------------------

pregnancies4 %<>%
  left_join(., location %>% select(location, Postcode), by = c("x_location_at_end_of_pregnancy" = "location")) %>%
  left_join(., SPD %>% select(pc8, hb2019), by = c("Postcode" = "pc8")) %>%
  mutate(x_nhs_board_at_end_of_pregnancy = hb2019)

## Maternal height -------------------------------------------------------------
# In cm.  Currently available from SMR02, to be added to ABC

pregnancies4 %<>% 
  mutate(x_maternal_height = case_when(smr02_height == 99 & smr02_weight_of_mother == 99 ~ NA_real_, 
                                       T ~ smr02_height))

## Maternal weight -------------------------------------------------------------
#In kg. Currently available from SMR02, to be added to ABC

pregnancies4 %<>% 
  mutate(x_maternal_weight = smr02_weight_of_mother)

## Maternal bmi ----------------------------------------------------------------

pregnancies4 %<>% 
  mutate(x_bmi = round(smr02_weight_of_mother/((smr02_height/100) ^ 2)), 
         x_bmi = case_when(x_bmi %in% feasible_bmi ~ x_bmi, 
                           T ~ NA_real_), 
         x_maternal_height = case_when(is.na(x_bmi) ~ NA_real_, 
                                       T ~ x_maternal_height), 
         x_maternal_weight = case_when(is.na(x_bmi) ~ NA_real_, 
                                       T ~ x_maternal_weight)) 


## Induction of Labour ---------------------------------------------------------

pregnancies4 %<>% 
  mutate(smr02_induction_of_labour_category = case_when( smr02_induction_of_labour == 0 ~ "N",
                                                         smr02_induction_of_labour >= 1 & smr02_induction_of_labour <= 8 ~ "Y",
                                                         T ~ "U"), 
         x_induction_of_labour = smr02_induction_of_labour)

## Presentation at delivery ----------------------------------------------------

pregnancies4 %<>% 
   mutate(x_presentation_at_delivery = smr02_presentation_at_delivery, 
          x_presentation_at_delivery = case_when(is.na(x_presentation_at_delivery) ~ NA, 
                                                 x_presentation_at_delivery %notin% c("1", "2", "3", "4", "5", "6", "7", "8", "9") ~ "9", 
                                                 T ~ x_presentation_at_delivery))

## Mode of delivery -----------------------------------------------------------
pregnancies4 %<>%
  mutate(smr02_mode_of_delivery_category = case_when(smr02_mode_of_delivery %in% c("0", "1", "2", "5", "6", "A", "B", "C", "D", "E") ~ "Vaginal",
                                                     smr02_mode_of_delivery == "7" ~ "Elective CS",
                                                     smr02_mode_of_delivery == "8" ~ "Emergency CS",
                                                     smr02_mode_of_delivery == "9" |
                                                       smr02_mode_of_delivery == "NA" |
                                                       is.na(smr02_mode_of_delivery) ~ "U"), 
         
         x_mode_of_delivery = smr02_mode_of_delivery)


## Onset of delivery -----------------------------------------------------------
pregnancies4 %<>%
  mutate(
    smr02_duration_of_labour_category = case_when(
      smr02_duration_of_labour == 0 ~ "0",
      smr02_duration_of_labour >= 1 & smr02_duration_of_labour <= 98 ~ ">0",
      T ~ "U"
    ),      
    smr02_premature_rupture_of_membrane = case_when(
      str_sub(smr02_indication_for_operative_del, 1, 3) == "O42" ~ T,
      str_sub(smr02_main_condition, 1, 3) == "O42" ~ T,
      str_sub(smr02_other_condition_1, 1, 3) == "O42" ~ T,
      str_sub(smr02_other_condition_2, 1, 3) == "O42" ~ T,
      str_sub(smr02_other_condition_3, 1, 3) == "O42" ~ T,
      str_sub(smr02_other_condition_4, 1, 3) == "O42" ~ T,
      str_sub(smr02_other_condition_5, 1, 3) == "O42" ~ T,
      T ~ F), 
    x_onset_of_delivery = case_when(
      smr02_premature_rupture_of_membrane == T ~ "Spontaneous",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category == "Y" ~ "Medically Indicated",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category %in% c("N", "U") & smr02_mode_of_delivery_category == "Vaginal" ~ "Spontaneous",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category %in% c("N", "U") & smr02_duration_of_labour_category == "0" & smr02_mode_of_delivery_category %in% c("Elective CS", "Emergency CS") ~ "Medically Indicated",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category %in% c("N", "U") & smr02_duration_of_labour_category == ">0" & smr02_mode_of_delivery_category %in% c("Elective CS", "Emergency CS") ~ "Spontaneous",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category %in% c("N", "U") & smr02_duration_of_labour_category == "U" & smr02_mode_of_delivery_category == "Elective CS" ~ "Medically Indicated",
      smr02_premature_rupture_of_membrane == F & smr02_induction_of_labour_category %in% c("N", "U") & smr02_duration_of_labour_category == "U" & smr02_mode_of_delivery_category == "Emergency CS" ~ "Spontaneous",
      T ~ "Undefined"), 
    x_onset_of_delivery = case_when(x_fetus_outcome_1 %in% c("Live birth", "Stillbirth") ~ x_onset_of_delivery, 
                                    x_fetus_outcome_2 %in% c("Live birth", "Stillbirth") ~ x_onset_of_delivery, 
                                    T ~ NA)
  )

## Perineal Tears --------------------------------------------------------------

pregnancies4 %<>% 
  mutate(x_perineal_tear = smr02_tears)

## Fetus/babu marker -----------------------------------------------------------
log_print("Adding definitive values : baby related values")
pregnancies4 %<>% 
  mutate(x_fetus_marker = smr02_baby)

## Baby Sex --------------------------------------------------------------------

pregnancies4 %<>% 
  mutate(x_baby_sex = coalesce(nrs_sex, 
                               smr02_sex)) 

## Baby Ethnicity --------------------------------------------------------------
# For live births only

pregnancies4 <-  pregnancies4 %>% left_join(baby_ethnicity, by = c("births_baby_upi" = "chi")) %>% 
  rename(x_baby_ethnicity_code = ethnicity) %>%
  mutate(baby_ethnicity = reporting_ethnicity(x_baby_ethnicity_code))

## Baby birthweight 

pregnancies4 %<>% 
  mutate(x_birthweight = smr02_birthweight)

## Bbay birthweight centile ----------------------------------------------------

### birthweight centile
birthweight_centiles <- read_csv(paste0(folder_documentation, "who_birthweight_centiles_lms.csv")) %>%
  clean_names() %>%
  rename_with(~ paste0("x_baby_birthweight_", .)) %>%
  mutate(x_baby_birthweight_sex = case_when(x_baby_birthweight_sex == 1 ~ "M",
                                            x_baby_birthweight_sex == 2 ~ "F"))

pregnancies4 %<>% left_join(birthweight_centiles, by = c("x_baby_sex" = "x_baby_birthweight_sex", "x_gestation_at_outcome" = "x_baby_birthweight_gestation"))

pregnancies4 %<>% 
  mutate(x_birthweight_z = (((((x_birthweight/1000)/x_baby_birthweight_m)^x_baby_birthweight_l))-1)/(x_baby_birthweight_l*x_baby_birthweight_s) ) %>%
  mutate(x_birthweight_percentile = 100 * pnorm(x_birthweight_z, 0, 1))

rm(birthweight_centiles)


### OFC centile 
ofc_lms <- read_csv(paste0(folder_documentation, "who_ofc_lms.csv")) %>%
  clean_names() %>%
  rename_with(~ paste0("x_ofc_", .)) %>%
  mutate(x_ofc_sex = case_when(x_ofc_sex == 1 ~ "M",
                               x_ofc_sex == 2 ~ "F"))

pregnancies4 %<>% left_join(ofc_lms, by = c("x_baby_sex" = "x_ofc_sex", "x_gestation_at_outcome" = "x_ofc_estimated_gestation"))

pregnancies4 %<>% 
  mutate(x_ofc_z = (((((smr02_ofc/10)/x_ofc_m)^x_ofc_l))-1)/(x_ofc_l*x_ofc_s) ) %>%
  mutate(x_ofc_percentile = 100 * pnorm(x_ofc_z, 0, 1))


rm(ofc_lms)

## 5 min Apgar -----------------------------------------------------------------

pregnancies4 %<>%
  mutate(x_baby_apgar_5_mins = smr02_apgar_5_minutes) %>%
  mutate(x_baby_apgar_5_mins = stringr::str_pad(x_baby_apgar_5_mins, pad="0", side="left", width=2))
  



## Maternal smoking status at booking ------------------------------------------
#
pregnancies4 %<>% 
  mutate(x_booking_smoking_status = coalesce(anbooking_smoking_status, as.character(smr02_booking_smoking_history))) %>% 
  mutate(x_booking_smoking_status= as.character(x_booking_smoking_status)) %>% 
  mutate(x_booking_smoking_status = case_when(x_booking_smoking_status== "0" ~ "non-smoker", 
                                              x_booking_smoking_status== "1" ~ "smoker", 
                                              x_booking_smoking_status== "2" ~ "ex-smoker")) 

log_print("Adding definitive values : geographic values")
## Maternal healthboard of residence booking ------------------
hbs_lookup <- SPD %>% select(pc7, tidyselect::vars_select(names(SPD), 
                                                          starts_with('hb', ignore.case = TRUE)))



pregnancies4$hb2019 <- NULL 

pregnancies4 <- pregnancies4 %>% 
  #select(-hb2019) %>%
  mutate(year_of_booking = year(x_antenatal_booking_date)) %>%
  left_join(hbs_lookup, by=c("x_postcode_at_booking" = "pc7"))%>% 
  # keep appropriate SIMD/carstairs for the year of delivery
  mutate(maternal_hb_res_booking = case_when(year_of_booking >= 2019 ~ hb2019,
                                             year_of_booking >= 2018  ~ hb2018,
                                             year_of_booking >= 2014 ~ hb2014,
                                             year_of_booking >= 2006  ~ hb2006,
                                             year_of_booking < 2006  ~ as.character(hb1995),
                                             TRUE ~ NA)) %>%
  #join hb name
  left_join(hb_names, by  = c("maternal_hb_res_booking" = "HealthboardCode")) %>% 
  mutate(NRSHealthBoardAreaName = case_when(is.na(NRSHealthBoardAreaName) & anbooking_anbooking==TRUE ~ "Unknown", 
                                            T ~ NRSHealthBoardAreaName))%>%
  rename( healthboard_res_name_booking = NRSHealthBoardAreaName ) %>%
  select(-c(hb2019, hb2018, hb2014, hb2006, hb1995, hb2019name))

## Maternal healthboard of residence end of pregnancy ------------------

pregnancies4 <- pregnancies4 %>% 
  left_join(hbs_lookup, by=c("x_postcode_end_pregnancy" = "pc7"))%>% 
  mutate(year_of_booking = year(anbooking_booking_date)) %>%
  mutate(year_of_delivery = year(pregnancy_end_date)) %>%
  # keep appropriate SIMD/carstairs for the year of delivery
  mutate(maternal_hb_res_end = case_when(year_of_delivery >= 2019 ~ hb2019,
                                         year_of_delivery >= 2018  ~ hb2018,
                                         year_of_delivery >= 2014 ~ hb2014,
                                         year_of_delivery >= 2006  ~ hb2006,
                                         year_of_delivery < 2006  ~ as.character(hb1995),
                                         TRUE ~ NA)) 
  #join hb name
pregnancies4 <- pregnancies4 %>% 
  left_join(hb_names, by  = c("maternal_hb_res_end" = "HealthboardCode")) %>% 
  rename( healthboard_res_name_end = NRSHealthBoardAreaName ) %>%
  mutate(healthboard_res_name_end = case_when(is.na(healthboard_res_name_end) ~"Unknown",
                                              T~healthboard_res_name_end )) %>%
  select(-c(hb2019, hb2018, hb2014, hb2006, hb1995, hb2019name))


##Maternal SIMD at booking ---------------------

#SIMD is slightly complicated as have to select the most relevant year
#a consideration is to just extract all the simd_xxxx_sc_quintile variables and paste ALL on - this is then robust to updates to SIMD
#it would then be up to the database suer to select the correct one

simd_lookup <-SIMD  %>% 
  select(pc7, tidyselect::vars_select(names(SIMD),ends_with("sc_quintile") & 
                                        starts_with('simd', ignore.case = TRUE)))
# add SIMD  information
pregnancies4 <- pregnancies4 %>% 
  left_join(simd_lookup, by=c("x_postcode_at_booking" = "pc7"))%>% 
  
  # keep appropriate SIMD/carstairs for the year of delivery
  mutate(simd_sc_quintile_booking = case_when(year_of_booking >= 2017 ~ simd2020v2_sc_quintile,
                                              year_of_booking >= 2014 & year_of_booking < 2017 ~ simd2016_sc_quintile,
                                              year_of_booking >= 2010 & year_of_booking < 2014 ~ simd2012_sc_quintile,
                                              year_of_booking >= 2007 & year_of_booking < 2010 ~ simd2009v2_sc_quintile,
                                              year_of_booking >= 2004 & year_of_booking < 2007 ~ simd2006_sc_quintile,
                                              year_of_booking >= 1999 & year_of_booking < 2004 ~ simd2004_sc_quintile,
                                              TRUE ~ NA_real_))  %>%
  # SIMD 2004, 2006 use 5 for MOST deprived and 1 for LEAST deprived so we need to reverse this
  mutate(simd_sc_quintile_booking  = case_when(year_of_booking >= 1999 & year_of_booking < 2007 &
                                                 simd_sc_quintile_booking == 5 ~ 1,
                                               year_of_booking >= 1999 & year_of_booking < 2007 &
                                                 simd_sc_quintile_booking == 4 ~ 2,
                                               year_of_booking >= 1999 & year_of_booking < 2007 &
                                                 simd_sc_quintile_booking == 3 ~ 3,
                                               year_of_booking >= 1999 & year_of_booking < 2007 &
                                                 simd_sc_quintile_booking == 2 ~ 4,
                                               year_of_booking >= 1999 & year_of_booking < 2007 &
                                                 simd_sc_quintile_booking == 1 ~ 5,
                                               TRUE ~ simd_sc_quintile_booking )) %>% 
  mutate(simd_sc_quintile_booking  = case_match(simd_sc_quintile_booking ,
                                                1 ~ "1 (most deprived)",
                                                5 ~ "5 (least deprived)",
                                                .default = as.character(simd_sc_quintile_booking ))) %>%
  select(-c(simd2020v2_sc_quintile, simd2016_sc_quintile, simd2012_sc_quintile, 
            simd2009v2_sc_quintile, simd2006_sc_quintile, simd2004_sc_quintile))

pregnancies4 <- pregnancies4 %>% 
  mutate(simd_sc_quintile_booking = case_when(is.na(simd_sc_quintile_booking) & anbooking_anbooking==TRUE ~ "Unknown", 
                                          T ~simd_sc_quintile_booking))
##Maternal SIMD at end of pregnancy--------------------
# add SIMD information
pregnancies4 <- pregnancies4 %>% 
  left_join(simd_lookup, by=c("x_postcode_end_pregnancy" = "pc7"))%>% 
  # keep appropriate SIMD/carstairs for the year of delivery
  mutate(simd_sc_quintile_end = case_when(year_of_delivery >= 2017 ~ simd2020v2_sc_quintile,
                                          year_of_delivery >= 2014 & year_of_delivery < 2017 ~ simd2016_sc_quintile,
                                          year_of_delivery >= 2010 & year_of_delivery < 2014 ~ simd2012_sc_quintile,
                                          year_of_delivery >= 2007 & year_of_delivery < 2010 ~ simd2009v2_sc_quintile,
                                          year_of_delivery >= 2004 & year_of_delivery < 2007 ~ simd2006_sc_quintile,
                                          year_of_delivery >= 1999 & year_of_delivery < 2004 ~ simd2004_sc_quintile,
                                          TRUE ~ NA_real_))  %>%
  # SIMD 2004, 2006 use 5 for MOST deprived and 1 for LEAST deprived so we need to reverse this
  mutate(simd_sc_quintile_end = case_when(year_of_delivery >= 1999 & year_of_delivery < 2007 &
                                            simd_sc_quintile_end == 5 ~ 1,
                                          year_of_delivery >= 1999 & year_of_delivery < 2007 &
                                            simd_sc_quintile_end == 4 ~ 2,
                                          year_of_delivery >= 1999 & year_of_delivery < 2007 &
                                            simd_sc_quintile_end == 3 ~ 3,
                                          year_of_delivery >= 1999 & year_of_delivery < 2007 &
                                            simd_sc_quintile_end == 2 ~ 4,
                                          year_of_delivery >= 1999 & year_of_delivery < 2007 &
                                            simd_sc_quintile_end == 1 ~ 5,
                                          TRUE ~ simd_sc_quintile_end)) %>% 
  mutate(simd_sc_quintile_end = case_match(simd_sc_quintile_end,
                                           1 ~ "1 (most deprived)",
                                           5 ~ "5 (least deprived)",
                                           .default = as.character(simd_sc_quintile_end))) %>%
  select(-c(simd2020v2_sc_quintile, simd2016_sc_quintile, simd2012_sc_quintile, 
            simd2009v2_sc_quintile, simd2006_sc_quintile, simd2004_sc_quintile, year_of_delivery, year_of_booking))


pregnancies4 <- pregnancies4 %>% 
  mutate(simd_sc_quintile_end = case_when(is.na(simd_sc_quintile_end)~ "Unknown", 
                                              T ~simd_sc_quintile_end))
##Maternal local authority area at booking--------------------

la_lookup <- SPD %>% select(pc7, tidyselect::vars_select(names(SPD),starts_with('ca', ignore.case = TRUE)))

pregnancies4 <- pregnancies4 %>% 
  mutate(year_of_booking = year(anbooking_booking_date)) %>%
  mutate(year_of_delivery = year(pregnancy_end_date)) %>%
  left_join(la_lookup , by=c("x_postcode_at_booking" = "pc7")) %>%
  mutate(maternal_la_at_booking = case_when(year_of_booking >= 2019 ~ca2019,
                                            year_of_booking >= 2018 ~ca2018,
                                            year_of_booking < 2018 ~ca2011,
                                            T~NA)) %>% 
  select(-c(ca2019,ca2018,ca2011, ca2019name)) %>%
  mutate(maternal_la_at_booking =
           case_when(is.na(maternal_la_at_booking) & anbooking_anbooking==TRUE ~"Unknown",
                     T~ maternal_la_at_booking))

##Maternal local authority area at end of pregnancy --------------------
pregnancies4 <- pregnancies4 %>% 
  left_join(la_lookup , by=c("x_postcode_end_pregnancy" = "pc7")) %>%
  mutate(maternal_la_at_end = case_when(year_of_delivery >= 2019 ~ ca2019,
                                        year_of_delivery >= 2018 ~ ca2018,
                                        year_of_delivery < 2018 ~ ca2011,
                                        T~NA)) %>%
  select(-c(ca2019,ca2018,ca2011,  ca2019name))%>%
  mutate(maternal_la_at_end =
           case_when(is.na(maternal_la_at_end) ~"Unknown",
                     T~ maternal_la_at_end))

##add council area names.
pregnancies4 <- pregnancies4 %>% 
  left_join(la_names , by=c("maternal_la_at_booking" = "CouncilAreaCode")) %>%
  rename(CouncilAreaName_booking = CouncilAreaName) %>%
  mutate(CouncilAreaName_booking =
           case_when(is.na(CouncilAreaName_booking) & anbooking_anbooking==TRUE ~"Unknown",
                     T~ CouncilAreaName_booking))%>%
  left_join(la_names , by=c("maternal_la_at_end" = "CouncilAreaCode")) %>%
  rename(CouncilAreaName_end_preg = CouncilAreaName) %>%
  mutate(CouncilAreaName_end_preg =
           case_when(is.na(CouncilAreaName_end_preg) ~"Unknown",
                     T~ CouncilAreaName_end_preg))


##attach names to end of pregnancy location
pregnancies4 <- pregnancies4 %>% 
  left_join(hb_names, by = c("x_nhs_board_at_end_of_pregnancy"= "HealthboardCode")) %>%
 rename("x_nhs_board_treatment_name "  = "NRSHealthBoardAreaName") 
rm(la_names, hb_names, hbs_lookup )


log_print("Add infant deaths")
## Add on infant deaths ---------------------------------------------------------
data_infant_deaths <- readRDS(paste0(folder_temp_data, "infant_deaths.rds"))

##copying in from cops - check this works for our data
data_infant_deaths_triplicate <- data_infant_deaths %>% 
  filter(nrs_triplicate_id != "BNANANA") %>%
  filter(nrs_triplicate_id != "B-000-") %>%
  filter(nrs_triplicate_id != "B00000") %>%
  select(-chi) %>%
  rename(triplicate_date_of_baby_death = date_of_baby_death, 
         triplicate_deaths_triplicate_id=  deaths_triplicate_id) %>%
  rename(triplicate_underlying_cause_of_baby_death = underlying_cause_of_baby_death) %>%
  rename(triplicate_cause_of_baby_death_0 = cause_of_baby_death_0) %>%
  rename(triplicate_cause_of_baby_death_1 = cause_of_baby_death_1) %>%
  rename(triplicate_cause_of_baby_death_2 = cause_of_baby_death_2) %>%
  rename(triplicate_cause_of_baby_death_3 = cause_of_baby_death_3) %>%
  rename(triplicate_cause_of_baby_death_4 = cause_of_baby_death_4) %>%
  rename(triplicate_cause_of_baby_death_5 = cause_of_baby_death_5) %>%
  rename(triplicate_cause_of_baby_death_6 = cause_of_baby_death_6) %>%
  rename(triplicate_cause_of_baby_death_7 = cause_of_baby_death_7) %>%
  rename(triplicate_cause_of_baby_death_8 = cause_of_baby_death_8) %>%
  rename(triplicate_cause_of_baby_death_9 = cause_of_baby_death_9)

data_infant_deaths_chi <-data_infant_deaths %>% 
  filter(!is.na(chi)) %>%
  select(-nrs_triplicate_id) %>%
  rename(chi_date_of_baby_death = date_of_baby_death) %>%
  rename(chi_underlying_cause_of_baby_death = underlying_cause_of_baby_death, chi_deaths_triplicate_id  = deaths_triplicate_id) %>%
  rename(chi_cause_of_baby_death_0 = cause_of_baby_death_0) %>%
  rename(chi_cause_of_baby_death_1 = cause_of_baby_death_1) %>%
  rename(chi_cause_of_baby_death_2 = cause_of_baby_death_2) %>%
  rename(chi_cause_of_baby_death_3 = cause_of_baby_death_3) %>%
  rename(chi_cause_of_baby_death_4 = cause_of_baby_death_4) %>%
  rename(chi_cause_of_baby_death_5 = cause_of_baby_death_5) %>%
  rename(chi_cause_of_baby_death_6 = cause_of_baby_death_6) %>%
  rename(chi_cause_of_baby_death_7 = cause_of_baby_death_7) %>%
  rename(chi_cause_of_baby_death_8 = cause_of_baby_death_8) %>%
  rename(chi_cause_of_baby_death_9 = cause_of_baby_death_9)

# join by triplicate id first 
pregnancies4 <- pregnancies4 %>% 
  left_join(data_infant_deaths_triplicate, by = c("nrs_triplicate_id" = "nrs_triplicate_id")) 

# remove joined death triplicate ids that have successfully joined to pregnancies file from the chi infant deaths file (so we don't have any duplicate deaths)
data_infant_deaths_chi <- data_infant_deaths_chi %>%
  filter(!(chi_deaths_triplicate_id %in% pregnancies4$triplicate_deaths_triplicate_id))

pregnancies4 <- pregnancies4 %>%
  left_join(data_infant_deaths_chi, by = c("births_baby_upi" = "chi")) %>%
  mutate(deaths_triplicate_id=  coalesce(triplicate_deaths_triplicate_id, chi_deaths_triplicate_id)) %>%
  mutate(x_date_of_baby_death = coalesce(triplicate_date_of_baby_death, chi_date_of_baby_death)) %>%
  mutate(x_underlying_cause_of_baby_death = coalesce(triplicate_underlying_cause_of_baby_death, chi_underlying_cause_of_baby_death)) %>%
  mutate(x_cause_of_baby_death_0 = coalesce(triplicate_cause_of_baby_death_0, chi_cause_of_baby_death_0)) %>%
  mutate(x_cause_of_baby_death_1 = coalesce(triplicate_cause_of_baby_death_1, chi_cause_of_baby_death_1)) %>%
  mutate(x_cause_of_baby_death_2 = coalesce(triplicate_cause_of_baby_death_2, chi_cause_of_baby_death_2)) %>%
  mutate(x_cause_of_baby_death_3 = coalesce(triplicate_cause_of_baby_death_3, chi_cause_of_baby_death_3)) %>%
  mutate(x_cause_of_baby_death_4 = coalesce(triplicate_cause_of_baby_death_4, chi_cause_of_baby_death_4)) %>%
  mutate(x_cause_of_baby_death_5 = coalesce(triplicate_cause_of_baby_death_5, chi_cause_of_baby_death_5)) %>%
  mutate(x_cause_of_baby_death_6 = coalesce(triplicate_cause_of_baby_death_6, chi_cause_of_baby_death_6)) %>%
  mutate(x_cause_of_baby_death_7 = coalesce(triplicate_cause_of_baby_death_7, chi_cause_of_baby_death_7)) %>%
  mutate(x_cause_of_baby_death_8 = coalesce(triplicate_cause_of_baby_death_8, chi_cause_of_baby_death_8)) %>%
  mutate(x_cause_of_baby_death_9 = coalesce(triplicate_cause_of_baby_death_9, chi_cause_of_baby_death_9)) %>%
  ungroup() %>%
  mutate(infant_death = case_when(!is.na(x_date_of_baby_death ) ~1,T~0)) %>% 
select(
    -c(
      triplicate_date_of_baby_death,
      triplicate_underlying_cause_of_baby_death,
      triplicate_cause_of_baby_death_0,
      triplicate_cause_of_baby_death_1,
      triplicate_cause_of_baby_death_2,
      triplicate_cause_of_baby_death_3,
      triplicate_cause_of_baby_death_4,
      triplicate_cause_of_baby_death_5,
      triplicate_cause_of_baby_death_6,
      triplicate_cause_of_baby_death_7,
      triplicate_cause_of_baby_death_8,
      triplicate_cause_of_baby_death_9,
      chi_date_of_baby_death,
      chi_underlying_cause_of_baby_death,
      chi_cause_of_baby_death_0,
      chi_cause_of_baby_death_1,
      chi_cause_of_baby_death_2,
      chi_cause_of_baby_death_3,
      chi_cause_of_baby_death_4,
      chi_cause_of_baby_death_5,
      chi_cause_of_baby_death_6,
      chi_cause_of_baby_death_7,
      chi_cause_of_baby_death_8,
      chi_cause_of_baby_death_9
    )
  )

rm(data_infant_deaths_chi, data_infant_deaths_triplicate)

#create variable for category of infant death (early /late neonatal and later infant death)
pregnancies4 <- pregnancies4 %>%
  mutate(type_infant_death = case_when(difftime(x_date_of_baby_death, x_pregnancy_end_date, units="days") %in% 0:6 ~ "Early neonatal death", 
                                       difftime(x_date_of_baby_death, x_pregnancy_end_date, units="days") %in% 7:28 ~ "Late neonatal death",
                                       difftime(x_date_of_baby_death, x_pregnancy_end_date, units="days")>28 ~"Post-neonatal death"))


# Create selective reduction flag ----------------------------------------------
pregnancies4 <- pregnancies4 %>%
  mutate(x_selective_reduction = case_when(aas_selective_reduction == 1 ~ TRUE, T~FALSE))

# Create code 8 flag for pregnancies that have a code 8 flag associated with them ---------------------------------- 

pregnancies4 <- pregnancies4 %>% 
  mutate(x_abortion_of_fetus_died_in_utero = 
           case_when(smr02_abortion_of_fetus_died_in_utero_flag == 1 ~ TRUE, T~FALSE)) %>%
  group_by(pregnancy_id) %>%
  mutate(x_abortion_of_fetus_died_in_utero = max_(x_abortion_of_fetus_died_in_utero ), 
         x_abortion_of_fetus_died_in_utero = case_when(is.na(x_abortion_of_fetus_died_in_utero) ~ 0, 
                                                       T ~ x_abortion_of_fetus_died_in_utero)) %>%
  ungroup() 

log_print("rename and select final variables for dataset")
# Select final variables for the database ---------------

pregnancies4 <- pregnancies4 %>% rename("baby_chi" = "births_baby_upi", 
                                        "est_date_conception" = "x_est_conception_date", 
                                        "date_end_pregnancy" = "x_pregnancy_end_date" ,
                                        "gest_end_pregnancy" = "x_gestation_at_outcome",
                                        "infant_deaths_triplicate_id" = "deaths_triplicate_id",
                                        "gestation_ascertainment" =  "x_gestation_ascertainment" ,
                                        "number_of_babies" = "births_db_births_number_of_births",
                                        "fetus_outcome1" =  "x_fetus_outcome_1",
                                        "fetus_outcome2" =  "x_fetus_outcome_2",
                                        "maternal_death_in_preg" = "x_maternal_death_in_pregnancy", "date_maternal_death" = "x_maternal_date_of_death", 
                                        "maternal_emigration_in_preg" = "x_maternal_emigration_during_pregnancy", "date_maternal_emigration" = "x_date_of_maternal_emigration",
                                        "mother_dob" = "x_mother_dob", "antenatal_booking_date" = "x_antenatal_booking_date",
                                        "maternal_age_conception" = "x_mother_age_at_conception","maternal_age_end_preg" = "x_mother_age_at_outcome",
                                        "maternal_postcode_booking" = "x_postcode_at_booking","maternal_postcode_end_preg" = "x_postcode_end_pregnancy",
                                        "maternal_nhs_board_res_booking" = "maternal_hb_res_booking" ,"maternal_nhs_board_res_name_at_booking" = "healthboard_res_name_booking"  ,
                                        "maternal_nhs_board_res_end_preg" = "maternal_hb_res_end", "maternal_nhs_board_res_name_end_preg" =  "healthboard_res_name_end" ,
                                        "maternal_CouncilArea_booking" = "maternal_la_at_booking" , "maternal_CouncilAreaName_booking" = "CouncilAreaName_booking" , 
                                        "maternal_CouncilArea_end_preg" = "maternal_la_at_end"  ,"maternal_CouncilAreaName_end" = "CouncilAreaName_end_preg",  
                                        "maternal_SIMD_booking" = "simd_sc_quintile_booking" ,"maternal_SIMD_end_preg" =  "simd_sc_quintile_end"  ,
                                        "maternal_ethnicity" = "mother_ethnicity",
                                        "nhs_board_treatment_end_name" = "x_nhs_board_treatment_name ",
                                        "n_prev_pregnancies" = "x_previous_pregnancies","n_prev_deliveries" = "x_previous_deliveries",
                                        "anbooking_record" = "x_record_of_ab" , 
                                        "anbooking_date" ="anbooking_booking_date" ,
                                        "gestation_at_booking" = "x_gestation_at_anbooking",
                                        "nhs_board_treatment_booking" = "x_nhs_board_at_booking", 
                                        "nhs_board_treatment_delivery"="x_nhs_board_at_end_of_pregnancy" , "location_care_end_pregnancy" = "x_location_at_end_of_pregnancy"  ,
                                        "maternal_height" = "x_maternal_height", "maternal_weight" = "x_maternal_weight", "maternal_bmi" = "x_bmi",
                                       "maternal_smoking" = "x_booking_smoking_status",
                                        "induction_of_labour" = "x_induction_of_labour", "presentation_at_delivery" ="x_presentation_at_delivery",
                                        "onset_delivery"= "x_onset_of_delivery", "mode_delivery" = "x_mode_of_delivery", "perineal_tear" = "x_perineal_tear",
                                        "fetus_marker" = "x_fetus_id"  ,
                                        "baby_sex" = "x_baby_sex",
                                        "baby_ethnicity_code" = "x_baby_ethnicity_code", 
                                        "birthweight" = "x_birthweight", "birthweight_centile"= "x_birthweight_percentile", "apgar" = "x_baby_apgar_5_mins",
                                        "selective_reduction_flag" = "x_selective_reduction", 
                                        "date_infant_death" = "x_date_of_baby_death",
                                        "underlying_cause_infant_death" = "x_underlying_cause_of_baby_death" ,
                                        "other_cause_infant_death_0" = "x_cause_of_baby_death_0" ,
                                        "other_cause_infant_death_1" = "x_cause_of_baby_death_1" ,
                                        "other_cause_infant_death_2" = "x_cause_of_baby_death_2"  ,
                                        "other_cause_infant_death_3" = "x_cause_of_baby_death_3" , 
                                        "other_cause_infant_death_4"= "x_cause_of_baby_death_4" , 
                                        "other_cause_infant_death_5"= "x_cause_of_baby_death_5" , 
                                        "other_cause_infant_death_6"= "x_cause_of_baby_death_6" , 
                                        "other_cause_infant_death_7"= "x_cause_of_baby_death_7" , 
                                        "other_cause_infant_death_8"= "x_cause_of_baby_death_8" , 
                                        "other_cause_infant_death_9"= "x_cause_of_baby_death_9" ,
                                       "abortion_of_fetus_died_in_utero" = "x_abortion_of_fetus_died_in_utero", 
                                       "maternal_death_postpartum" = "x_maternal_death_postpartum"
)
##Link to births file to get the nrs and smr02 flags linked on.

births_ids <- readRDS(paste0(folder_temp_data,"slipbd_births_updates.rds" )) %>%
  select(baby_id, nrs, smr02_live_births) %>%
  rename(NRS_id = nrs ,  SMR02_lb_id = smr02_live_births)

pregnancies4 <- pregnancies4 %>% left_join(births_ids, by = c("slipbd_births_file_1" = "baby_id"))

pregnancies4 <- pregnancies4 %>% 
  mutate(date_end_pregnancy = case_when(fetus_outcome1=="Ongoing"~NA, T~date_end_pregnancy))


saveRDS(pregnancies4, paste0(folder_temp_data, "final_dataset_all_variables.rds"))
pregnancies4 <- readRDS(paste0(folder_temp_data, "final_dataset_all_variables.rds"))
#load in previous all variables file####

#Add flags for data sources#### 

pregnancies4 <- pregnancies4 %>% 
  mutate(sourceABC = case_when(!is.na(anbooking_1)~1, T~0), 
         sourceSMR01 = case_when(!is.na(SMR01_1)~1, T~0), 
         sourceSMR02= case_when(!is.na(SMR02_1) ~1,
                                !is.na(SMR02_lb_id) ~1, T~0),
         sourceNRS= case_when(!is.na(NRS_id)~1, T~0), 
         sourceToPs= case_when(!is.na(ToPs_1)~1, T~0))
  
  
  
pregnancies_update <- pregnancies4  %>% 
  select(c(pregnancy_id, mother_upi, baby_chi, nrs_triplicate_id, infant_deaths_triplicate_id, 
           est_date_conception, date_end_pregnancy, gest_end_pregnancy,gestation_ascertainment,
           number_of_babies, fetus_outcome1, fetus_outcome2,
           additional_fetus_outcome1 ,additional_fetus_outcome2, 
           maternal_death_in_preg, maternal_death_postpartum, any_maternal_death,
           date_maternal_death, maternal_emigration_in_preg, date_maternal_emigration, mother_dob,
           maternal_age_conception, maternal_age_end_preg, maternal_postcode_booking, maternal_postcode_end_preg,
           maternal_nhs_board_res_end_preg, maternal_nhs_board_res_name_end_preg,
           maternal_nhs_board_res_booking, maternal_nhs_board_res_name_at_booking,
           maternal_SIMD_booking, maternal_SIMD_end_preg ,    
           year_of_booking,   year_of_delivery,     
           maternal_CouncilArea_booking,  maternal_CouncilArea_end_preg,      
           maternal_CouncilAreaName_booking ,  maternal_CouncilAreaName_end,
           maternal_ethnicity, n_prev_pregnancies, n_prev_deliveries,
           anbooking_record, antenatal_booking_date, gestation_at_booking,
           nhs_board_treatment_booking, nhs_board_treatment_end_name, location_care_end_pregnancy,maternal_height,
           maternal_weight, maternal_bmi,
            maternal_smoking,
           induction_of_labour, presentation_at_delivery, onset_delivery, mode_delivery, perineal_tear,
           fetus_marker, baby_sex, baby_ethnicity_code, baby_ethnicity, 
           birthweight, birthweight_centile, apgar, selective_reduction_flag, abortion_of_fetus_died_in_utero, 
           infant_death, date_infant_death, type_infant_death,
           underlying_cause_infant_death,other_cause_infant_death_0, other_cause_infant_death_1, other_cause_infant_death_2 , other_cause_infant_death_3, 
           other_cause_infant_death_4, other_cause_infant_death_5, other_cause_infant_death_6, other_cause_infant_death_7, 
           other_cause_infant_death_8, other_cause_infant_death_9, 
           sourceABC, sourceNRS, sourceSMR01, sourceSMR02, sourceToPs)) 
#historic update only - save straight to main file.
saveRDS(pregnancies_update, paste0(folder_database_path, "slipbd_database.rds"))
# backup file
saveRDS(pregnancies_update, paste0(folder_backup_path, "slipbd_database.rds"))


# Bring in the old dataset and join to the new -------------------------------
log_print("Joining on to old dataset")

old_dataset <- readRDS( paste0(folder_backup_path, "slipbd_database_old_",yr_last, "_", month_last, ".rds"))

#max end date - also filter out ongoing pregnancies
old_dataset <- old_dataset %>% 
  filter(fetus_outcome1 != "Ongoing") %>% 
  mutate(date_maternal_death=as.Date(date_maternal_death),
         date_infant_death = as.Date(date_infant_death)) %>%
  mutate(max_date = case_when(!is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death >= date_maternal_death) ~ date_infant_death,
                              !is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death < date_maternal_death) ~  date_maternal_death,
                              !is.na(date_infant_death) ~ date_infant_death, 
                              !is.na(date_maternal_death) ~ date_maternal_death,
                             is.na(date_end_pregnancy) ~ est_date_conception +(38*7),#for temporary use, doesnt matter if its in the future.
                              T~ date_end_pregnancy)) %>% 
  group_by(pregnancy_id) %>% mutate(max_date = max_(max_date)) %>%
  ungroup()%>%
  filter(max_date <  cutoff_date)

pregnancies_update  <- pregnancies_update %>%  mutate(date_maternal_death=as.Date(date_maternal_death),
                               date_infant_death = as.Date(date_infant_death)) %>%
  mutate(max_date = case_when(!is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death >= date_maternal_death) ~ date_infant_death,
                              !is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death < date_maternal_death) ~  date_maternal_death,
                              !is.na(date_infant_death) ~ date_infant_death, 
                              !is.na(date_maternal_death) ~ date_maternal_death,
                              is.na(date_end_pregnancy) ~ est_date_conception +(38*7),#for temporary use for unknowns, doesnt matter if its in the future.
                              T~ date_end_pregnancy)) %>% 
  group_by(pregnancy_id) %>% mutate(max_date = max_(max_date)) %>%
  ungroup()%>%
  filter(fetus_outcome1=="Ongoing" | max_date >=  cutoff_date) 

pregnancies_final <- rbind(pregnancies_update  , old_dataset) %>% select(-max_date)


# Write rds file ---------------------------------------------------------------

saveRDS(pregnancies_final, paste0(folder_database_path, "slipbd_database.rds"))

# backup file

saveRDS(pregnancies_final, paste0(folder_backup_path, "slipbd_database.rds"))

#identifiers file----------------------------------------------

identifiers <- pregnancies4 %>% select(pregnancy_id, mother_upi, baby_chi, fetus_marker,smr02_baby,  est_date_conception,
                                   date_end_pregnancy, fetus_outcome1,
                                   nrs_mother_upi_number, nrs_baby_upi_number, nrs_triplicate_id, 
                                   smr02_upi_number, smr02_baby_upi_number, smr02_date_of_delivery, 
                                   smr02_admission_date, smr01_mother_upi_number, smr01_cis_admission_date,
                                   smr01_cis_discharge_date, aas_mother_upi_number, aas_date_of_termination, 
                                   anbooking_mother_upi, anbooking_date, infant_deaths_triplicate_id, 
                                   date_infant_death, date_maternal_death) 
identifiers <- identifiers %>% rename(smr02_baby_order =  smr02_baby )
#historic refresh only
#saveRDS(identifiers , paste0(folder_database_path, "dataset_identifiers.rds"))
#saveRDS(identifiers , paste0(folder_backup_path, "dataset_identifiers.rds"))

####

old_identifiers <- 
  readRDS(paste0(folder_database_path, "dataset_identifiers",yr_last, "_", month_last, ".rds"))
#max end date - also filter out ongoing pregnancies
old_identifiers <- old_identifiers %>% 
  filter(fetus_outcome1 != "Ongoing") %>% 
  mutate(date_maternal_death=as.Date(date_maternal_death),
         date_infant_death = as.Date(date_infant_death)) %>%
  mutate(max_date = case_when(!is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death >= date_maternal_death) ~ date_infant_death,
                              !is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death < date_maternal_death) ~  date_maternal_death,
                              !is.na(date_infant_death) ~ date_infant_death, 
                              !is.na(date_maternal_death) ~ date_maternal_death,
                              is.na(date_end_pregnancy) ~ est_date_conception +(38*7),
                              T~ date_end_pregnancy)) %>% 
  group_by(pregnancy_id) %>% mutate(max_date = max_(max_date)) %>%
  ungroup()%>%
  filter(max_date <  cutoff_date)

identifiers_update  <-identifiers %>% 
  mutate(date_maternal_death=as.Date(date_maternal_death),
         date_infant_death = as.Date(date_infant_death)) %>%
  mutate(max_date = case_when(!is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death >= date_maternal_death) ~ date_infant_death,
                              !is.na(date_infant_death) & !is.na(date_maternal_death) & (date_infant_death < date_maternal_death) ~  date_maternal_death,
                              !is.na(date_infant_death) ~ date_infant_death, 
                              !is.na(date_maternal_death) ~ date_maternal_death,
                              is.na(date_end_pregnancy) ~ est_date_conception +(38*7),
                              T~ date_end_pregnancy)) %>% 
  group_by(pregnancy_id) %>% mutate(max_date = max_(max_date)) %>%
  ungroup()%>%
  filter(fetus_outcome1=="Ongoing" | max_date >=  cutoff_date) 

identifers_final <- rbind(identifiers_update, old_identifiers) %>% select(-max_date)

saveRDS(identifers_final , paste0(folder_database_path, "dataset_identifiers.rds"))
saveRDS(identifers_final , paste0(folder_backup_path, "dataset_identifiers.rds"))

