
# CREATE FILE for all pregnancies -------------------------------------------------------
#source("SLiPBD process/00-setup.R")
log_print("start fetus level file - load all pregnancy files")
## Import data -------------------------------------------------------

# read in data
births <- readRDS(paste0(folder_temp_data, "slipbd_births_updates.rds")) %>% ungroup()
tops <- readRDS(paste0(folder_temp_data, "tops_aas.rds"))
smr02 <- readRDS( paste0(folder_temp_data, "smr02_nonlive.rds"))
smr01 <- readRDS(paste0(folder_temp_data, "smr01.rds"))
an_booking <- readRDS( paste0(folder_temp_data, "antenatal_booking.rds"))

##load NRS and smr live to get event type into births file
log_print("load nrs and smr02 births files")
##paste on pregnancyID so that multiples get correctly grouped
NRS_all_births <- readRDS(paste0(folder_temp_data, "nrs_all_births.rds")) %>%
  select(event_id, termination, stillbirth, livebirth, triplicate_id,nrs_preg_id, duration_of_pregnancy, assumed_gestation )

NRS_all_births <- NRS_all_births %>%
  mutate(stillbirth = case_when(substr(triplicate_id,1,1)=="S" ~T, T~stillbirth )) 
# correction to allow dual termination/ stillbirth outcomes to be flagged

smr02_births <- read_rds(paste0(folder_temp_data, "smr02_data.rds")) %>%
  filter(smr02_outcome_type == "Live birth" |smr02_outcome_type == "Stillbirth" ) %>% 
  select(event_id, smr02_outcome_type, smr02_preg_id, smr02_assumed_gestation, smr02_estimated_gestation )

log_print("join details to births files")
births <- births %>%  left_join(smr02_births, by = c("smr02_live_births" = "event_id"))        
births <- births %>%  left_join(NRS_all_births , by = c("nrs" = "event_id")) 

#prioritise the nrs outcome
#where termination and stillbirth are both recorded in NRS, use both.
births <- births %>% mutate(nrs_outcome_type = case_when(stillbirth==T & termination==T ~ "Stillbirth/Termination",
                                                         stillbirth==T ~ "Stillbirth",                                                         livebirth==1 ~"Live birth", 
                                                         termination==T ~ "Termination",
                                                         T ~ NA_character_)) %>%
  ##keep primary and secondary outcomes where there is a conflict (3 cases where labelled as livebirth in one dataset and stillbirth in another)
  mutate(event_type = case_when(nrs_outcome_type== "Stillbirth/Termination" ~"Termination", 
                                !is.na(nrs_outcome_type) ~ nrs_outcome_type, 
                                is.na(nrs_outcome_type) ~ smr02_outcome_type ),
         event_type2 = case_when(nrs_outcome_type== "Stillbirth/Termination" ~"Stillbirth", 
                                 !is.na(nrs_outcome_type) ~ smr02_outcome_type, 
                                 T~NA_character_)) %>% 
  #correct gestations
  mutate(births_gestation = case_when(smr02_assumed_gestation==1 & assumed_gestation==1  & db_births_number_of_births==1 ~40, 
                                      smr02_assumed_gestation==1 & assumed_gestation==1  & db_births_number_of_births >1 ~37, 
                                      smr02_assumed_gestation==0 ~ smr02_estimated_gestation, 
                                      smr02_assumed_gestation==1 & assumed_gestation==0 ~ duration_of_pregnancy, 
                                      is.na(smr02_assumed_gestation) ~ duration_of_pregnancy)) %>%
  mutate(births_assumed_gest = case_when(smr02_assumed_gestation==1 & assumed_gestation==1~1,
                                         is.na(smr02_assumed_gestation) & assumed_gestation==1 ~1, 
                                         smr02_assumed_gestation==0  ~0, 
                                         assumed_gestation==0 ~0 )) 

#
births <- births %>% filter(!is.na(event_type))
conflicts <- births  %>% filter(event_type=="Live birth" & event_type2=="Stillbirth")


##filter out smr02 only births
births <- births %>% filter(!is.na(nrs))

##Bind together the minimal information:  event dates, mother chi/upi, conception date. event type
## also include event ID and data source to enable linkage back to other information

log_print("bind together identifiers from  all outcomes")
births_events <- births %>% 
  select(mother_upi, event_date, conception_date,event_type , event_type2, baby_id, births_assumed_gest, births_gestation ) %>% 
  mutate(data_source="slipbd_births_file")
tops_events<- tops %>% 
  select(aas_mother_upi_number, aas_date_of_termination, aas_estimated_date_of_conception,aas_outcome_type,  aas_event_id, aas_assumed_gestation, aas_estimated_gestation)%>%
  mutate(data_source="ToPs", event_type2 = NA)
smr02_events<- smr02 %>% 
  select(smr02_upi_number,smr02_admission_date, smr02_estimated_conception_date,smr02_outcome_type,  event_id, smr02_assumed_gestation, smr02_estimated_gestation) %>% 
  mutate(data_source="SMR02", event_type2 = NA)
smr01_events <- smr01 %>% mutate(smr01_assumed_gestation=1) %>%
  select(smr01_mother_upi_number, smr01_cis_admission_date, smr01_estimated_conception_date,smr01_outcome_type, event_id , smr01_assumed_gestation,  smr01_gestation) %>%
  mutate(data_source="SMR01", event_type2 = NA)
anbooking_events <- an_booking %>%
  select(anbooking_mother_upi, anbooking_booking_date, anbooking_estimated_conception_date, anbooking_event_type, anbooking_event_id, anbooking_assumed_gestation, anbooking_gestation_at_booking) %>% 
  mutate(data_source="anbooking", event_type2 = NA)

rm(births, tops, smr02, smr01,an_booking)
###make names the same

colnames(births_events) <-  c("mother_chi",  "event_date", "conception_date", "event_type", "event_type2", "event_id", "assumed_gest", "gest_at_event", "data_source")
colnames(tops_events)   <-  c("mother_chi",  "event_date", "conception_date", "event_type", "event_id","assumed_gest", "gest_at_event", "data_source", "event_type2")
colnames(smr02_events)  <-  c("mother_chi",  "event_date", "conception_date", "event_type", "event_id","assumed_gest", "gest_at_event", "data_source", "event_type2")
colnames(smr01_events)   <-  c("mother_chi",  "event_date", "conception_date", "event_type", "event_id","assumed_gest", "gest_at_event", "data_source", "event_type2")
colnames(anbooking_events) <- c("mother_chi",  "event_date", "conception_date", "event_type", "event_id","assumed_gest", "gest_at_event", "data_source", "event_type2")


#bind rows
#
pregnancies1 <- bind_rows(births_events, 
                          smr02_events, 
                          smr01_events,
                          tops_events,
                          anbooking_events
) %>%
  arrange(mother_chi, conception_date, event_date)


#### Replace mother CHI with UPI ####
log_print("Repalce CHI with definitive UPI")
clear_temp_tables(SMRAConnection)

chi_to_upi <- SMRAConnection %>% tbl(dbplyr::in_schema("UPIP", "L_UPI_DATA")) %>% 
  select(CHI_NUMBER, UPI_NUMBER) %>% 
  inner_join(pregnancies1 %>% select(mother_chi) %>% distinct() %>% rename(CHI_NUMBER = mother_chi), copy = TRUE) %>% 
  distinct() %>% 
  collect() %>% 
  group_by(CHI_NUMBER) %>% 
  slice(1) %>% 
  ungroup() %>% 
  rename(mother_chi = CHI_NUMBER, mother_upi = UPI_NUMBER) 

pregnancies1 %<>%
  left_join(chi_to_upi)

pregnancies1 %>%
  select(mother_chi, mother_upi) %>%
  mutate(chi_matches_upi = mother_chi == mother_upi) %>%
  tabyl(chi_matches_upi)

pregnancies1 %<>%
  mutate(mother_upi = ifelse(is.na(mother_upi), mother_chi, mother_upi)) %>%
  select(-mother_chi)

rm(chi_to_upi)

pregnancies1 %>%
  mutate(validity = chi_check(mother_upi)) %>%
  tabyl(validity)


#### Group by event date ####
log_print("Begin grouping events by date")
##group/deduplicate  by event date (83 day window)####
#separate booking first as we don't expect to group on event date
#and they can end up grouped to wrong pregnancy in the case of 2 consecutive pregnancies shortly following each other
booking <- pregnancies1 %>% filter(event_type=="booking")
pregnancies1 <-  pregnancies1 %>% filter(event_type!="booking")
# Assign each event to a single pregnancy event. 
#This allows us to group events which seem to be related and which probably should belong to the same pregnancy.
#It helps us overcome any issues with innacurate conception dates. 
pregnancies2 <- 
  moving_index_deduplication(pregnancies1, mother_upi, event_date, dedupe_period)

pregnancies2 <- pregnancies2 %>%
  group_by(mother_upi, event) %>%
  mutate(revised_conception_date = min(conception_date)) %>%
  ungroup() 

#### Group by conception date #####
#resolve into nominal  pregnancy groups based on the dedup rule
#add bookings####
pregnancies3 <- bind_rows(pregnancies2, booking) %>% 
  mutate(revised_conception_date = case_when(event_type=="booking" ~ conception_date, 
                                             T~revised_conception_date)) 

pregnancies3 <- 
  moving_index_deduplication(pregnancies3, mother_upi, revised_conception_date, dedupe_period)

pregnancies3 <- pregnancies3 %>% 
  select(-c(revised_conception_date)) %>% # This variable was based only on grouping by event date - pregnancy_start_date takes into account our final pregnancy groupings
  rename(pregnancy = event)


#### Determine the start and end date of each pregnancy and check that bookings are grouped correctly ####
log_print(" Determine the start and end date of each pregnancy")
pregnancies5 <- pregnancies3 %>%
  group_by(mother_upi, pregnancy) %>%
  mutate(pregnancy_start_date = min_(conception_date)) %>%
  mutate(pregnancy_end_date = max_(event_date)) %>%
  ungroup() %>% 
 # mutate(pregnancy_start_date = case_when(event_type=="booking" ~ conception_date, T~pregnancy_start_date), 
 #        pregnancy_end_date = case_when(event_type=="booking" ~ NA, T~pregnancy_end_date)) %>%
  arrange(mother_upi, event_date) %>% 
  mutate(pregnancy = case_when(event_type == "booking" & 
                                 mother_upi == lead(mother_upi) &
                                 event_date >= lead(pregnancy_start_date) & 
                                 event_date <= lead(pregnancy_end_date) ~ lead(pregnancy),
                               event_type == "booking" & 
                                 mother_upi == lag(mother_upi) &
                                 event_date >= lag(pregnancy_start_date) & 
                                 event_date <= lag(pregnancy_end_date) ~ lag(pregnancy),
                                                              T ~ pregnancy)) %>% 
  group_by(mother_upi, pregnancy) %>%
  mutate(pregnancy_id = UUIDgenerate()) %>% 
  ungroup() %>% 
  group_by(pregnancy_id) %>%
  #apply start and end dates, so that the bookings are given correct dates
  mutate(pregnancy_start_date = min_(pregnancy_start_date), 
         pregnancy_end_date = max_(pregnancy_end_date))
  

#sometimes short pregnancies can be grouped with a subsequent pregnancy as a 2nd conception can happen in 83 days
#ungroup any events where event 1 occurs >14 days before the est conception date of event 2
##these are more plausibly separate pregnancies than anything else
#
log_print("Splitting short pregnancies")

pregnancies5a <- pregnancies5 %>% 
  arrange(pregnancy_id, event_date) %>% 
  group_by(pregnancy_id) %>% 
  mutate(event_timediff = difftime(event_date, lag(event_date), units="days")) %>%
  mutate(conception_eventdiff = difftime(conception_date, lag(event_date), units="days")) %>%
  ungroup() %>% 
  mutate(split = ifelse(conception_eventdiff > 14,1,0)) %>%
  group_by(pregnancy_id) %>%
  mutate(split_preg = max_(split)) %>% ungroup() 


# here we are splitting up pregnancies that have been grouped together incorrectly 
# e.g. a live birth quickly followed by a miscarriage/termination (e.g., within 2 weeks of each other)
# it is possible for a woman to ovulate after an end of pregnancy event and get pregnant within the same month 
# note that some women have multiple bookings associated with the data. This is likely to be due to either 
# 1) an early pregnancy loss followed by a new pregnancy 
# 2) poor chi linkage of booking records 
# split preg takes the most recent booking record associated with an end of pregnancy event 
# note that this might mean that a booking record associated with a pregnancy is the wrong one and we have split up a booking record (resulting in an unknown pregnancy)
# this means we will be counting more pregnancies than we actually should have
# fix this in definitive values by grouping pregnancies

split_pregs <- pregnancies5a %>% filter(split_preg==1) %>% 
  rowwise() %>% 
  mutate(pregnancy_id = if_else(split_preg == 1 & is.na(conception_eventdiff), UUIDgenerate(), pregnancy_id)) %>% 
  ungroup()#  %>% # regenerate IDs for the first pregnancy in a group that needs to be split up

# how many pregnancies have a wildly different conception date from their booking record? 
check_conception_dates <- split_pregs %>%
  group_by(pregnancy_id) %>%
  mutate(conception_timediff = difftime(conception_date, lag(conception_date), units="days")) 

nosplit <- pregnancies5a %>% filter(is.na(split_preg) | split_preg==0) 

##the split above will tackle 2 records being boud together where the 2nd should eb separated.
#however with more records e.g. 4 records making 2 pregnancies. It will only split one record off.
#next section deals with cases where there are 2 early records that should be grouped separate from a later record
split_pregs  <- split_pregs  %>%
  mutate(flag_grouping = case_when(mother_upi == lag(mother_upi) & split==0 ~ lag(pregnancy_id),
                                   T~pregnancy_id)) %>%
  mutate(pregnancy_id = case_when(mother_upi == lag(mother_upi) & split==0 ~ lag(pregnancy_id),
                                  T~pregnancy_id)) %>%
  #then recalculate start and end dates
  group_by(pregnancy_id, mother_upi) %>%
  mutate(pregnancy_start_date = min_(conception_date)) %>%
  mutate(pregnancy_end_date = max_(event_date)) %>%
  ungroup()
split_pregs  <- split_pregs  %>% select(-flag_grouping)
table(split_pregs$mother_upi == lag(split_pregs$mother_upi) & split_pregs$split==0)

##combine back into one df
pregnancies5b <- rbind(split_pregs, nosplit)

pregnancies5b <- pregnancies5b %>%
  arrange(mother_upi, event_date) %>% 
    mutate(pregnancy_id = case_when(event_type == "booking" & 
                                   mother_upi == lag(mother_upi) &
                                   event_date >= lag(pregnancy_start_date) & 
                                   event_date <= lag(pregnancy_end_date) ~ lag(pregnancy_id),
                                 event_type == "booking" & 
                                   mother_upi == lead(mother_upi) &
                                   event_date >= lead(pregnancy_start_date) & 
                                   event_date <= lead(pregnancy_end_date) ~ lead(pregnancy_id),
                                 T ~ pregnancy_id)) 
  

##remove variables used to calclate time differences
pregnancies5b <- pregnancies5b %>% select(-c(event_timediff, split_preg, split,conception_eventdiff  ))

#remove any spurious late dates
pregnancies6 <- pregnancies5b %>% filter(event_date <= Sys.Date()) 

##identify any more potential problems at this stage and at least flag them 
discrepant_dates <- pregnancies6 %>% filter(event_type !="booking") %>%
  group_by(pregnancy_id) %>% 
  mutate(outcome_diff = difftime(max_(event_date), min_(event_date), units = "days")) %>% 
  mutate(discrepancy_outcome_date = case_when(difftime(max_(event_date), min_(event_date), units = "days") > 30~1,T~ 0)) %>% 
  ungroup() %>%
  mutate(length_preg = difftime(pregnancy_end_date,pregnancy_start_date, units = "weeks")) #

discrepant_dates <- discrepant_dates %>% mutate(too_long = case_when(length_preg > 42 ~1, T~0))
#table(discrepant_dates$too_long, discrepant_dates$discrepancy_outcome_date)

#relatively small numbers with problems
discrepant <- discrepant_dates %>% filter(too_long==1 | discrepancy_outcome_date==1)
##

## Create flags for pregnancies that have multiple outcomes 
log_print("Flag multiple outcomes for one fetus")
##Check conflicting outcomes and make 
##Amendments for special cases (eg termination following live birth), threatened early losses
##add all flags here.
pregnancies6 <- pregnancies6 %>% 
  # calculate any date of birth for all births and we can then compare the date difference between the event date that isn't a birth associated with that pregnancy
  # event_type for live births will have event_type2 live birth 
  mutate(birth_date = if_else(event_type == "Live birth" | event_type ==  "Stillbirth" | 
                                event_type2=="Stillbirth" , event_date, as.POSIXct("1970-01-01"))) %>% 
  group_by(pregnancy_id) %>% 
  mutate(birth_date = max_(birth_date)) %>% 
  # if there is a birth (live or stillbirth) associated with a pregnancy and another outcome, flag it 
  mutate(termination_loss_birth = if_else(("Live birth" %in% event_type | "Stillbirth" %in% event_type|"Live birth" %in% event_type2 | "Stillbirth" %in% event_type2) & 
                                            ("Termination" %in% event_type | "Molar pregnancy" %in% event_type | "Ectopic pregnancy" %in% event_type | "Miscarriage" %in% event_type), 1, 0)) %>% 
  ungroup() %>% 
  # if there is another outcome associated with a birth, flag if termination/miscarriage/molar/ectopic happens 30 days after a birth 
  mutate(post_birth_termination_loss = if_else(termination_loss_birth == 1 & 
                                                 (event_type == "Termination" | event_type == "Molar pregnancy" | event_type == "Ectopic pregnancy" | event_type == "Miscarriage") 
                                               & difftime(birth_date, event_date, units = "days") < (-30) , 
                                               1, 0)) %>% 
  group_by(pregnancy_id) %>% 
  #pairing of terminations with early losses - flag for further stats, 
  mutate(ectopic_term = ifelse("Termination" %in% event_type &"Ectopic pregnancy" %in% event_type,1,0)) %>%
  mutate(misc_term = ifelse("Termination" %in% event_type &  "Miscarriage" %in% event_type,1,0)) %>%
  mutate(mol_term = ifelse("Termination" %in% event_type &  "Molar pregnancy" %in% event_type,1,0)) %>%
  ungroup() %>% 
  # flag if a termination occurs 30 days (or more) before a birth 
  mutate(missed_term = ifelse(termination_loss_birth == 1 & event_type == "Termination" &
                                difftime(birth_date, event_date, units = "days") > 30, 1, 0 )) %>%
  # flag if a pregnancy is too long 
  mutate(flag_excess_length = 
           if_else(difftime(pregnancy_end_date,pregnancy_start_date, units = "weeks") >42,1,0)) %>%
  ##many of these are combinations of miscarriage or termination & later live birth.
  #The total pregnancy length is too long, however the danger in breaking them into 2
  #is that the early outcomes generally have imputed conception date,
  #so actual date cd be a few weeks later, making it plausible to be an early threatened loss
  rowwise() %>% 
  # create a new pregnancy id if there is a pregnancy loss/termination 30 days after birth 
  mutate(pregnancy_id = if_else(post_birth_termination_loss == 1, UUIDgenerate(), pregnancy_id)) %>% 
  ungroup() %>% 
  group_by(pregnancy_id) %>% 
  # get new pregnancy start and end dates based on new pregnancies 
  mutate(pregnancy_start_date = min_(conception_date)) %>%
  mutate(pregnancy_end_date = max_(event_date)) %>% 
  ungroup() 

###Add N of outcomes #####
log_print("Add n outomes per pregnancy")

# We use this information to split singletons/multiples in the pregnancy file 
births <- readRDS(paste0(folder_temp_data, "slipbd_births_updates.rds")) %>% ungroup()
tops <- readRDS(paste0(folder_temp_data, "tops_aas.rds"))
smr02 <- readRDS( paste0(folder_temp_data, "smr02_nonlive.rds"))

temp_births_num_of_outcomes <- births %>% select(baby_id, db_births_number_of_births) %>%
  rename(number_of_outcomes = db_births_number_of_births) %>% rename(event_id = baby_id)

temp_tops_num_of_outcomes <- tops %>% select(aas_event_id, aas_original_number_of_foetuses) %>% 
  rename(number_of_outcomes = aas_original_number_of_foetuses, event_id = aas_event_id) %>% 
  mutate(number_of_outcomes = case_when(is.na(number_of_outcomes) ~ 1, T ~ number_of_outcomes))

temp_smr02_num_of_outcomes<- smr02 %>% select(event_id, smr02_num_of_outcomes_this_pregnancy) %>%
  rename(number_of_outcomes = smr02_num_of_outcomes_this_pregnancy)

temp_num_of_outcomes <- temp_births_num_of_outcomes %>%
  bind_rows(temp_tops_num_of_outcomes) %>%
  bind_rows(temp_smr02_num_of_outcomes)

rm(temp_births_num_of_outcomes, temp_tops_num_of_outcomes, temp_smr02_num_of_outcomes)

##remove intermediate variables used to identify problems
pregnancies6 <- pregnancies6 %>%
  select(-c(termination_loss_birth, post_birth_termination_loss, ectopic_term, misc_term, mol_term, missed_term ))

## split bookings and end of pregnancy events up 
pregnancies_an_booking <- pregnancies6 %>% # Store booking data separately and match it on later
  filter(event_type == "booking")

#create n of outcomes=1 where is it not giiven/not known
pregnancies7 <- pregnancies6 %>%
  filter(event_type != "booking") %>%
  left_join(temp_num_of_outcomes, by="event_id") %>%
  mutate(number_of_outcomes = case_when(is.na(number_of_outcomes) ~ 1,
                                        T ~ number_of_outcomes)) %>%
  group_by(mother_upi, pregnancy_id) %>% # changed to pregnancy id as pregnancy is not always correct 
  mutate(number_of_outcomes = max(number_of_outcomes)) %>%
  ungroup()


log_print("Splittign multiples and singletons to deal with exceptionals")
##separate singletons, multiples to deal with excess outcomes separately
pregnancies_singleton1 <- pregnancies7 %>% filter(number_of_outcomes == 1)
pregnancies_multiple1  <- pregnancies7 %>% filter(number_of_outcomes >  1)

####### Dealing with exceptional singletons (pregnancies that should only have 1 outcome but have been linked as having 2)
log_print("dealing with exceptional singletons")
##deal with singletons with  >1 outcome record
pregnancies_singleton1 <- pregnancies_singleton1 %>%
  group_by(pregnancy_id) %>%
  mutate(exceptional_singleton = case_when(n() > 1 ~ T,
                                           T ~ F)) %>%
  ungroup()

#add selective reductions as this enables differentiating valid terminations mid pregnancy
#these should all be labelled multiples but add in to check
tops_selective <- tops %>% 
  filter(aas_selective_reduction==1) %>% select(aas_event_id, aas_selective_reduction)

pregnancies_singleton1 <- left_join(pregnancies_singleton1, tops_selective, by = c("event_id"= "aas_event_id") )


pregnancies_singleton_exceptional <- pregnancies_singleton1 %>%
  filter(exceptional_singleton == T) %>%
  arrange(pregnancy_id, event_date) %>% 
  group_by(pregnancy_id) %>%
  mutate(event_timediff = difftime(event_date, lag(event_date), units="days")) %>%
  # hierarchy - termination/livebirth/stillbirth/ectopic/molar/miscarriage
  mutate(outcome1 = case_when("Termination" %in% event_type |"Termination" %in% event_type2 ~ "Termination",
                              "Live birth" %in% event_type|"Live birth" %in% event_type2 ~ "Live birth",
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth",
                              "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              "Molar pregnancy" %in% event_type ~ "Molar pregnancy",
                              "Miscarriage" %in% event_type ~ "Miscarriage")) %>%
  mutate(outcome2 = case_when(outcome1== "Termination" & ("Live birth" %in% event_type |"Live birth" %in% event_type2 )~ "Live birth",
                              outcome1== "Termination" & ("Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 )~ "Stillbirth", 
                              outcome1== "Termination" &  "Ectopic pregnancy" %in% event_type ~  "Ectopic pregnancy",
                              outcome1== "Termination" &  "Molar pregnancy" %in% event_type ~  "Molar pregnancy",
                              outcome1== "Termination" &  "Miscarriage" %in% event_type ~  "Miscarriage"))  %>%
  ungroup() %>%
  #deal with early terminations followed by a birth 
  #(so bad links or terminations that were unsuccessful / in particular at home termination recorded but didnt happen)
  ##pick out invalid early termination records that are succeeded by a birth
  #don't flag selective reductions as this combination is valid. (and should be in the multiples)
  #only need to check event type 1 as will only be in event type 2 if also a stillbirth, therefore not an early termination
  mutate(missed_term = case_when(event_type == "Termination" &  gest_at_event <17  &  
                                   (outcome2 =="Live birth" | outcome2== "Stillbirth") &
                                   is.na(aas_selective_reduction ) ~ 1, T~0)) %>%
  group_by(pregnancy_id) %>%
  mutate(outcome1 = case_when(max_(missed_term)==1 ~ outcome2, T~outcome1),
         outcome2 = case_when(max_(missed_term)==1 ~ NA_character_, T~outcome2)) %>% ungroup()

table(pregnancies_singleton_exceptional$outcome1, pregnancies_singleton_exceptional$outcome2)

#flag where outcome are too far apart.
pregnancies_singleton_exceptional <- pregnancies_singleton_exceptional %>%
  group_by(pregnancy_id) %>%
  mutate(bad_link_term = case_when(outcome1=="Termination" & outcome2=="Ectopic pregnancy" & 
                                     max_(event_timediff >28)~ T, 
                                   outcome1=="Termination" & outcome2=="Molar pregnancy" & 
                                     max_(event_timediff >28)~ T, 
                                   outcome1=="Termination" & outcome2=="Miscarriage" & 
                                     max_(event_timediff >28)~ T)) %>%
  mutate(bad_outcome_dates = ifelse(max_(event_timediff>28),1,0)) %>% ungroup


#remove missed terminations (ie early terminations followed much later by a birth)
pregnancies_singleton_exceptional2<- pregnancies_singleton_exceptional %>% ungroup %>%
  filter(missed_term==0)%>% 
  group_by(pregnancy_id) %>%
  mutate(n_in_group = n()) %>%
  ungroup() 

#pregnancies with only one record after excluding missed terminations are sorted now 
pregnancies_singleton_exceptional_resolved <- pregnancies_singleton_exceptional2 %>% 
  filter(n_in_group==1) %>% select(-c(missed_term, n_in_group, event_timediff, bad_outcome_dates, bad_link_term))

#check dates on the rest
pregnancies_singleton_exceptional2 <- pregnancies_singleton_exceptional2 %>% filter(n_in_group !=1) 

#if the dates are ok, should be resolved as multiple records refer to the same event and the hierarchy 
#takes care of which outcome takes precedence if there is >1 event type
pregnancies_singleton_exceptional_dates_ok <- pregnancies_singleton_exceptional2 %>%
  filter(bad_outcome_dates==0) %>% 
  select(-c(missed_term, n_in_group, event_timediff, bad_outcome_dates, bad_link_term))


##bind resolved results.
pregnancies_singleton_exceptional_resolved<- rbind(pregnancies_singleton_exceptional_resolved, 
                                                   pregnancies_singleton_exceptional_dates_ok) 


pregnancies_singleton_exceptional_baddates <- pregnancies_singleton_exceptional2 %>% filter(bad_outcome_dates==1)


#resolve remaining singletons with >1 outcome >28 days apart.  
#IF bad outcome dates (ie >28 days apart - cannot have a dual outcome)
# instead select the first in the hierarchy (we have already discarded early termination records that look suspect)
#hierarchy for these is LB >SB > termination > ectopic >molar> miscarriage.
pregnancies_singleton_exceptional_baddates <- pregnancies_singleton_exceptional_baddates %>%
  arrange(pregnancy_id, event_date) %>% 
  group_by(pregnancy_id) %>%
  mutate(event_timediff = difftime(event_date, lag(event_date), units="days")) %>%
  mutate(outcome1 = case_when("Live birth" %in% event_type ~ "Live birth",
                              "Stillbirth" %in% event_type ~ "Stillbirth",
                              "Termination" %in% event_type ~ "Termination", #change herarchy as in these cases the live birth must have happened and the dates are too far apart for bboth to be valid outcomes
                              "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              "Molar pregnancy" %in% event_type ~ "Molar pregnancy",
                              "Miscarriage" %in% event_type ~ "Miscarriage")) %>% 
  mutate(outcome2 = NA) %>%
  ungroup() %>%
  mutate(bad_outcome_dates_flag=1)

#drop early pregnancy loss records or terminations where followed by a birth record >28 days 
pregnancies_singleton_exceptional_baddates <- pregnancies_singleton_exceptional_baddates %>%
  # give ectopics and molars a dummy chi as this is more likely to be a chi linkage error (they are impossible to have with a live birth after it)
  mutate(mother_upi = case_when(event_type %in% c("Molar pregnancy", "Ectopic pregnancy") &
                                  outcome1 %in% c("Live birth", "Stillbirth") ~ paste0("61",str_pad(string = row_number(),
                                                                                                    width = 8,
                                                                                                    side = "left",
                                                                                                    pad = "0")), 
                                T ~ mother_upi)) %>%
  mutate(drop = 
           case_when(event_type %in% c("Miscarriage") &
                       outcome1 %in% c("Live birth", "Stillbirth") ~ 1, 
                     event_type =="Termination" & is.na(aas_selective_reduction) &  
                       outcome1 %in% c("Live birth", "Stillbirth") ~1,
                     T~0)) %>% 
  filter(drop==0) 

separated_records <- pregnancies_singleton_exceptional_baddates %>% filter(substr(mother_upi,1,2) =="61") %>%
  rowwise() %>%
  mutate(pregnancy_id = UUIDgenerate()) 
#remove the birth record ID from this record as well as it is not a birth

pregnancies_singleton_exceptional_baddates <- pregnancies_singleton_exceptional_baddates %>% 
  filter(substr(mother_upi,1,2) !="61")  %>%
  rbind(separated_records)%>%
  group_by(pregnancy_id) %>%
  mutate(n_in_group=n())

##separate thsoe that are now resolved
pregnancies_singleton_dates_sorted <-    
  pregnancies_singleton_exceptional_baddates %>% filter(n_in_group==1) %>%
  select(event_date, conception_date, event_type, event_type2, event_id, assumed_gest, 
         gest_at_event, data_source, mother_upi, pregnancy, pregnancy_start_date, pregnancy_end_date,     
         pregnancy_id, birth_date, flag_excess_length, number_of_outcomes, exceptional_singleton, 
         aas_selective_reduction, outcome1, outcome2     )    

pregnancies_singleton_exceptional_resolved <-
  rbind(pregnancies_singleton_exceptional_resolved, pregnancies_singleton_dates_sorted)

###get final outcomes on all singleton pregnancies except those with bad dates first 
pregnancies_singleton_exceptional_resolved <-    pregnancies_singleton_exceptional_resolved %>%
  group_by(pregnancy_id) %>%
  mutate(outcome1 = case_when("Termination" %in% event_type |"Termination" %in% event_type2 ~ "Termination",
                              "Live birth" %in% event_type|"Live birth" %in% event_type2 ~ "Live birth",
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth",
                              "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              "Molar pregnancy" %in% event_type ~ "Molar pregnancy",
                              "Miscarriage" %in% event_type ~ "Miscarriage")) %>%
  mutate(outcome2 = case_when(outcome1== "Termination" & ("Live birth" %in% event_type |"Live birth" %in% event_type2 )~ "Live birth",
                              outcome1== "Termination" & ("Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 )~ "Stillbirth", 
                              outcome1== "Termination" & "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              outcome1== "Termination" &  "Molar pregnancy" %in% event_type ~  "Molar pregnancy", 
                              outcome1== "Termination" &  "Miscarriage" %in% event_type ~ "Miscarriage"))  %>%
  ungroup() %>%
  mutate(outcome2 = case_when(outcome1==outcome2 ~ NA_character_,
                              T~outcome2)) %>%
  mutate(bad_outcome_dates_flag=0)

##
pregnancies_singleton_exceptional_baddates <- 
  pregnancies_singleton_exceptional_baddates %>% filter(n_in_group>1) 

#Some exceptionals are just the same outcome recorded on different databases with same/close date
#eg a termination recorded on both tops and smr02
# nothing wrong with retaining information on both records in this case as no conflict
## count of numbers with same outcome type and dates within 7 days.

# get outcomes for all singleton pregnancies with bad dates 
pregnancies_singleton_exceptional_baddates <- pregnancies_singleton_exceptional_baddates %>%
  select( event_date, conception_date, event_type, event_type2, event_id, assumed_gest, gest_at_event, 
          data_source, mother_upi, pregnancy, pregnancy_start_date, pregnancy_end_date, pregnancy_id,
          birth_date, flag_excess_length, number_of_outcomes, exceptional_singleton, aas_selective_reduction, 
          outcome1, outcome2, bad_outcome_dates_flag ) %>%
  group_by(pregnancy_id) %>%
  mutate(outcome1 = case_when("Termination" %in% event_type |"Termination" %in% event_type2 ~ "Termination",
                              "Live birth" %in% event_type|"Live birth" %in% event_type2 ~ "Live birth",
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth",
                              "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              "Molar pregnancy" %in% event_type ~ "Molar pregnancy",
                              "Miscarriage" %in% event_type ~ "Miscarriage")) %>%
  mutate(outcome2 = case_when(outcome1== "Termination" & ("Live birth" %in% event_type |"Live birth" %in% event_type2 )~ "Live birth",
                              outcome1== "Termination" & ("Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 )~ "Stillbirth", 
                              outcome1== "Termination" & "Ectopic pregnancy" %in% event_type ~ "Ectopic pregnancy",
                              outcome1== "Termination" &  "Molar pregnancy" %in% event_type ~  "Molar pregnancy", 
                              outcome1== "Termination" &  "Miscarriage" %in% event_type ~ "Miscarriage"))  %>%
  ungroup() %>%
  mutate(outcome2 = case_when(outcome1==outcome2 ~ NA_character_,
                              T~outcome2))

#add bad dates - these dont allow dual outcomes if dates too far apart
pregnancies_singleton_exceptional_all <- rbind(pregnancies_singleton_exceptional_resolved,
                                               pregnancies_singleton_exceptional_baddates )


##all singleton exceptionals dealt with  now
#- long to wide format
pregnancies_singleton_exceptional_all <-   pregnancies_singleton_exceptional_all  %>% 
  select(c(mother_upi, event_id, data_source, pregnancy_id,  pregnancy_start_date, pregnancy_end_date, outcome1, outcome2 )) %>%
  group_by(pregnancy_id, data_source) %>%
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  ungroup() %>%
  pivot_wider( values_from = c(event_id), names_from = data_source)  #
##retain just the outcomes main dates and the event IDs - these to be used to match back other info later


#bind all singletons back together
pregnancies_singleton2 <- pregnancies_singleton1 %>%
  filter(exceptional_singleton == F) %>%
  rename(outcome1 = event_type) %>%
  select(c(mother_upi, event_id, data_source, pregnancy_id, pregnancy_start_date, pregnancy_end_date, outcome1)) %>%
  group_by(pregnancy_id, data_source) %>%
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  ungroup() %>%
  pivot_wider(values_from = event_id, names_from=data_source) %>%
  bind_rows(pregnancies_singleton_exceptional_all)

rm(pregnancies_singleton1)
###Check multiples N of outcomes ####
log_print("dealing with exceptional multiples")
# Identify multiple pregnancies where we have more outcomes than expected
pregnancies_multiple1 <- pregnancies_multiple1 %>%
  group_by(pregnancy_id) %>%
  mutate(exceptional_multiple = case_when(n() > number_of_outcomes ~ T,
                                          T ~ F)) %>%
  ungroup()

#non exceptionals
pregnancies_multiple_non_exceptional <- pregnancies_multiple1 %>%
  filter(exceptional_multiple == F) %>%
  group_by(pregnancy_id) %>%
  mutate(outcome_no = row_number()) %>%
  ungroup() %>%
  group_by(event_id) %>%
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  ungroup() %>%
  pivot_wider(values_from = event_id, names_from=data_source) %>%
  rename(outcome1 = event_type) %>%
  select(-c(pregnancy, number_of_outcomes, exceptional_multiple, outcome_no, event_date, conception_date)) %>%
  select(mother_upi, pregnancy_id, pregnancy_start_date, pregnancy_end_date, outcome1, everything())
###filter out any miscarriage records that combine with birth records as not possible.
##most of these will end up in exceptional multiples as will lead to excess records
#but can happen that nrs birth records indicates 2 babies but 1 record does not link
#leading to right number being found but with an incompatible combination
pregnancies_multiple_non_exceptional <- pregnancies_multiple_non_exceptional %>% 
  mutate(flag_miscarriage = case_when(outcome1=="Miscarriage" ~ 1, T~0), 
         flag_other_loss =  case_when(outcome1 %in% c("Molar pregnancy","Ectopic pregnancy")~ 1, T~0),
         flag_birth =  case_when(outcome1 %in% c("Live birth", "Stillbirth")~ 1, T~0),
         flag_term =  case_when(outcome1 %in% c("Termination")~ 1, T~0)) %>%
  group_by(pregnancy_id) %>%
  mutate(flag_miscarriage = max_(flag_miscarriage), 
         flag_other_loss =  max_(flag_other_loss),
         flag_birth =  max_(flag_birth),
         flag_term = max_(flag_term) ) %>% ungroup() %>%
  mutate(drop = case_when(flag_birth==1 & outcome1=="Miscarriage" ~1, T~0)) %>%
    select(-c(flag_birth, flag_miscarriage, flag_term, flag_other_loss, drop))

#Non-exceptional multiples are now done. Time to tackle the exceptional multiples, 

#### Create an interim record of resolved pregnancies ####
#i.e. pregnancies where we're happy we've got the outcomes sorted
pregnancies_all_outcomes <- pregnancies_singleton2 %>%
  bind_rows(pregnancies_multiple_non_exceptional)

rm(pregnancies_singleton2)
#### Resolve exceptional multiples ####
pregnancies_multiple_exceptional <- pregnancies_multiple1 %>%
  filter(exceptional_multiple == T)

rm(pregnancies_multiple1)
#add selective tops info
tops_selective <- tops %>% 
  filter(aas_selective_reduction==1) %>% 
  select(aas_event_id, aas_selective_reduction, aas_original_number_of_foetuses, aas_reduced_to)

pregnancies_multiple_exceptional<- left_join(  pregnancies_multiple_exceptional, tops_selective, by = c("event_id"= "aas_event_id") )


# Deal with threatened early losses -
#i.e. pregnancies with an SMR01 or smr02 Loss early on, but which then goes on to produce a live birth
temp_threatened_early_losses <- pregnancies_multiple_exceptional %>%
  group_by(pregnancy_id) %>%
  filter("Live birth" %in% event_type & ("Ectopic pregnancy" %in% event_type | 
                                           "Molar pregnancy" %in% event_type | 
                                           "Miscarriage" %in% event_type)) %>%
  ungroup()

temp_threatened_early_losses_loss_records <- temp_threatened_early_losses %>%
  filter(event_type == "Ectopic pregnancy" |
           event_type == "Molar pregnancy" |
           event_type == "Miscarriage") %>%
  select(pregnancy_id, data_source, event_id) %>%
  group_by(pregnancy_id) %>%
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  pivot_wider(names_from = data_source, values_from = event_id) %>%
  ungroup()

# drop early losses that have a live birth associated with it as that loss must not have happened
# still include associated threatened loss record in the dataframe so we can link it later 
final_threatened_early_losses <- temp_threatened_early_losses %>% 
  # give ectopics and molars a dummy chi as this is more likely to be a chi linkage error (impossible to have with a live birth after it)
  # for multiple exceptionals need to create a temp outcome 1 based on groupings to evaluate
  mutate(birth_in_group =case_when(event_type %in% c("Live birth", "Stillbirth")~1, T~0)) %>%
  group_by(pregnancy_id) %>%
  mutate(birth_in_group =max_(birth_in_group)) %>%
  ungroup() %>%
  mutate(mother_upi = case_when(event_type %in% c("Molar pregnancy", "Ectopic pregnancy") &
                                  birth_in_group ==1 ~ paste0("64",str_pad(string = row_number(),
                                                                           width = 8,
                                                                           side = "left",
                                                                           pad = "0")), 
                                T ~ mother_upi)) %>% 
  filter(event_type != "Miscarriage" ) %>%
  select(mother_upi, pregnancy_id, pregnancy_start_date, pregnancy_end_date, event_type, event_id) %>%
  rename(outcome1 = event_type, slipbd_births_file_1 = event_id) %>%
  left_join(temp_threatened_early_losses_loss_records, by = "pregnancy_id")

separated_records <- final_threatened_early_losses %>% filter(substr(mother_upi,1,2)=="64") %>%
  rowwise() %>%
  mutate(pregnancy_id = UUIDgenerate()) %>% mutate(slipbd_births_file_1 = NA)

final_threatened_early_losses <- final_threatened_early_losses %>% filter(substr(mother_upi,1,2)!="64") %>%
  rbind(separated_records)

#Finally, deal with the remaining exceptional multiples
#exclude early tops followed by a birth (missed term), except in cases of selective reduction

#this needs checking to allow multiple outcomes per fetus
#add checks for outcome dates with termination combinations.  

# CHECK THIS AS SLIPBD FILE HAS AN EXTRA ROW FOR THESE OUTCOMES THAT HAS NOT BEEN GROUPED TOGETHER 
pregnancies_multiple_exceptional <- pregnancies_multiple_exceptional %>%
  mutate(event_type2 = case_when(data_source=="slipbd_births_file" & event_type=="Termination" ~"Stillbirth"))

# remove any pregnancy that has a threatened loss associated with it as it can't have happened
# we have already dealt with these and in final_threatened_early_losses dataframe 
temp_remaining_exceptional_multiples <- pregnancies_multiple_exceptional %>%
  group_by(pregnancy_id) %>%
  mutate(exclude = case_when("Live birth" %in% event_type & ("Ectopic pregnancy" %in% event_type | 
                                                               "Molar pregnancy" %in% event_type | 
                                                               "Miscarriage" %in% event_type) ~ T,
                             T ~F)) %>%
  filter(exclude == F) %>% select(-exclude)# %>%
##highlight termination + birth outcomes

####most are termination/birth combinations
##need to sort which are valid and which are termaiations that look like they didnt happen

# remove all early terminations that have resulted in a live birth and are not due to selective reduction 
temp_remaining_exceptional_multiples <- temp_remaining_exceptional_multiples %>%
  group_by(pregnancy_id) %>%
  mutate(preg_outcome1 = case_when("Live birth" %in% event_type|"Live birth" %in% event_type2 ~ "Live birth", 
                                   "Stillbirth" %in% event_type| "Stillbirth" %in% event_type2 ~ "Stillbirth", 
                                   "Termination" %in% event_type |"Termination" %in% event_type2~ "Termination"),
         preg_outcome2 = case_when("Stillbirth" %in% event_type & "Live birth" %in% event_type ~ "Stillbirth",
                                   "Live birth" %in% event_type & "Termination" %in% event_type  ~"Termination", 
                                   "Stillbirth" %in% event_type & "Termination" %in% event_type  ~"Termination"), 
         preg_outcome3 = case_when(preg_outcome1=="Live birth" & preg_outcome2== "Stillbirth" & 
                                     "Termination" %in% event_type ~ "Termination")) %>% 
  ##pick out invalid early termination records that are succeeded by a birth
  #don't flag selective reductions as this combination is valid.
  mutate(missed_term = case_when(event_type == "Termination" & gest_at_event <17 
                                 & is.na(aas_selective_reduction ) &
                                   (preg_outcome1 =="Live birth" | preg_outcome1=="Stillbirth") ~ 1, T~0)) %>% 
  filter(missed_term==0)  ##remove termination records that appear not to have actually happenned



temp_remaining_exceptional_multiples <- temp_remaining_exceptional_multiples %>%
  group_by(pregnancy_id) %>%
  mutate(exceptional_multiple = case_when(n() > number_of_outcomes ~ T,
                                          T ~ F)) %>% ungroup

table(temp_remaining_exceptional_multiples$exceptional_multiple)
##those that are resolved by this, combine with rest of multiples

temp_resolved_multiple <- temp_remaining_exceptional_multiples %>% filter(exceptional_multiple==F) %>%
  mutate(outcome1 = event_type, outcome2=event_type2) %>%
  select(-preg_outcome1, -preg_outcome2, -preg_outcome3, -missed_term ) %>%
  group_by(pregnancy_id) %>%
  mutate(outcome_no = row_number()) %>%
  ungroup() %>%
  group_by(event_id) %>%
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  ungroup() %>%
  pivot_wider(values_from = event_id, names_from=data_source)%>%
  # rename(outcome1 = event_type) %>%
  select(-c(pregnancy, number_of_outcomes, event_type, outcome_no, aas_selective_reduction,aas_original_number_of_foetuses, aas_reduced_to, exceptional_multiple,  event_date, conception_date)) %>%
  select(mother_upi, pregnancy_id, pregnancy_start_date, pregnancy_end_date, outcome1, everything())

##sometimes end up with different columns labeling different dataset events that dont appear in both dataframes (eg SMR01_1, TOPS_1 etc 0 - from final_threatened_early_losses dataframe) 
##wont always be the same so make a small function to add missing columns
fncols <- function(data, cname) {
  
  add <-cname[!cname%in%names(data)]
  if(length(add)!=0) data[add] <- NA
  
  data
  
}
#identify missing cols form each dataset and create them in the other. 
refnos <- which(!names(temp_resolved_multiple) %in% names(pregnancies_all_outcomes))
refnames<- names(temp_resolved_multiple)[refnos]

pregnancies_all_outcomes<- fncols(pregnancies_all_outcomes, refnames)

refnos <- which(!names(pregnancies_all_outcomes) %in% names(temp_resolved_multiple))
refnames<- names(pregnancies_all_outcomes)[refnos]

temp_resolved_multiple <- fncols(temp_resolved_multiple,  refnames)

##now multiples should rbind without a problem
pregnancies_all_outcomes <- pregnancies_all_outcomes %>%
  bind_rows(temp_resolved_multiple) 

##having got rid of these the remaining termination/birth combinations should be valid - redo preg outcomes
#these will still be "exceptional" in that the "extra" termination record is retained  - but has been checked and seems valid

temp_remaining_exceptional_multiples1 <- temp_remaining_exceptional_multiples %>%   
  filter(exceptional_multiple==T) %>%
  group_by(pregnancy_id) %>%
  mutate(selective_reduction_preg_flag = case_when(max_(aas_selective_reduction)==1 ~1, 
                                                   T ~ 0)) %>%
  ungroup()

# deal with non-selective terminations that have resulted in a birth first 

# use this dataset for creating the duplicate rows
# need to get the correct number of rows per birth 
remaining_exceptional_multiples_non_selective_reductions_terminations_only <- temp_remaining_exceptional_multiples1 %>%
  filter(selective_reduction_preg_flag == 0) %>% 
  group_by(pregnancy_id) %>%
  filter("Termination" %in% event_type) %>%
  mutate(total_births = sum(data_source == "slipbd_births_file")) %>%
  filter(data_source != "slipbd_births_file") %>% 
  group_by(pregnancy_id, data_source) %>%
  slice(rep(1:n(), each = total_births)) %>%
  select(-total_births)

remaining_exceptional_multiples_non_selective_reductions <- temp_remaining_exceptional_multiples1 %>%
  filter(selective_reduction_preg_flag == 0) %>% 
  group_by(pregnancy_id) %>%
  filter("Termination" %in% event_type) %>%
  # # need to repeat termination events as when we pivot_wider it can't cope and creates a list for slipbd_event_id
  filter(data_source == "slipbd_births_file") %>%
  bind_rows(remaining_exceptional_multiples_non_selective_reductions_terminations_only) %>%
  arrange(pregnancy_id, data_source) %>%
  group_by(pregnancy_id, data_source) %>%
  mutate(termination_event =  1:n()) %>% 
  group_by(pregnancy_id) %>%
  mutate(outcome1 = case_when("Termination" %in% event_type | "Termination" %in% event_type2 ~ "Termination"), 
         outcome2 = case_when("Live birth" %in% event_type | "Live birth" %in% event_type2 ~ "Live birth", 
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth")) 


remaining_exceptional_multiples_selective_reductions <- temp_remaining_exceptional_multiples1 %>%
  filter(selective_reduction_preg_flag == 1) %>%
  group_by(pregnancy_id) %>% 
  # group by most likely reduced foetus. These seem to be exceptional cases where the reduction occurs quite late and removal of foetus hasn't worked? 
  # CHECK WITH RACHAEL
  mutate(reduced_foetus = case_when(event_type != "Live birth" ~ 1)) %>%
  group_by(pregnancy_id, reduced_foetus) %>%
  mutate(outcome1 = case_when("Termination" %in% event_type | "Termination" %in% event_type2 ~ "Termination", 
                              "Live birth" %in% event_type | "Live birth" %in% event_type2 ~ "Live birth", 
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth"), 
         outcome2 = case_when("Live birth" %in% event_type | "Live birth" %in% event_type2 ~ "Live birth", 
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth"))

# right now there are 3 live births but 2 have different dates of births... create a new pregnancy for these events (likely a linkage error)
remaining_exceptional_multiples <-  temp_remaining_exceptional_multiples1 %>%
  filter(selective_reduction_preg_flag == 0) %>% 
  group_by(pregnancy_id) %>%
  filter(!("Termination") %in% event_type) %>%
  group_by(pregnancy_id, event_date) %>%
  mutate(new_pregnancy_id = UUIDgenerate())# feel like we need to have something that keeps the same pregnancy id with that particular baby for joining

##to deal with creating variables if zero rows once filtered (if variables not created it stops next section running)
if(nrow(remaining_exceptional_multiples) ==0){
  remaining_exceptional_multiples <- remaining_exceptional_multiples %>%
    mutate(fetus=NA, outcome1=NA_character_, outcome2=NA_character_)}

if(nrow(remaining_exceptional_multiples) >0){
remaining_exceptional_multiples <- remaining_exceptional_multiples %>% 
  group_by(new_pregnancy_id) %>%
  mutate(outcome1 = case_when("Termination" %in% event_type | "Termination" %in% event_type2 ~ "Termination", 
                              "Live birth" %in% event_type | "Live birth" %in% event_type2 ~ "Live birth", 
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth"), 
         outcome2 = case_when("Live birth" %in% event_type | "Live birth" %in% event_type2 ~ "Live birth", 
                              "Stillbirth" %in% event_type | "Stillbirth" %in% event_type2 ~ "Stillbirth"), 
         fetus = 1:n()) %>% ungroup()
}


temp_remaining_exceptional_multiples2 <- remaining_exceptional_multiples_non_selective_reductions %>%
  bind_rows(remaining_exceptional_multiples_selective_reductions) %>%
  bind_rows(remaining_exceptional_multiples)

# 
 
temp_remaining_exceptional_multiples_wide <- temp_remaining_exceptional_multiples2 %>% 
  select(mother_upi, pregnancy_id, new_pregnancy_id, pregnancy_start_date, pregnancy_end_date, event_id, data_source, termination_event, fetus, outcome1:outcome2) %>%
  mutate(data_source = paste0(data_source, "_1")) %>%
    pivot_wider(names_from = data_source, values_from = event_id) %>%
  select(-c(termination_event, fetus)) %>%
  ungroup() #%>%
#rename(outcome1 = event_type)

##fix issue caused by the 5-fetus selective reduction.
if(is.list(temp_remaining_exceptional_multiples_wide$slipbd_births_file_1)){
temp_remaining_exceptional_multiples_wide<- temp_remaining_exceptional_multiples_wide %>%
  unnest(slipbd_births_file_1, keep_empty = TRUE) %>% unnest(ToPs_1, keep_empty = TRUE) %>% 
  unnest(SMR02_1, keep_empty = TRUE)
}

# Add on our remaining outcomes
log_print("Joining all outcomes back together")

pregnancies_all_outcomes <- pregnancies_all_outcomes %>% mutate(new_pregnancy_id = NA_character_) %>%
  bind_rows(final_threatened_early_losses %>% mutate(new_pregnancy_id = NA_character_)) %>%
  bind_rows(temp_remaining_exceptional_multiples_wide) 

##add back antenatal bookings####
#Pivot antenatal bookings to one row per pregnancy
pregnancies_an_booking_wide <- pregnancies_an_booking %>%
  select(mother_upi, pregnancy_id, event_id, data_source, pregnancy_start_date) %>%
  rename(an_mother_upi = mother_upi, an_pregnancy_start_date = pregnancy_start_date) %>%
  group_by(pregnancy_id) %>%
  mutate(an_pregnancy_start_date = min(an_pregnancy_start_date)) %>% # We need to ensure that each pregnancy has a consistent AN Booking conception date, otherwise we'll end up with multiple lines per pregnancy after the pivot
  mutate(data_source = paste0(data_source, "_", row_number())) %>%
  pivot_wider(names_from = data_source, values_from = event_id)

#Add antenatal booking back on to our main df
pregnancies_all_outcomes_and_ongoing <- pregnancies_all_outcomes %>%
  full_join(pregnancies_an_booking_wide, by = "pregnancy_id") %>%
  mutate(mother_upi = case_when(is.na(mother_upi) ~ an_mother_upi,
                                T ~ mother_upi)) %>%
  mutate(pregnancy_start_date = case_when(is.na(pregnancy_start_date) ~ an_pregnancy_start_date,
                                          T ~ pregnancy_start_date)) %>%
  mutate(outcome1 = case_when(is.na(outcome1) & !is.na(anbooking_1) ~ "Ongoing",
                              T ~ outcome1)) %>%
  mutate(pregnancy_id = case_when(!is.na(new_pregnancy_id) ~ new_pregnancy_id, 
                                  T ~ pregnancy_id)) %>%
  select(-c(an_mother_upi, an_pregnancy_start_date, new_pregnancy_id))

# remove termination duplicates 
# these are terminations that occur after 24 weeks gestation but do not have an nrs record
# any termination that occurs after 24 weeks should have a live or still birth record, so we assume that smr02 or aas orphans have already been added to the database
# need to remove these smr02 or aas orphans 
dup_terminations <- pregnancies_all_outcomes_and_ongoing %>% 
  filter((outcome1 == "Termination" | outcome2 == "Termination") & gest_at_event >= 24 & (is.na(slipbd_births_file_1)))

pregnancies_all_outcomes_and_ongoing <- pregnancies_all_outcomes_and_ongoing %>%
  filter(ToPs_1 %notin% dup_terminations$ToPs_1)


log_print("Link to emigrations and deaths")
##Check unknown outcomes against deaths and CHI database. 
unknown_out <- pregnancies_all_outcomes_and_ongoing %>% filter(outcome1=="Ongoing")

clear_temp_tables(SMRAConnection)
migration <- SMRAConnection %>% tbl(dbplyr::in_schema(chi_schema,chi_table)) %>% 
  select(CHI_NUMBER, UPI_NUMBER, TRANSFER_OUT_CODE, DATE_TRANSFER_OUT) %>% 
  inner_join(unknown_out %>% select(mother_upi) %>% distinct() %>% rename(CHI_NUMBER = mother_upi), copy = TRUE) %>% 
  distinct() %>% 
  collect() %>% filter(!is.na(TRANSFER_OUT_CODE)) %>%
  group_by(CHI_NUMBER) %>%
  mutate(n=n()) %>% ungroup 


migration <- migration %>% 
  filter(as.Date(DATE_TRANSFER_OUT) >= as.Date("2000-01-01") )  %>%# remove transfers before cohort start date
  filter(substr(TRANSFER_OUT_CODE,1,1) %in% c("C", "E"))

#Some have multiple transfer dates so checks against pregnancy dates needed

unknown_out <- unknown_out %>% mutate(max_end_date = as.Date(pregnancy_start_date) + (38*7) )
unknown_out <- left_join(unknown_out, migration,by = c("mother_upi" = "CHI_NUMBER"))
migrated <- unknown_out %>% mutate(relevant_migration = case_when(DATE_TRANSFER_OUT >= pregnancy_start_date & 
                                                                    DATE_TRANSFER_OUT <= max_end_date ~1,T~0  )) %>%
  filter(relevant_migration==1) %>%
  mutate(outcome1="Unknown - emigrated")

#some have 2 out dates within the plausible pregnancy period - just select the first
migrated <- migrated %>% arrange(pregnancy_id, DATE_TRANSFER_OUT)%>% group_by(pregnancy_id) %>% slice(1)


##add this info back into main pregnancies file.
pregnancies_all_outcomes_and_ongoing <- pregnancies_all_outcomes_and_ongoing %>%
  filter(!(pregnancy_id) %in% migrated$pregnancy_id) %>%
  bind_rows(migrated)

##Check known outcomes against emigration data 

##check vs deaths 
#check all deaths even for known endpoints
clear_temp_tables(SMRAConnection)
deaths <- SMRAConnection %>% tbl(dbplyr::in_schema(analysis_schema, deaths_table)) %>% 
  select(UPI_NUMBER, DATE_OF_DEATH) %>% 
  inner_join(pregnancies_all_outcomes_and_ongoing %>% select(mother_upi) %>% 
               distinct() %>% rename(UPI_NUMBER = mother_upi), copy = TRUE) %>% 
  distinct() %>% 
  collect() 

deaths <- deaths %>%
  filter(as.Date(DATE_OF_DEATH) >= as.Date("2000-01-01"))

pregnancies_deaths <- pregnancies_all_outcomes_and_ongoing  %>%
  left_join(deaths, by = c("mother_upi"= "UPI_NUMBER")) %>% 
  filter(!is.na(DATE_OF_DEATH)) %>%
  # need to include postpartum deaths as well (6 weeks after end of pregnancy)
  mutate(temp_preg_end_date = case_when(is.na(pregnancy_end_date) ~ as.Date(pregnancy_start_date) + weeks(38), 
                                        T ~ as.Date(pregnancy_end_date))) %>%
  mutate(temp_post_partum_end_date = temp_preg_end_date + weeks(6)) %>%
  mutate(death_in_pregnancy = case_when(DATE_OF_DEATH >= pregnancy_start_date & 
                                          DATE_OF_DEATH <= temp_preg_end_date ~1,
                                        T~0), 
         death_post_pregnancy = case_when(DATE_OF_DEATH > temp_preg_end_date &
                                            DATE_OF_DEATH <= temp_post_partum_end_date ~1,
                                          T~0)) %>%
  mutate(bad_dates = case_when(DATE_OF_DEATH < pregnancy_start_date ~ 1, T~0))

pregnancies_deaths <- pregnancies_deaths %>% filter(death_in_pregnancy==1 | death_post_pregnancy == 1) %>% 
  mutate(outcome1 = case_when(death_in_pregnancy==1 & outcome1=="Ongoing" ~ "Maternal death", 
                              T~ outcome1), 
         outcome2 = case_when(death_in_pregnancy==1 & outcome1!="Ongoing" & outcome1!="Maternal death" & is.na(outcome2) ~ "Maternal death", 
                              T~ outcome2) ) %>%
  mutate(pregnancy_end_date = case_when(is.na(pregnancy_end_date) & death_in_pregnancy==1 ~ DATE_OF_DEATH,
                                        is.na(pregnancy_end_date) & death_in_pregnancy==1 ~ temp_preg_end_date,
                                        T~pregnancy_end_date)) %>%
  select(-temp_preg_end_date, -temp_post_partum_end_date)


pregnancies_all_outcomes_and_ongoing <- pregnancies_all_outcomes_and_ongoing %>%
  filter(!pregnancy_id %in% pregnancies_deaths$pregnancy_id ) %>%
  bind_rows(pregnancies_deaths )

#combine migration and deaths with the main pregnancy file
pregnancies_final <-  pregnancies_all_outcomes_and_ongoing %>%
  select(-c(n,bad_dates, flag_excess_length ))

pregnancies_final <- pregnancies_final %>%
  mutate(outcome1 = case_when(outcome1 =="Ongoing" & 
                                (pregnancy_start_date < Sys.Date() - (40*7)) ~ "Unknown", 
                              T~ outcome1))
#Save out final pregnancy record
saveRDS(pregnancies_final, paste0(folder_temp_data, "script3_pregnancy_record.rds"))
log_print("finished script 3")

