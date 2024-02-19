
# CREATE LIVE BIRTH FILE -------------------------------------------------------

# We need to create a file that collates all births
# Uses NRS live births and NRS stilllbirths as the spine

## Import and clean data -------------------------------------------------------

# read in data
nrs_live_births <- read_rds(paste0(folder_temp_data, "nrs_live_births.rds"))
smr02_births <- read_rds(paste0(folder_temp_data, "smr02_data.rds")) %>%
  filter(smr02_outcome_type == "Live birth" |smr02_outcome_type == "Stillbirth" )
nrs_stillbirths <- readRDS(paste0(folder_temp_data, "NRS_sb.rds")) 

### Clean NRS data and join live births and still births together -------------- 
log_print("Joining NRS live and stillbirths")
#resolve multiples in lb and sb NRS records into pregnancy id groups


names(nrs_live_births) <- sub(".*nrslb_", "", names(nrs_live_births))
names(nrs_stillbirths) <- sub(".*nrssb_", "", names(nrs_stillbirths))


names(nrs_stillbirths)[which(!names(nrs_stillbirths) %in% names(nrs_live_births))]

nrs_stillbirths <- nrs_stillbirths %>% mutate(duplicate_baby_upi_flag = NA, 
                                              baby_upi_number = child_upi_number) %>%
  mutate(livebirth=0) %>% select(-child_upi_number)


nrs_live_births <- nrs_live_births %>% 
  rename(baby_upi_number = child_upi_number ,
         duration_of_pregnancy = estimated_gestation, 
         estimated_date_of_conception = estimated_conception_date) %>%
  mutate(assumed_gestation =1, #all are imputed for nrs live births
         weight_of_foetus   = NA, primary_cause_of_death = NA , 
         secondary_cause_of_death_0 = NA, secondary_cause_of_death_1  = NA, 
         secondary_cause_of_death_2 = NA, secondary_cause_of_death_3 = NA, 
         termination=  NA, stillbirth= NA )  %>% mutate(livebirth=1)

NRS_all_births <- rbind(nrs_live_births, nrs_stillbirths) 
# run the process to identify and group births in same multiples pregnancies
#and label ones in same pregnancy with same ID
#and create same dummy upi if mother upi is missing or >1 valid upi in group

linked_ids <- NRS_all_births %>% filter(!is.na(multiple_births_linked_records)) %>%
  select(multiple_births_linked_records, triplicate_id) %>%
  separate(multiple_births_linked_records, into = c("id1", "id2", "id3", "id4"),sep="(?=[A-Z])", remove = F) %>% 
  mutate(id2=  ifelse(id2 ==triplicate_id,NA,id2), #remove ids if they duplicate the nrs id f that record
         id3=  ifelse(id3 ==triplicate_id,NA,id3),
         id4=  ifelse(id4 ==triplicate_id,NA,id4))

#alphabetical order so that all rows from same group should end up with same long identifier (all the triplicate IDS)
linked_ids <- linked_ids %>% 
  select(-id1) %>% #(id1 is always blank)
  mutate(all_ids_group = pmap_chr(list(triplicate_id, id2, id3, id4),
                                  ~paste(sort(c(...)), collapse = "")))

long_id <- linked_ids %>% 
  select(triplicate_id, all_ids_group)#

# create nrs pregnancy ids based on grouping of joined triplicate IDS  
grouped_ids <- long_id %>% group_by(all_ids_group) %>% summarise(n_of_rows = n()) %>% ungroup() %>%
  mutate(nrs_preg_id =  paste0("nrs_preg_", row_number()))
grouped_ids <- grouped_ids  %>% 
  separate(all_ids_group, into = c("id1", "id2", "id3", "id4", "id5"),sep="(?=[A-Z])", remove = F) %>%
  pivot_longer(id1:id5, names_to = "id", values_to = "triplicate_id") %>% 
  filter(!is.na(triplicate_id), triplicate_id!="") %>% select(-id) %>%
  unique()

grouped_ids <- grouped_ids %>% 
  filter(triplicate_id != all_ids_group) %>%# filter out if only own ID in group
  select(-n_of_rows) 

#a couple of cases of multiples group ids linking to one record triplicate ID
# two causes: 1."phantom" records 
#(ID appears in linked ids but correponding record itself does not exist - could be due to data entry error)
# 2. in the case of a triplet or more pregnancy where for some records
#all the linked records do not appear in the linked id column
#select triplicate ID with highest group membership to ensure no records are missed
grouped_ids <- grouped_ids  %>% group_by(all_ids_group) %>%
  mutate(group_membership = n()) %>%
  ungroup() %>% 
  group_by(triplicate_id) %>%
  mutate(count=n()) %>%
  arrange(-group_membership) %>% slice(1) %>% select(-count, -group_membership)

grouped_ids_check <- grouped_ids %>% group_by(triplicate_id) %>% mutate(count=n())
#table(grouped_ids_check$count) 


NRS_all_births <- left_join(NRS_all_births, grouped_ids, by=c( "triplicate_id"  ="triplicate_id"))

##give a preg_id to all the singletons as well 
NRS_all_births <-NRS_all_births %>% 
  mutate(nrs_preg_id = 
           ifelse(is.na(nrs_preg_id), paste0("nrs_preg",
                                             row_number()+ max_(as.numeric(substr(NRS_all_births$nrs_preg_id,10,20)))),
                  nrs_preg_id))


##Check for upi numbers of mother being the same in groups
# if they are not, create a dummy upi/use the valid upi number 
NRS_all_births <- NRS_all_births %>%  
  mutate(valid_upi = ifelse(chi_check(mother_upi_number)=="Valid CHI",1,0), group_number = substr(nrs_preg_id,10,20) )%>%
  group_by(nrs_preg_id) %>% 
  arrange(nrs_preg_id, mother_upi_number) %>%
  mutate(mismatch_upi = ifelse(max(mother_upi_number) != min(mother_upi_number),1,0)) %>%
  mutate(count_valid_upi =  sum(valid_upi)) %>%
  mutate(mother_upi_number = 
           case_when(mismatch_upi==1 & count_valid_upi==0 ~  # if no valid upi in group create single dummy based on group number
                       
                       paste0("46",str_pad(string = group_number,
                                           width = 8,
                                           side = "left",
                                           pad = "0")), 
                     mismatch_upi==1 & count_valid_upi >1 ~    #if more than one valid upi, one must be wrong so change to dummy as above.
                       paste0("46",str_pad(string = group_number,
                                           width = 8,
                                           side = "left",
                                           pad = "0")), 
                     mismatch_upi==1 & count_valid_upi==1 ~ 
                       first(mother_upi_number), #if one valid upi, use this (it will be first in group due to arrange above)
                     T~ mother_upi_number)) %>% 
  ungroup() %>%
  select(-c(mismatch_upi, group_number, count_valid_upi, valid_upi))

saveRDS(NRS_all_births, paste0(folder_temp_data, "nrs_all_births.rds"))
log_print("finished Joining NRS live and stillbirths")
###
NRS_all_births <- readRDS(paste0(folder_temp_data, "nrs_all_births.rds"))
NRS_all_births <- NRS_all_births %>% group_by(nrs_preg_id) %>% 
  mutate(group_members = n()) %>%
  ungroup() %>%
  mutate(multiple_pregnancy = case_when(total_births_live_and_still >1~1,
                                        T ~ 0)) %>%
  mutate(multiple_pregnancy = case_when(group_members >1 ~ 1, 
                                        T~multiple_pregnancy))  ##label multiple if maximum n is >1

#check1 <- NRS_all_births %>% group_by(nrs_preg_id) %>%  filter(min_(group_members) > min(total_births_live_and_still))
#check2 <- NRS_all_births %>% group_by(nrs_preg_id) %>%  filter(min_(group_members) < max(total_births_live_and_still))

NRS_all_births_extra_grps <- NRS_all_births %>% ungroup %>%
  filter(group_members ==1) %>% group_by(mother_upi_number, date_of_birth) %>% count() %>% filter(n>1)
NRS_extra_grps <- NRS_all_births %>% 
  filter(mother_upi_number %in% NRS_all_births_extra_grps$mother_upi_number & date_of_birth %in% NRS_all_births_extra_grps$date_of_birth)  %>% filter(group_members ==1)


### Clean NRS  Births ------------------------------------------------------
nrs_births <- NRS_all_births %>% #filter(livebirth==1) %>%  
  rename_with( ~ paste0("nrs_", .))

temp_nrs_births <- nrs_births %>%
  mutate(max_births = pmax(nrs_total_births_live_and_still, nrs_group_members)) %>%
  group_by(nrs_nrs_preg_id) %>%
  mutate(max_babies = max_(max_births)) %>% ungroup()%>% #maximum count of births in group
  select(-max_births) 

temp_nrs_births  <- temp_nrs_births  %>% 
  mutate(baby_type = case_when(max_babies ==1 ~ "singleton",
                               T ~ "multiple")) %>% # multiple if either total births >1 or n babies in group >1 
  # this does mean a few with total births >1 for which the linked record could not be found are given multiple status
  # determine baby order in nrs live births 
  group_by(nrs_mother_upi_number, nrs_date_of_birth) %>%
  arrange(nrs_time_of_birth) %>%
  mutate(baby_order = seq(1:n())) %>%
  mutate(baby_order = paste0("baby", baby_order)) %>%
  ungroup() %>%
  # some babies have wrong chi associated with it so we want to remove those babies upis as well 
  mutate(chi_dob = dob_from_chi(nrs_baby_upi_number, min_date = cohort_start_date, max_date = cohort_end_date)) %>%
  mutate(wrong_chi = case_when(chi_dob != nrs_date_of_birth ~ T, 
                               T ~ F)) %>%
  mutate(nrs_baby_upi_number = case_when(wrong_chi == T ~ NA_character_, 
                                         T ~ nrs_baby_upi_number)) %>%
  # need to remove duplicate upis but with different mothers 
  group_by(nrs_baby_upi_number, baby_order) %>% 
  mutate(duplicate = case_when(n() > 1 ~ T)) %>%
  ungroup() %>%
  mutate(nrs_baby_upi_number = case_when(duplicate == T ~ paste0("55",str_pad(string = row_number(),
                                                                              width = 8,
                                                                              side = "left",
                                                                              pad = "0")),
                                         T ~ nrs_baby_upi_number))

# there are some mother upi's associated with several  babies who are born on the same day 
# but these are not multiple pregnancies according to grouping and total_births field
# need to remove these mother upis as they are wrong
temp_nrs_births  <- temp_nrs_births %>% 
  group_by(nrs_mother_upi_number, nrs_date_of_birth) %>%
  # this won't help if mother upi associated with eg 2 groups of multiples in error, that scenario needs to be checked and rectified in another step
  mutate(duplicate_mother_upi = case_when(n() > 1 & max_babies == 1 ~ T)) %>% 
  ungroup() %>% 
  mutate(group_number = substr(nrs_nrs_preg_id,10,20)) %>%
  mutate(nrs_mother_upi_number = case_when(duplicate_mother_upi == T ~ paste0("47",str_pad(string =  group_number,
                                                                                           width = 8,
                                                                                           side = "left",
                                                                                           pad = "0")),
                                           T ~ nrs_mother_upi_number)) 

#check for >1 group of multiples given same mother id
temp_nrs_births <- temp_nrs_births %>% 
  group_by(nrs_mother_upi_number, nrs_date_of_birth) %>% 
  mutate(count_mother_upi_date = n()) %>% 
  ungroup()


##check these records
#mothers_excess <- temp_nrs_births2 %>% filter(count_mother_upi_date > max_babies)
#2 mother upis affected, one applied to 2 pairs of twins on same day one to 3 pairs. 
#the pairing is genuine, linked on triplicate ids, but the mother UPI has to be wrong for all but one pair
temp_nrs_births <- temp_nrs_births %>%  
  mutate(duplicate_mother_upi = case_when(count_mother_upi_date > max_babies ~ T)) %>% ungroup() %>% 
  mutate(nrs_mother_upi_number = case_when(duplicate_mother_upi == T ~ paste0("47",str_pad(string = group_number,
                                                                                           width = 8,
                                                                                           side = "left",
                                                                                           pad = "0")),
                                           T ~ nrs_mother_upi_number)) 

# select appropriate columns
temp_nrs_births <- temp_nrs_births %>% 
  select(nrs_mother_upi_number, nrs_baby_upi_number, nrs_date_of_birth, nrs_nrs_preg_id,
         nrs_estimated_date_of_conception, nrs_event_id, baby_type, baby_order, nrs_time_of_birth, max_babies) %>%
  mutate(source = "nrs") 

#test <- temp_nrs_births %>% group_by(nrs_baby_upi_number, baby_order) %>% filter(n() > 1)
log_print("finished cleaning NRS combined file")
log_print("start cleaning smr02")
### Clean SMR02 Births ----------------------------------------------------
temp_smr02_births <- smr02_births %>%
  #group multiples
  group_by(smr02_upi_number, smr02_date_of_delivery) %>% 
  mutate(baby_type = case_when(n() > 1 ~ "multiple",
                               smr02_num_of_births_this_pregnancy == 1 ~ "singleton",
                               T ~ "multiple")) %>% ungroup() %>%
  group_by(smr02_preg_id) %>% 
  mutate(baby_type_preg_id = case_when(n() > 1 ~ "multiple",
                                       smr02_num_of_births_this_pregnancy == 1 ~ "singleton",
                                       T ~ "multiple")) %>% 
  ungroup() %>%
  select(smr02_preg_id, smr02_upi_number, smr02_baby_upi_number, smr02_date_of_delivery,
         smr02_estimated_conception_date, event_id, baby_type,baby_type_preg_id , smr02_baby, smr02_num_of_births_this_pregnancy) %>%
  mutate(source = "smr02")

# note: we keep time of birth to determine baby order for multiples whose mother might not be chi-seeded yet 
colnames(temp_nrs_births) <- c("mother_upi", "baby_upi", "event_date", "preg_id", 
                               "conception_date", "event_id", "baby_type", "baby_order", "nrs_time_of_birth", "n_babies", "data_source")


colnames(temp_smr02_births) <- c( "preg_id","mother_upi", "baby_upi", "event_date",
                                  "conception_date", "event_id", "baby_type", "baby_type_smr_pregid", "baby_order", "n_babies", "data_source")

log_print("SMR02 file cleaned") 
log_print("Start creating combined birth file") 

## Create Birth File ------------------------------------------------------
#### create births file using NRS live births as the spine
# we bind rows rather than left join
# because we miss out babies who have a missing mother CHI in smr02 or a missing CHI in general
births <- bind_rows(temp_nrs_births,
                    temp_smr02_births) %>%
  # remove dummy upis for mothers who had missing data
  mutate(mother_upi = case_when(str_starts(mother_upi, "4") ~ NA_character_,
                                T ~ mother_upi))


smr02_orphans1 <- births %>%
  group_by(baby_upi) %>%
  filter(min_(data_source) == "smr02" & max_(data_source) == "smr02") 
# 48686 babies who only appear in smr02 

### Finding correct mother UPI for each baby -----------------------------------
log_print("Finding correct mother UPI for each baby") 
# there are babies who have duplicate upis despite having different mother's associated with them 
# we need to remove these babies upis because we cannot link them 
duplicate_babies_diff_mother = births %>%
  group_by(baby_upi, data_source) %>% 
  filter(n_distinct(mother_upi) > 1)

# we need to group the babies together to account for missing baby chi in each data set
births <- births %>%
  group_by(baby_upi, data_source) %>% 
  mutate(dup_baby_upi_diff_mother = case_when(n_distinct(mother_upi) > 1 & 
                                                !is.na(mother_upi) ~ T, 
                                              T ~ F)) %>%
  ungroup() %>%
  # we don't know what baby's upi is right so set dummy baby upi if there are duplicates
  mutate(baby_upi = case_when(dup_baby_upi_diff_mother == T ~ paste0("56",str_pad(string = row_number(),
                                                                                  width = 8,
                                                                                  side = "left",
                                                                                  pad = "0")), 
                              T ~ baby_upi)) %>%
  group_by(baby_upi) %>%
  mutate(flag_inconsistent_mother = case_when(n_distinct(mother_upi) > 1 ~ T, 
                                              T  ~ F)) %>%
  ungroup()

missing_event_date <- births %>% filter(is.na(event_date))
#write_rds(missing_event_date, paste0(folder_data_path, "problems/births_missing_event_date.rds"))


log_print("make mother upis consistent") 
# make mother upis consistent
inconsistent_mother_upi <- births %>%
  filter(flag_inconsistent_mother == T) %>%
  group_by(baby_upi) %>%
  arrange(data_source) %>%
  mutate(mother_upi = first_non_na(mother_upi)) %>%
  mutate(flag_inconsistent_mother = case_when(n_distinct(mother_upi) > 1 ~ T)) %>%
  ungroup()

consistent_mother_upi <- births %>%
  filter(flag_inconsistent_mother == F) %>%
  ungroup() %>%  
  mutate(mother_upi = case_when(is.na(mother_upi)  ~ paste0("48",str_pad(string = row_number(),
                                                                         width = 8,
                                                                         side = "left",
                                                                         pad = "0")),
                                T ~ mother_upi)) 

# all babies should have a mother with the same upi now
births2 <- bind_rows(inconsistent_mother_upi, consistent_mother_upi) %>%
  select(-flag_inconsistent_mother) %>%
  filter(!(is.na(event_date))) %>%
  moving_index_deduplication(., mother_upi, event_date, 2) %>%
  group_by(mother_upi, event) %>%
  arrange(event_date, data_source) %>%
  mutate(event_date = max_(event_date)) %>%
  ungroup() 

##count babies who only appear in smr02
smr02_orphans3 <- births2 %>%
  group_by(baby_upi) %>%
  filter(min_(data_source) == "smr02" & max_(data_source) == "smr02") 



### Find correct baby upi for each event ---------------------------------------
log_print("Find correct baby upi for each event ")

#
births2 %<>% 
  # we need to make sure we have our multiple babies in the right order now 
  #we have a mother upi associated with each baby
  # some multiples would not have a mother attached to them
  mutate(nrs_multiple = case_when(data_source == "nrs" & baby_type == "multiple" ~ T,
                                  T ~ F)) %>%
  # for babies missing a mother upi in nrslb, there are some babies who are coded 
  # as singletons but appear to be multiples in smr02 
  # by making mother upi consistent (above) for these babies, they become identifiable as multiples  

  group_by(mother_upi, event_date, data_source) %>%
  mutate(nrs_multiple = case_when(data_source == "nrs"&        
                                    !is.na(mother_upi) & 
                                    n() > 1 & baby_type == "singleton" & 
                                    min_(nrs_time_of_birth) != max_(nrs_time_of_birth) ~ T, 
                                  T ~ nrs_multiple)) 
births2 <- births2 %>%  
  ungroup() %>% 
  mutate(rownumber = row_number()) %>%
  group_by(mother_upi, event_date, data_source)%>%
  mutate(birth_id = case_when(nrs_multiple==T ~ paste0("birth_id_", first(rownumber)))) %>%
  ungroup() %>% 
  group_by(mother_upi, preg_id) %>%
  mutate(birth_id = case_when(nrs_multiple!=T ~ paste0("birth_id_", first(rownumber)), 
                              T ~ birth_id))  %>%
  ungroup()

log_print("correct baby order for multiples")
# correct baby order for multiples as some babies in a multiple had a missing mother chi 
nrs_multiples <- births2 %>%
  filter(nrs_multiple == T) %>%
  mutate(baby_type = "multiple") %>%
  group_by(mother_upi, birth_id) %>%
  arrange(nrs_time_of_birth) %>%
  mutate(baby_order = paste0("baby", seq(1:n())))

# we now need to make sure we have the right baby upi for all babies
births2 <- births2 %>%
  filter(nrs_multiple == F) %>%
  bind_rows(nrs_multiples) %>%
  mutate(baby_upi = case_when(str_starts(baby_upi, "5") ~ NA_character_,
                              T ~ baby_upi)) %>%
  #### Fill in missing baby UPIs where possible
  #### For every mother and event date (across smr and nrs), we want to make 
  #sure that we have the same baby upi associated with it
  group_by(mother_upi, event_date) %>%
  mutate(inconsistent_baby_upi = case_when(min_(baby_upi) != max_(baby_upi) ~ T,
                                           min(is.na(baby_upi)) | max(is.na(baby_upi)) ~ T,
                                           T ~ F)) %>%
  ungroup() %>%
  # if there isn't a consistent baby upi, we need to be able to fix it (and can
  #only do this if we have a mother upi)
  mutate(correctable_baby_upi = case_when(inconsistent_baby_upi == T & !is.na(mother_upi) ~ T,
                                          T ~ F))


# need to deal with singletons and multiples separately 
log_print("Splitting singletons and multiples to deal with duplicates")
#### Singletons ---------------------------------------------------------------
# for singletons, group by mother upi and event date to get correct upi 
inconsistent_baby_upi_singleton = births2 %>%
  filter(correctable_baby_upi == T & baby_type == "singleton") %>%
  group_by(mother_upi, event_date) %>%
  # Select the NRS upi to be the correct upi
  arrange(data_source) %>%
  mutate(baby_upi = case_when(correctable_baby_upi == T ~ first_non_na(baby_upi), 
                              T ~ baby_upi)) %>%
  # there are some instances where a baby appears twice in nrs or has their siblings chi number 
  group_by(baby_upi, data_source) %>%
  mutate(duplicate_upi = case_when(!is.na(baby_upi) & n() > 1 ~ T, 
                                   T ~ F)) %>% 
  ungroup() 


# shouldn't have duplicate baby chi in singletons - check here
dups_singletons <- inconsistent_baby_upi_singleton %>% 
  group_by(baby_upi, data_source) %>% 
  filter(n() > 1) %>%
  filter(!is.na(baby_upi)) 

#### Multiples ----------------------------------------------------------------
# multiples require slightly more work when dealing with upis 
# this is because there are a lot of duplicate chis for multiples in nrslb 
inconsistent_baby_upi_multiple = births2 %>%
  filter(correctable_baby_upi == T & baby_type == "multiple") %>%
  group_by(baby_upi, data_source) %>%
  mutate(duplicate_upi = case_when(!is.na(baby_upi) & data_source == "nrs" & n() > 1 ~ T, 
                                   T ~ F)) %>%
  group_by(mother_upi, event_date) %>%
  mutate(duplicate_upi = case_when(min_(duplicate_upi) == T | max_(duplicate_upi) == T ~ T, 
                                   T ~ F)) %>%
  group_by(mother_upi, event_date, baby_order) %>%
  # most duplicate baby upis in multiples come from nrs - we take the upi from smr02 in this instance 
  # this doesn't work for some babies who do not have an smr02 record but fixes most 
  arrange(desc(data_source)) %>%
  mutate(new_baby_upi = case_when(duplicate_upi == T ~ first_non_na(baby_upi), 
                                  T ~ NA_character_)) %>%
  # for all other babies where there isn't a duplicate upi, we select the nrs upi 
  arrange(data_source) %>%
  mutate(new_baby_upi = case_when(duplicate_upi == F ~ first_non_na(baby_upi), 
                                  T ~ new_baby_upi)) %>%
  select(-baby_upi) %>%
  rename(baby_upi = "new_baby_upi")


dups <- inconsistent_baby_upi_multiple %>%
  group_by(baby_upi, data_source) %>%
  filter(n()>1 & !is.na(baby_upi)) # 1 

dup_pregs <- dups %>% group_by(mother_upi, event_date) %>% summarise(n = n())

# Create live birth file with all the corrected baby upis we can 
births4 <- births2 %>%
  filter(correctable_baby_upi == F) %>%
  bind_rows(., inconsistent_baby_upi_singleton) %>%
  bind_rows(., inconsistent_baby_upi_multiple) %>%
  # select(-c(inconsistent_baby_upi, correctable_baby_upi, nrs_multiple, duplicate_upi)) %>%
  arrange(mother_upi, baby_upi)

#duplicate types
# - smr02 duplicates  - transfers we can remove them 
# - nrs duplicates for multiples are multiples who do not have a baby upi in 
# smr02 but a duplicate upi in nrs -- we cannot fix these 
# - nrs duplicates for singletons seem to be re-registrations 
births4_duplicate_babies <- births4 %>% 
  group_by(data_source, baby_upi) %>%
  filter(n() > 1 & !is.na(baby_upi))


nrs_only <- births4 %>%
  group_by(baby_upi) %>%
  filter(min_(data_source) == "nrs" & max_(data_source) == "nrs") 

nrs_only_births <- nrs_births %>%
  filter(nrs_event_id %in% nrs_only$event_id)

## Dealing with duplicates -----------------------------------------------------

# deal with duplicate baby upis in each datasource
# - smr02 duplicates remove 
# - nrs duplicates for multiples are multiples who do not have a baby upi in smr02 but a duplicate upi in nrs -- we cannot fix these 
# - nrs duplicates for singletons seem to be re-registrations 

#### Some of our baby UPIs appear more than once. This makes it impossible to accurately link these babies, so for linkage purposes delete their numbers.
births5 <- births4 %>%
  group_by(data_source, baby_upi, baby_order) %>%
  mutate(duplicate_smr02 = case_when(data_source == "smr02" & n() > 1 & !is.na(baby_upi) ~ T)) %>%
  group_by(data_source, baby_upi, preg_id) %>%
  mutate(duplicate_nrs_multiple = case_when(data_source == "nrs" & baby_type == "multiple" &  
                                              n() > 1 & !is.na(baby_upi) ~ T), 
         duplicate_nrs_singletons = case_when(data_source == "nrs" & baby_type == "singleton" & 
                                                n() > 1 & !is.na(baby_upi) ~ T), 
         duplicate_smr02_multiple = case_when(data_source == "smr02" & baby_type == "multiple" &  
                                                n() > 1 & !is.na(baby_upi) ~ T), 
         duplicate = case_when(duplicate_smr02 == T | duplicate_nrs_multiple == T | 
                                 duplicate_nrs_singletons == T | duplicate_smr02_multiple ~ T, 
                               T ~ F))


# smr02 duplicates appear to be transfers that we could not remove when cleaning smr02 due to missing upis 
# slice so we only have one record per baby
duplicates_smr02 <- births5 %>%
  filter(duplicate_smr02 == T & is.na(duplicate_smr02_multiple)) %>%
  group_by(mother_upi, baby_upi, baby_order) %>%
  slice(1)

# duplicate multiples are when nrs has given the same upi to multiples 
# we cannot use these upis so we create a dummy upi for linkage purposes 
duplicate_nrs_multiple <- births5 %>%
  filter(duplicate_nrs_multiple == T) %>% 
  ungroup() %>%
  mutate(baby_upi = paste0("57", str_pad(string = row_number(), width = 8, side = "left", pad = "0")))

# there are also smr02 multiples who have the same upi 
# we cannot use these for linkage purposes and so give dummy upis
duplicate_smr02_multiple <- births5 %>% 
  filter(duplicate_smr02_multiple == T) %>%
  ungroup() %>%
  mutate(baby_upi = paste0("58", str_pad(string = row_number(), width = 8, side = "left", pad = "0")))

# duplicate nrs records for singletons appear to be reregistrations 
# delete one of the records 
duplicate_nrs_singleton <- births5 %>% 
  filter(duplicate_nrs_singletons == T) %>%
  group_by(baby_upi) %>%
  slice(1)

births6 <- births5 %>% 
  filter(duplicate == F) %>%
  bind_rows(., duplicates_smr02) %>%
  bind_rows(., duplicate_nrs_multiple) %>%
  bind_rows(., duplicate_nrs_singleton) %>%
  bind_rows(., duplicate_smr02_multiple) %>%
  select(mother_upi, baby_upi, event_date, baby_order, 
         conception_date, event_id, data_source) %>%
  ungroup() %>% 
  mutate(baby_upi = case_when(is.na(baby_upi) ~ paste0("59", str_pad(string = row_number(), width = 8, side = "left", pad = "0")), 
                              T ~ baby_upi)) %>%
  group_by(mother_upi, event_date, baby_order) %>%
  mutate(baby_upi = case_when(str_starts(baby_upi, "5") ~ first_non_na(baby_upi), 
                              T ~ baby_upi))

duplicates <- births6 %>%
  group_by(baby_upi, data_source) %>% 
  filter(n() > 1)

na_only <- births6 %>% filter(is.na(mother_upi) & is.na(baby_upi)) 


log_print("finish dealing with duplicates")

## CREATE ONE ROW PER BABY -----------------------------------------------------
log_print("create one row per baby and resolve small nos one-to-many nrs-smr02 links")

##delete rows where 2 smr02 records for one baby
#(this happens where there is a multiple record for birth in smr02 but only one nrs record
#- probably due to chi seeding issues
#very small numbers affects (3 between 2000-2015)
#So drop the smr02 records to stop it affecting 
# the pivot but keeping the NRS records as we use them as the spine, 
births6 <- births6 %>% group_by(baby_upi, data_source) %>% mutate(count_source=n()) %>%
  ungroup() %>% filter(!(data_source=="smr02" & count_source>1 )) %>% select(-count_source)


##Identify rows where two NRS records have been confounded
# (specific situation where only one baby has a valid chi on both nrs and smr02 records
# AND  baby order is inverted)
# despite efforts to dedup etc, this has resulted in these pairs of babies having duplicated chi
# make these into dummy upis
dup_nrs <- births6 %>% group_by(baby_upi, data_source) %>%
  mutate(count_source=n()) %>%
  ungroup() %>% 
  filter(data_source=="nrs" & count_source >1) %>% 
  mutate(baby_upi = paste0("70",str_pad(string = row_number(),
                                        width = 8,
                                        side = "left",
                                        pad = "0"))) %>%
  select(-count_source)


#take out these events from main births file and add on the ones where upi has been changed
# this prevents duplicates going into the pivot 
# and ensures both NRS event IDs carry through to the next stage
births6  <- births6 %>% filter(!event_id %in% dup_nrs$event_id ) %>%
  rbind(dup_nrs)



##now group records for the same baby long to wide
births_grouped <- births6 %>%
  select(-baby_order) %>% 
  ungroup() %>%
  mutate(new_mother_upi = case_when(is.na(mother_upi) ~ paste0("49", str_pad(string = row_number(), width = 8, side = "left", pad = "0")),
                                    T ~ mother_upi)) %>%
  group_by(preg_id) %>%
  mutate(mother_upi = case_when(str_starts(new_mother_upi, "49") ~ first_non_na(new_mother_upi), 
                                T ~ new_mother_upi)) %>%
  ungroup() %>%
  group_by(baby_upi) %>% 
  mutate(mother_upi = first_non_na(mother_upi)) %>%
  select(-c(preg_id, new_mother_upi)) %>%
  ungroup() %>% 
  group_by(mother_upi, baby_upi) %>%
  mutate(conception_date = min(conception_date)) %>%
  mutate(event_date = min(event_date)) %>%
  group_by(baby_upi) %>%
  pivot_wider(names_from="data_source", values_from="event_id") %>%
  ungroup() %>%
  mutate(smr02 = as.character(smr02), 
         nrs = as.character(nrs))



## CHECK NUMBER OF BIRTHS FOR EACH BABY IS THE SAME ----------------------------

num_of_births_smr02 <- smr02_births %>% select(event_id, smr02_num_of_births_this_pregnancy)
num_of_births_nrs <- temp_nrs_births %>% select(event_id, n_babies)

# if the number of babies does not match, then we want to exclude that birth 
log_print("remove excess records that cannot be resolved from multiple births")
births_limited <- births_grouped %>%
  filter(!is.na(mother_upi)) %>%
  left_join(num_of_births_smr02, by = c("smr02" = "event_id")) %>%
  left_join(num_of_births_nrs, by = c("nrs" = "event_id")) %>%
  rowwise() %>%
  mutate(number_of_births = max_(na.omit(c(smr02_num_of_births_this_pregnancy, n_babies)))) %>%
  group_by(mother_upi, event_date) %>%
  mutate(number_of_births = max_(na.omit(number_of_births))) %>%
  mutate(observed_babies = n() ) %>%
  arrange(nrs, smr02) %>%
  mutate(excess_birth = case_when(row_number() > number_of_births ~ T, 
                                  T ~ F)) %>%
    group_by(mother_upi, event_date) %>%
  ungroup() 

excess_births <- births_limited %>%
  filter(excess_birth == T)

births_limited <- births_limited %>%
  select(-c(observed_babies, excess_birth, smr02_num_of_births_this_pregnancy, n_babies)) %>%
  mutate(nrs = case_when(nrs == "NULL" ~ NA_character_, 
                         T ~ nrs), 
         smr02 = case_when(smr02 == "NULL" ~ NA_character_, 
                           T ~ smr02))

orphans = births_limited %>%
  filter(is.na(nrs) & !(is.na(smr02)))

## CREATE FINAL LIVE BIRTH FILE ------------------------------------------------
log_print("create final file with all birth records")
births7 <- births_grouped %>%
  filter(is.na(mother_upi)) %>%
  # select(-baby_order) %>%
  bind_rows(births_limited) %>%
  arrange(mother_upi, baby_upi) %>%
  rename(db_births_number_of_births = number_of_births) %>%
  rename(smr02_live_births = smr02) %>%
  rowwise() %>% mutate(baby_id = UUIDgenerate()) %>% ungroup()

#orphans = births7 %>%
#  filter(is.na(nrs) & !(is.na(smr02_live_births)))

# fix issue where a mother has been linked to the wrong baby.
# This is a chi-seeding issue where the mother's upi has been linked to the wrong baby in nrs live births 
# This means that a women could have multiple births associated 
# with them within e.g. a couple of months (which is impossible) 
# To fix this, we will use the smr02 mother upi for these cases 
log_print("start final cleaning")
log_print("fix overlapping pregnancies")

nrs_births_upis <- temp_nrs_births %>% 
  select(mother_upi, baby_upi, event_date, event_id) %>%
  rename_with(.fn = function(.x){paste0(.x,"_nrs")})

smr02_births_upis <- temp_smr02_births %>%
  select(mother_upi, baby_upi, event_date, event_id) %>%
  rename_with(.fn = function(.x){paste0(.x,"_smr02")})

# we group births into events 
# it is impossible for a mother to have another live birth 112 (16 weeks) days after a live birth 
# if we group these events together, we can try to figure out whether the smr02 record will be right 
births7 <- births7 %>%
  moving_index_deduplication(., mother_upi, event_date, 112) %>%
  group_by(mother_upi, event) %>%
  mutate(wrong_mother_chi = case_when(n() > db_births_number_of_births ~ T, 
                                      T ~ F)) %>% ungroup()

multiple_births_same_mother_upi <- births7 %>%
  filter(wrong_mother_chi == T) %>%
  left_join(., nrs_births_upis, by = c("nrs" = "event_id_nrs")) %>%
  left_join(., smr02_births_upis, by = c("smr02_live_births" = "event_id_smr02")) %>%
  rowwise() %>%
  mutate(mother_upi = case_when(mother_upi != mother_upi_smr02 ~ mother_upi_smr02, 
                                T ~ mother_upi)) %>%
  group_by(mother_upi, event) %>%
  # there are still cases where there are mothers linked to multiple babies 
  # in these situations, we need to create a dummy upi for mothers we think are bad links 
  # if there is only one record, assume that to be the wrong one and create a dummy upi for these ones 
  mutate(wrong_mother_chi = case_when(n() > db_births_number_of_births ~ T)) %>%
  ungroup() %>%
  mutate(mother_upi = case_when(wrong_mother_chi == T & 
                                  (is.na(nrs) | is.na(smr02_live_births)) ~ 
                                  paste0("60",str_pad(string = row_number(),
                                                      width = 8, side = "left",
                                                      pad = "0")), 
                                T ~ mother_upi)) 

births7 <- births7 %>%
  filter(wrong_mother_chi == F) %>%
  bind_rows(multiple_births_same_mother_upi) 
log_print("finish fixing overlapping pregnancies - save final file")

write_rds(births7, paste0(folder_temp_data, "slipbd_births_updates.rds"), compress = "gz")


