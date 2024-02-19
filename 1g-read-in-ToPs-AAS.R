############################
## read in ToPs###########
############################

##STILL TO DO - FINALISE WHICH HOSP /LOCAITON VARIABLES

#source("SLiPBD process/00-setup.R")

# View variable names
#names1 <- colnames(dplyr::tbl(aas_connect, dbplyr::in_schema("AAS", "ANALYST_DATA")))

##extract
tops <- tibble::as_tibble(dbGetQuery(aas_connect,
                                     statement=paste0(aas_sql_vars ,
                                                      "WHERE DATE_OF_TERMINATION >= TO_DATE('", cohort_start_date, "', 'yyyy-mm-dd')"))) %>%
  clean_names()

#table(tops$statutory_ground_a, useNA="always")
#table(tops$age, useNA="always")
#table(year(tops$date_of_termination))
#table((tops$second_hospital)== (tops$hospital))
#table((tops$type_of_premises_a))
##clean file
##use admission date if term date not there
##flag implausible ages
##create flag for selective reduction

tops <- tops %>% 
  rename("mother_upi_number" = "derived_upi") %>%
  mutate(mother_upi_number = chi_pad(mother_upi_number)) %>% 
  mutate(validity = chi_check(mother_upi_number)) %>% 
  mutate(mother_upi_number = case_when(validity == "Valid CHI" ~ mother_upi_number,
                                       T ~ NA_character_)) %>%
  select(-validity) %>% 
  mutate(mother_upi_number = case_when(
    is.na(mother_upi_number) ~ paste0("45", str_pad(
      string = row_number(),
      width = 8,
      side = "left",
      pad = "0"
    )),
    T ~ mother_upi_number
  )) %>%
  mutate(termination = T) %>% 
  mutate(outcome_type = "Termination") %>%
  mutate(estimated_gestation = if_else(estimated_gestation %in% feasible_gestation_termination, 
                                          estimated_gestation, NA_real_)) %>% 
  mutate(assumed_gestation = if_else(is.na(estimated_gestation), 1, 0)) %>% 
  mutate(estimated_gestation = case_when(is.na(estimated_gestation) ~ assumed_gestation_aas_termination,
                                             T ~ estimated_gestation)) %>%
  mutate(estimated_date_of_conception = date_of_termination - (weeks(estimated_gestation) - weeks(2) )) %>%
  arrange(mother_upi_number, estimated_date_of_conception) %>%
  #record selective reduction as it is codes as 1/2 which is non-intuitive.
  mutate(selective_reduction = case_when(selective_reduction == 1 ~T , TRUE ~ F)) %>%
  #flag home location
  mutate(one_at_home = ifelse(type_of_premises_a ==6|type_of_premises_a ==8   | type_of_premises_p==8,1,0),
         both_at_home = ifelse((type_of_premises_a ==6|type_of_premises_a ==8 )  & type_of_premises_p==8,1,0))

##sort out the total pregnancies/parity variable
#it is derived from total pregnancies - theraputic abortions- spontaneous abortions
#sometimes it is <0 if total pregnacies=0 but there are abortion outcomes
#code these to 0 (we are assuming that the count of abortions is correct but that the toal must be wrong here)
table(tops$parity)
tops <- tops %>%
  mutate(parity_cleaned = ifelse(parity < 0, 0, parity)) %>%
 mutate(parity_cleaned = as.numeric(parity_cleaned))


##dedupicate
#remove any exact dups
tops <-tops %>%
  distinct() %>% arrange(mother_upi_number, date_of_termination)

#deduplicate on 83 days (grouping into events)
tops <- 
  moving_index_deduplication(tops, mother_upi_number, date_of_termination, dedupe_period)


#### Assign each termination to an Event, based on the revised conception date ####
tops %<>%
  group_by(mother_upi_number, event) %>%
  mutate(revised_conception_date = min(estimated_date_of_conception)) %>%
  ungroup()

tops  <- 
  moving_index_deduplication(tops , mother_upi_number, revised_conception_date, dedupe_period)


#### Set variable values based on each termination's Event counter ####
tops %<>%
  ungroup() %>%
  group_by(mother_upi_number, event) %>%
  mutate(date_of_termination = min(date_of_termination),
    date_of_birth = first_(date_of_birth), 
    postcode = first_(postcode),
    parity  = first_(parity),
    estimated_gestation = first_(estimated_gestation),
    selective_reduction = min_(selective_reduction),
    original_number_of_foetuses = max_(original_number_of_foetuses),
    reduced_to = max_(reduced_to), 
    spontaneous_abortions = first_(spontaneous_abortions),
    therapeutic_abortions = first_(therapeutic_abortions),
    total_number_of_pregnancies = first_(total_number_of_pregnancies),
    hospital  = first_(hospital) ,
    second_hospital = first(second_hospital) ,
    hb_treatment_cypher = first_(hb_treatment_cypher),
    parity_cleaned = first_(parity_cleaned),
    type_of_premises_p = first_(type_of_premises_p) ,
    type_of_premises_a = first_(type_of_premises_a) ,
    gest_based_on = first_(gest_based_on), 
    statutory_ground_a = max_(statutory_ground_a),
    statutory_ground_b = max_(statutory_ground_b),
    statutory_ground_c = max_(statutory_ground_c),
    statutory_ground_d = max_(statutory_ground_d),
    statutory_ground_e = max_(statutory_ground_e),
    statutory_ground_f = max_(statutory_ground_f),
    statutory_ground_g = max_(statutory_ground_g),
    assumed_gestation = first_(assumed_gestation),
    one_at_home = first(one_at_home),
    revised_conception_date = first_(revised_conception_date) )%>%
  ungroup() 

n_rows_before_slicing_to_event_level <- nrow(tops)


#### Cut terminations down to one row per Event ####
tops %<>%
  group_by(mother_upi_number, event) %>%
  arrange(assumed_gestation, date_of_termination) %>% 
  slice(1) %>%
  ungroup() %>%
  select(mother_upi_number,
         estimated_date_of_conception,
         outcome_type,
         everything()) %>%
  select(-event) %>% 
  rowwise() %>% mutate(event_id = UUIDgenerate()) %>% ungroup()%>% 
  rename_with( ~ paste0("aas_", .)) 


saveRDS(tops, paste0(folder_temp_data, "tops_aas.rds"))

####QA

tops <- readRDS(paste0(folder_temp_data, "tops_aas.rds"))
df <- tops 
ds_name <- "TOPS"

n_male <- df %>% filter(substr(aas_mother_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_sex_from_chi = phsmethods::sex_from_chi(aas_mother_upi_number)) %>%
  filter(mother_sex_from_chi==1) %>% count()
n_male$dataset <- ds_name
n_male$value <- "chi_implies_male"

n_missing_invalid_upi <- df %>%
  mutate(validity = chi_check(aas_mother_upi_number)) %>% filter(validity !="Valid CHI") %>% count()
n_missing_invalid_upi$dataset <- ds_name
n_missing_invalid_upi$value <- "mother_chi_missing_invalid"


df <- df %>%
  mutate(validity = chi_check(aas_mother_upi_number)) %>% 
  filter(substr(aas_mother_upi_number,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_dob_fr_chi = phsmethods::dob_from_chi(aas_mother_upi_number,
                                                      min_date= as.Date("1930-01-01"), max_date=Sys.Date())) %>%
  mutate(mother_age = floor(decimal_date(aas_date_of_termination) - decimal_date(mother_dob_fr_chi ))) 

table(df$mother_age, useNA="always")

n_bad_dob_fr_chi <-  df %>% mutate(high_low_age = ifelse(mother_age %in% feasible_age, 0,1)) %>% 
  filter(high_low_age==1 & !is.na(mother_age)) %>% count()

n_bad_dob_fr_chi$dataset <- ds_name
n_bad_dob_fr_chi$value <- "chi_implies_age_outside_range"

df <- rbind(n_bad_dob_fr_chi, n_male, n_missing_invalid_upi ) %>% select(dataset, value, n)
saveRDS(df,paste0(folder_temp_data,"checks/tops_checks.rds") )

rm(tops, df)
gc()
