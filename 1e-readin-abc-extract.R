#####################################
##Extract antenatal booking records
#Loads in file saved by maternity team
#Cleans (implausible dates etc) and dudups on 83 days
#saves file for later combination into the linked pregnancy file
####################################

#  find and import the latest file based on the date at end of filename.
filenames <- list.files(folder_mat_data_path)
filename_latest <- latest_file(filenames = filenames)

ABCextract <- readRDS(paste0(folder_mat_data_path,filename_latest ))

##########################################################################################
#clean up dates

ABCextract <- ABCextract  %>% 
  mutate(mother_upi = case_when(phsmethods::chi_check(upi)!="Valid CHI" ~chi, 
                                T~upi))%>% ##use chi for upi where upi not available 
  mutate(validity = chi_check(mother_upi)) %>% 
  mutate(mother_upi = case_when(validity == "Valid CHI" ~ mother_upi,
                         T ~ NA_character_)) %>% 
  ##dummy UPI where neither chi nor upi is avlible (or is not valid)
  mutate(mother_upi = case_when(is.na(mother_upi) ~ paste0("44", str_pad(  
      string = row_number(),
      width = 8,
      side = "left",
      pad = "0"
    )),
    T ~ str_pad( 
      string = mother_upi,
      width = 10,
      side = "left",
      pad = "0"
    )))
  
#sort gestation - feasible only and use lmp if gest is not given or not feasible
#calculate est conception date
ABCextract <- ABCextract  %>% 
  mutate(feasible_gestation = case_when(gestation_at_booking %in% feasible_gestation_booking ~ T,
                                        T ~ F)) %>% 
  mutate(gestation_at_booking = case_when(feasible_gestation == T ~ gestation_at_booking,
                                         feasible_gestation == F & !is.na(lmp)
                                         ~ as.numeric(floor(difftime(booking_date, lmp, units = "weeks"))),
                                         T ~ NA_real_)) %>% 
  mutate(feasible_gestation = case_when(gestation_at_booking %in% feasible_gestation_booking ~ T,
                                        T ~ F)) %>% 
  mutate(assumed_gestation = case_when(feasible_gestation == F ~ T,
                                       T ~ F)) %>% 
  mutate(gestation_at_booking = case_when(feasible_gestation == T ~ gestation_at_booking,
                                         feasible_gestation == F ~ assumed_gestation_booking
  )) %>%
  mutate(estimated_conception_date = booking_date - (weeks(gestation_at_booking) - weeks(2) )) %>%
  mutate(event_type = "booking") %>%
  select(booking_date, everything()) 


##deduplicate - the file has been cleaned by the mat team, which should result in most duplicates being removed
#however some duplicate bookings may still slip through - this step ensures ABC is cleaned and deduped in line with
# other data sources.
# Women may book multiple times if they move healthboard resultign in duplicate bookings

#remove any exact dups (usually none due to maternity team cleaning  process)
ABCextract <-ABCextract %>%
  distinct()

#find events within 83 days
ABCextract <- 
  moving_index_deduplication(ABCextract, mother_upi, booking_date, dedupe_period)

#take min date for grouped events
ABCextract  <- ABCextract %>%
  group_by(mother_upi, event) %>%
  mutate(revised_conception_date = min(estimated_conception_date))

##regroup events based on the minimum date of events (above)
ABCextract  <- 
  moving_index_deduplication(ABCextract  , mother_upi, revised_conception_date, dedupe_period)

##apply first non-missing info for hb, simd, lmp etc to all rows that refer to same event
## then take first row in group
ABCextract <- ABCextract %>%
  group_by(mother_upi, event) %>%
  arrange(assumed_gestation, booking_date) %>% 
  mutate(mothers_dob= first_(mothers_dob), 
         hbt= first_(hbt),
         hbt2019code= first_(hbt2019code),
         lmp= first_(lmp),
         smoking_status= first_(smoking_status), 
         smoking_desc = first_(smoking_desc), 
         pc7 = first_(pc7)
         ) %>% 
  slice(1) %>%
  ungroup() %>%
  select(mother_upi, everything()) %>%
  select(-c(upi, chi, event, revised_conception_date, feasible_gestation, -ethnicity)) %>%
  mutate(anbooking = T) %>%
  rowwise() %>% mutate(event_id = UUIDgenerate()) %>% ungroup() %>%
  rename_with( ~ paste0("anbooking_", .)) 


saveRDS(ABCextract , paste0(folder_temp_data, "antenatal_booking.rds"))

rm(ABCextract)
gc()
###############some qa##############
ABCextract<- readRDS( paste0(folder_temp_data, "antenatal_booking.rds"))
df <- ABCextract
ds_name <- "ABC"

n_male <- df %>% filter(substr(anbooking_mother_upi,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_sex_from_chi = phsmethods::sex_from_chi(anbooking_mother_upi)) %>%
  filter(mother_sex_from_chi==1) %>% count()
n_male$dataset <- ds_name
n_male$value <- "chi_implies_male"

n_missing_invalid_upi <- df %>%
  mutate(validity = chi_check(anbooking_mother_upi)) %>% filter(validity !="Valid CHI") %>% count()
n_missing_invalid_upi$dataset <- ds_name
n_missing_invalid_upi$value <- "mother_chi_missing_invalid"


df <- df %>%
  mutate(validity = chi_check(anbooking_mother_upi)) %>% 
  filter(substr(anbooking_mother_upi,1,1) !="4") %>% # remove dummy upi 1st
  mutate(mother_dob_fr_chi = phsmethods::dob_from_chi(anbooking_mother_upi,
                                                      min_date= as.Date("1930-01-01"), max_date=Sys.Date())) %>%
  mutate(mother_age = floor(decimal_date(anbooking_booking_date) - decimal_date(mother_dob_fr_chi ))) 

table(df$mother_age, useNA="always")

n_bad_dob_fr_chi <-  df %>% mutate(high_low_age = ifelse(mother_age %in% feasible_age, 0,1)) %>% 
  filter(high_low_age==1 & !is.na(mother_age)) %>% count()

n_bad_dob_fr_chi$dataset <- ds_name
n_bad_dob_fr_chi$value <- "chi_implies_age_outside_range"

df <- rbind(n_bad_dob_fr_chi, n_male, n_missing_invalid_upi ) %>% select(dataset, value, n)
saveRDS(df,paste0(folder_temp_data,"checks/abc_checks.rds") )

