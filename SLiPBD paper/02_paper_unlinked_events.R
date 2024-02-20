# -------------------------------------------------------------------------
# SLiPDB
# Unlinked events
# Denise Jennings, October 2023
# -------------------------------------------------------------------------

# 1. Find unlinked events from SMR01 --------------------------------------

# Select only required variables from data
all_variables <- all_variables %>%
  select(mother_upi:births_smr02_live_births, anbooking_1, births_nrs)

# Find event ids that are in SMR01 but not the final variables dataset
smr01_unlinked <- smr01 %>%
  filter(event_id %notin% all_variables$SMR01_1) 



# 2. Find unlinked events from ToPS --------------------------------------

# Find event ids that are in SMR01 but not the final variables datasets
tops_unlinked <- tops_aas %>% 
  filter(aas_event_id %notin% all_variables$ToPs_1)


# 3. Find unlinked events from Antenatal Booking --------------------------

# Find event ids that are in SMR01 but not the final variables dataset
anbooking_unlinked <- antenatal_booking %>% 
  filter(anbooking_event_id %notin% all_variables$anbooking_1)



# 4. Find unlinked events from SMR02 Births --------------------------

# Filter final variables data for live and stillbirths
all_variables_births <- all_variables %>%
  filter(fetus_outcome1 == "Live birth" | fetus_outcome1 == "Stillbirth" |fetus_outcome2 == "Live Birth" | fetus_outcome2 == "Stillbirth") 

# Filter SMR02 for live and stillbirths
smr02_births <- smr02_data %>%
  filter(smr02_outcome_type == "Live birth" | smr02_outcome_type == "Stillbirth")

# Find event ids that are in SMR02 but not the final variables dataset         
smr02_births_unlinked <- smr02_births %>%
  filter(event_id %notin% all_variables_births$births_smr02_live_births)



# 5. Find unlinked events from SMR02 nonlive --------------------------

# Filter final variables data to exclude live and stillbirths
all_variables_other_outcomes <- all_variables %>%
  filter(fetus_outcome1 != "Live birth" & fetus_outcome1 != "Stillbirth" |fetus_outcome2 != "Live Birth" | fetus_outcome2 != "Stillbirth") 

# Find event ids that are in SMR01 but not the final variables dataset 
smr02_other_unlinked <- smr02_nonlive %>%
  filter(event_id %notin% all_variables_other_outcomes$SMR02_1)




# 6. Find unlinked events from NRS Live births --------------------------
nrslb_unlinked <- nrs_live_births %>%
  filter(event_id %notin% all_variables$births_nrs) 


# 7. Find unlinked events from NRS Stillbirths --------------------------

nrssb_unlinked <- nrs_sb %>%
  filter(event_id %notin% all_variables$births_nrs) 

#rm(all_variables_other_outcomes, all_variables_births, all_variables)


# 8. Create a count of the dropped records by year for each data source

# First create count function
unlinked_count <- function(data, mutate, date, outcome){
  data %>% 
    #filter(!!sym(paste0(filter)) >= cutoff_date) %>%
    mutate(!!sym(paste0(mutate)) := year(!!sym(paste0(date)))) %>% 
    group_by(!!sym(paste0(mutate)), !!sym(paste0(outcome))) %>% 
    summarise(total = n()) %>% 
    ungroup() %>% 
    rename(outcome = contains("outcome"))
}

smr01_unlinked_count <- unlinked_count(smr01_unlinked, "year", "smr01_cis_admission_date", "smr01_outcome_type")
tops_unlinked_count <- unlinked_count(tops_unlinked, "year", "aas_date_of_termination", "aas_outcome_type")
smr02_births_unlinked_count <- unlinked_count(smr02_births_unlinked, "year", "smr02_date_of_delivery", "smr02_outcome_type") 
smr02_other_unlinked_count <- unlinked_count(smr02_other_unlinked, "year", "smr02_admission_date", "smr02_outcome_type")
nrslb_unlinked_count <- unlinked_count(nrslb_unlinked, "year", "nrslb_date_of_birth", "nrslb_outcome_type")
nrssb_unlinked_count <- unlinked_count(nrssb_unlinked, "year", "nrssb_date_of_birth", "nrssb_outcome_type")
  
  
# Combine counts from all data sources
total_not_in_slipbd <- full_join(smr01_unlinked_count, tops_unlinked_count)
total_not_in_slipbd <- full_join(total_not_in_slipbd, smr02_births_unlinked_count)
total_not_in_slipbd <- full_join(total_not_in_slipbd, smr02_other_unlinked_count)
total_not_in_slipbd <- full_join(total_not_in_slipbd, nrslb_unlinked_count)
total_not_in_slipbd <- full_join(total_not_in_slipbd, nrssb_unlinked_count)

# Group joined data by year and outcome for total dropped records
not_in_slipbd_year <- total_not_in_slipbd %>% 
  group_by(year, outcome) %>% 
  summarise(total = sum(total)) %>% 
  ungroup()

rm(smr01_unlinked_count, tops_unlinked_count, smr02_births_unlinked_count, smr02_other_unlinked_count, nrslb_unlinked_count, nrssb_unlinked_count, total_not_in_slipbd)

# End of script-------------------------------------------------------------------------







