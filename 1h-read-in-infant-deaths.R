############################
## read in infant deaths#########
############################

#(cpoying from cops - needs checked yet)
data_infant_deaths <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(
      infant_deaths_sql,
     "WHERE GRO.DATE_OF_DEATH >= TO_DATE('", cohort_start_date,"', 'yyyy-mm-dd')")
  )
) %>%
  clean_names() %>%
  unique() %>%
  mutate(deaths_triplicate_id = paste0("D", substr(year_of_registration_death,3,4),  registration_district_death,
                                       str_pad(entry_number_death, width = 4, pad="0"))) %>%
  mutate(nrs_triplicate_id = paste0("B",substr(year_of_registration,3,4), registration_district, 
                                    str_pad(entry_number, width = 4, pad="0"))) %>%
  mutate(chi = chi_pad(chi)) %>% 
  mutate(validity = chi_check(chi)) %>% 
  mutate(chi = case_when(validity == "Valid CHI" ~ chi,
                         T ~ NA_character_)) %>% 
  select(-validity)

##a couple of duplicate ids, these coded to NA
# if the alternate ID is unique it should still link.
data_infant_deaths <- data_infant_deaths %>% group_by(chi) %>% mutate(chi_count = n()) %>% 
  ungroup() %>% mutate(chi = case_when(chi_count >1 ~NA , T~chi))
data_infant_deaths <- data_infant_deaths %>% group_by(nrs_triplicate_id) %>% mutate(nrs_id_count = n()) %>% 
  ungroup() %>% mutate(nrs_triplicate_id = case_when(nrs_id_count >1 ~NA , T~nrs_triplicate_id))

data_infant_deaths <- data_infant_deaths %>% select(-nrs_id_count, -chi_count)

saveRDS(data_infant_deaths, paste0(folder_temp_data, "infant_deaths.rds"))
rm(data_infant_deaths)
gc()

