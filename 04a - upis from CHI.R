# READ IN CHI DATABASE FOR ALL UPIS --------------------------------------------

mothers <- read_rds(paste0(folder_temp_data, "script3_pregnancy_record.rds")) %>% 
  select(mother_upi) %>% 
  mutate(patient = "mother") %>%
  rename(upi = mother_upi)


# link mothers to CHI DATABASE -------------------------------------------------
mothers <- mothers %>% 
  distinct() %>%
  filter(chi_check(upi) == "Valid CHI")

clear_temp_tables(SMRAConnection)

upis <- SMRAConnection %>% tbl(dbplyr::in_schema(chi_schema,chi_table)) %>% 
  inner_join(mothers %>% rename(CHI_NUMBER = upi), copy = TRUE) %>%
  filter(is.na(DELETION_INDICATOR)) %>% # remove any that have been marked as deleted
  select(CHI_NUMBER, DATE_OF_BIRTH, CURRENT_POSTCODE, PREVIOUS_POSTCODE, DATE_ADDRESS_CHANGED) %>%
  distinct() %>% 
  collect() %>% 
  rename(upi = CHI_NUMBER,
         chi_dob = DATE_OF_BIRTH, 
         chi_current_postcode = CURRENT_POSTCODE, 
         chi_previous_postcode = PREVIOUS_POSTCODE, 
         chi_date_postcode_change = DATE_ADDRESS_CHANGED) %>% 
  mutate(chi_dob = as_date(chi_dob))

upi_dobs <- upis %>% select(upi, chi_dob) %>% distinct() %>% group_by(upi) %>% filter(n() == 1) %>% ungroup()
upi_postcodes <- upis %>% select(upi, chi_current_postcode, chi_date_postcode_change) %>% distinct() %>% group_by(upi) %>% filter(n() == 1) %>% ungroup()
upi_prev_postcodes <- upis %>% select(upi, chi_previous_postcode) %>% distinct() %>% group_by(upi) %>% filter(n() == 1) %>% ungroup()

mothers %<>% 
  left_join(., upi_dobs, by = "upi") %>%
  left_join(., upi_postcodes, by = "upi") %>%
  left_join(., upi_prev_postcodes, by = "upi")

saveRDS(mothers, paste0(folder_temp_data, "mother_upis_dob_from_CHI.rds"))


