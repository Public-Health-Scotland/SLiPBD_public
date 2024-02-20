###Number of infant deaths check
library(dplyr)
library(odbc)


count_deaths <- infant_deaths %>% group_by(year(date_of_baby_death)) %>% 
  count() %>% ungroup()
count_deaths

count_deaths_cohort <- slipbd_database %>% filter(infant_death==1) %>%
  group_by(year(date_infant_death)) %>% count() %>% ungroup()

#count_deaths
#count_deaths_cohort

names(count_deaths) <- c("year", "n_NRS_deaths")
names(count_deaths_cohort) <- c("year", "n_deaths_slipbd")


compare_counts <- left_join(count_deaths, count_deaths_cohort)
compare_counts <- compare_counts %>% mutate(difference_deaths = n_NRS_deaths - n_deaths_slipbd)

#View(compare_counts)
#NB do not expec to match counts in the first year as some deaths will be of infants born the previous year
# also do not expect the last year of data to match due to differences in extract dates or delay in proper record linkage


##Which deaths are not in the cohort.

in_cohort <- infant_deaths %>% filter(deaths_triplicate_id %in% slipbd_database$infant_deaths_triplicate_id)
not_in_cohort <- infant_deaths %>% filter(!(deaths_triplicate_id %in% slipbd_database$infant_deaths_triplicate_id))

not_in_cohort <- not_in_cohort %>% 
  filter(year_of_registration != 1999 | is.na(year_of_registration))

sum(is.na(not_in_cohort$nrs_triplicate_id))

test <- not_in_cohort %>% 
  group_by(nrs_triplicate_id) %>% 
  summarise(n = n())
