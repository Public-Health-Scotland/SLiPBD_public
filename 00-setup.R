
############################
######### SETUP ------------
############################

# renv::install("remotes")
# 
# remotes::install_version("dplyr", "1.0.10")
# remotes::install_version("tidyr", "1.2.1")
# remotes::install_version("tidyverse", "1.3.2")
# remotes::install_version("naniar", "1.0.0")
# remotes::install_version("magrittr", "2.0.3")
# 
# renv::snapshot()
# renv::status()
source("SLiPBD process/0-data-paths.R")
### Load packages ----
library(logr)
library(renv)
library(here)
library(odbc)
library(tidyverse)
library(magrittr)
library(lubridate)
library(readr)
library(dplyr)
library(tidyr)
library(janitor)
library(stringr)
library(naniar)
library(hablar)
library(uuid)
library(readxl)
library(kableExtra)
library(ggplot2)
library(scales)
library(flextable)
library(officer)
library(magrittr)
library(mschart)
library(openxlsx)
library(haven)
library(tictoc)
library(phsmethods)
library(keyring)

######### fixed values --------
# dates

cutoff_date <-  Sys.Date() - years(3) #normal cutoff date when merging the old and new data will be -3years

#cohort start date determines extract dates for datasets
# needs to go further back than cohort to get all dates associated with eg an infant death 3 yrs ago
#cohort_start_date <-  cutoff_date %m-% months(10) - lubridate::years(1)  # 
cohort_end_date <- Sys.Date()

cohort_start_date <- as.Date("2000-01-01") # historic re-runs only
# feasible values
dedupe_period <- 83 # 83 days. This variable is used to deduplicate outcomes within datasets

assumed_gestation_booking            <- 10
assumed_gestation_ectopic            <- 8
assumed_gestation_miscarriage        <- 10
assumed_gestation_smr02_termination  <- 16
assumed_gestation_aas_termination    <- 8
assumed_gestation_stillbirth         <- 32
assumed_gestation_live_birth_single       <- 40
assumed_gestation_live_birth_multiple     <- 37

feasible_gestation_lb <- 16:44
feasible_gestation_sb <- 24:44
feasible_gestation_miscarriage <- 4:23
feasible_gestation_termination <- 4:44
feasible_gestation_booking <- 4:44

feasible_age <- 11:55
feasible_bmi <- 10:100

icd10_miscarriage                             <- as_vector(read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Miscarriage ICD10"))
icd10_molar                                   <- as_vector(read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Molar ICD10"))
icd10_ectopic                                 <- as_vector(read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Ectopic ICD10"))


read_miscarriage                              <- read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Miscarriage Read") %>% clean_names()
read_molar                                    <- read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Molar Read") %>% clean_names()
read_ectopic                                  <- read_excel(paste0(folder_documentation, "Early spontaneous loss codes.xlsx"), "Ectopic Read") %>% clean_names()


######### lookups --------

SPD_latest <- max_(list.files(paste0(lookups_path , "Unicode/Geography/Scottish Postcode Directory"),
                              pattern = "\\.rds$", ignore.case=TRUE))
all_simd <- list.files(paste0(lookups_path ,"Unicode/Deprivation/"), pattern = "^postcode.*all_simd_carstairs.rds$")

SPD <- readRDS(paste0(paste0(lookups_path ,"Unicode/Geography/Scottish Postcode Directory/"),SPD_latest))

SIMD <- readRDS(paste0(lookups_path ,"Unicode/Deprivation/", max(all_simd)))
location <- read_csv(paste0(lookups_path,"Unicode/National Reference Files/location.csv" ))


##HB and council area codes to names
la_names <- read_csv(paste0(folder_data_path, "lookups/Council_area_names.csv")) %>%
  select( CouncilAreaCode,	CouncilAreaName)
hb_names <- read_csv(paste0(folder_data_path, "lookups/Health Board Area all years Lookup.csv")) %>%
  select( HealthboardCode ,NRSHealthBoardAreaName)
hb_names <- hb_names %>% mutate(NRSHealthBoardAreaName=  str_replace(NRSHealthBoardAreaName , "&", "and"))

######### FUNCTIONS ---------
latest_file <- function(filenames) {
  nums <- filenames %>% str_match_all("[0-9]+") %>% unlist
  dates <- as.Date(nums, "%d%m%Y")
  filenames[which(dates==max_(dates))]
}

`%notin%` <- Negate(`%in%`)

moving_index_deduplication <- function(dataframe, identifier, date, deduplication_period) {

  tic()

  dataframe <- dataframe %>%
    arrange({{ identifier }}, {{ date }}) %>%
    mutate(event = 1)

  print(paste0(nrow(dataframe), " rows in dataframe"))

  # If a UPI number only has one observation then we can just assign that single observation to group 1 without putting it through the loop
  dataframe_single_observations <- dataframe %>%
    group_by({{ identifier }}) %>%
    filter(n() == 1) %>%
    ungroup()

  print(paste0(nrow(dataframe_single_observations), " single observations in dataframe"))

  # If a UPI number only has two observations then we don't need to put it through the loop - we can just compare the dates of the two observations
  # and quickly assign them to the correct groups.
  dataframe_double_observations <- dataframe %>%
    group_by({{ identifier }}) %>%
    filter(n() == 2) %>%
    mutate(event = case_when(row_number() == 2 & difftime({{ date }}, lag({{ date }}), units = "days") > deduplication_period ~ 2, T ~ 1)) %>%
    ungroup()

  print(paste0(nrow(dataframe_double_observations), " double observations in dataframe"))

  dataframe_completed_groupings <-
    dataframe_single_observations %>%
    bind_rows(dataframe_double_observations)

  dataframe %<>% group_by({{ identifier }}) %>%
    filter(n() > 2) %>%
    ungroup()

  loop <- 1

  repeat {

    tic()
    print(loop)
    loop <- loop + 1

    print(paste0(nrow(dataframe), " rows in loop"))

    # This code groups records into Events.
    print("Determining index date")
    # dataframe %<>% mutate(index_date = first({{ date }})) # The first observed admission date for a woman becomes our initial index date, and then changes on every iteration to the first admission date which occurs >83 days after the previous index date.
    dataframe <- dataframe %>%
      dtplyr::lazy_dt() %>%
      group_by({{ identifier }}, event) %>%
      mutate(index_date = first({{ date }})) %>%
      collect() %>% ungroup()

    print("Calculating time differences")
    dataframe %<>% mutate(days_since_index_event = difftime({{ date }}, index_date, units = "days"))
    
    print("Setting event counter")
    dataframe %<>% mutate(event = case_when(days_since_index_event > deduplication_period ~ event + 1, T ~ event))

    print("Determining completed groupings")
    # dataframe %<>% group_by({{ identifier }})
    # dataframe %<>% mutate(max_days = max(days_since_index_event)) %>% ungroup()
    dataframe <- dataframe %>%
      dtplyr::lazy_dt() %>%
      group_by({{ identifier }}) %>%
      mutate(max_days = max(days_since_index_event)) %>%
      collect() %>%
      ungroup()

    dataframe %<>% mutate(grouping_complete = case_when(max_days <= deduplication_period ~ 1, T ~ 0))
    temp_complete_grouping <- dataframe %>% filter(grouping_complete == T) %>% select(-grouping_complete)

    print("Removing completed groupings from loop")
    print(paste0(nrow(temp_complete_grouping), " rows removed from next loop"))
    dataframe_completed_groupings %<>% bind_rows(temp_complete_grouping)
    dataframe %<>% filter(grouping_complete == F) %>% select(-grouping_complete)

    rm(temp_complete_grouping)

    toc()

    if (nrow(dataframe) == 0) {
      print("Rows succesfully grouped into events")
      break # If no records take place more than 83 days after that person's latest index event, then we've successfully allocated every row to its proper event group.
    }
    print("Running another loop...")
  }

  dataframe_completed_groupings %<>%
    select(-c(index_date, days_since_index_event, max_days)) %>%
    arrange({{ identifier }}, {{ date }})

  toc()
  return(dataframe_completed_groupings)

}


clear_temp_tables <- function(conn){
  # clears old dbplyr tables off the SQL database where possible
  tables <- odbc::dbListTables(conn)
  for(table in tables[str_sub(tables, 1, 7) == "dbplyr_"]){try(odbc::dbRemoveTable(conn, table), silent = TRUE)}
}


reporting_ethnicity <- function(ethnicity_code) {case_when(str_starts(ethnicity_code, "1") ~ "1 White", 
                                                           str_starts(ethnicity_code, "Group A - White") ~ "1 White", 
                                                           str_detect( toupper(ethnicity_code), "WHITE") ~ "1 White",
                                                           str_detect( toupper(ethnicity_code), "EUROPE") ~ "1 White",
                                                           substr(toupper(ethnicity_code),1,2) %in% c("3F", "3G", "3H") ~ "2 South Asian",
                                                           str_detect( toupper(ethnicity_code), "BANGLADESHI") |
                                                             str_detect( toupper(ethnicity_code), "INDIAN") |
                                                             str_detect( toupper(ethnicity_code), "PAKISTANI") ~ "2 South Asian",
                                                           substr(toupper(ethnicity_code),1,2) %in% c("5D", "5C", "5Y", "4D", "4Y","4X",  "5X") ~ "3 Black/Caribbean/African" ,
                                                           str_detect( toupper(ethnicity_code), "BLACK") ~ "3 Black/Caribbean/African" ,
                                                           str_detect( toupper(ethnicity_code), "AFRICAN") ~ "3 Black/Caribbean/African" ,
                                                           substr(toupper(ethnicity_code),1,2) %in% c("3J", "3X", "2A", "6A", "3Z", "6Z", "OT") ~ "4 Other or mixed ethnicity",
                                                           str_detect( toupper(ethnicity_code), "AUSTRALASIA") |   str_detect( toupper(ethnicity_code), "ARAB") |
                                                             str_detect( toupper(ethnicity_code), "CHINESE") |str_detect( toupper(ethnicity_code), "FAR EAST ASIA") |
                                                             str_detect( toupper(ethnicity_code), "SOUTH EAST ASIA") |
                                                             str_detect( toupper(ethnicity_code), "MIXED") |str_detect( toupper(ethnicity_code), "OTHER") ~ "4 Other or mixed ethnicity",
                                                           is.na(ethnicity_code) ~ "5 Unknown/missing", 
                                                           substr(toupper(ethnicity_code),1,2) %in% c("99", "98") ~ "5 Unknown/missing", 
                                                           toupper(ethnicity_code)=="ERROR" | toupper(ethnicity_code)=="UNKNOWN" |
                                                             toupper(ethnicity_code)=="NOT KNOWN" | str_detect( toupper(ethnicity_code), "REFUSED") ~ "5 Unknown/missing")} 



                                                                
council_area_codes <- function(council_code) {
  case_when(council_code=="S12000005"~ "Clackmannanshire",
            council_code=="S12000006"~ "Dumfries and Galloway",
            council_code=="S12000008"~ "East Ayrshire",
            council_code=="S12000010"~ "East Lothian",
            council_code=="S12000011"~ "East Renfrewshire",
            council_code=="S12000013"~ "Na h-Eileanan Siar",
            council_code=="S12000014"~ "Falkirk",
            council_code=="S12000015"~ "Fife",
            council_code=="S12000017"~ "Highland",
            council_code=="S12000018"~ "Inverclyde",
            council_code=="S12000019"~ "Midlothian",
            council_code=="S12000020"~ "Moray",
            council_code=="S12000021"~ "North Ayrshire",
            council_code=="S12000023"~ "Orkney Islands",
            council_code=="S12000024"~ "Perth and Kinross",
            council_code=="S12000026"~ "Scottish Borders",
            council_code=="S12000027"~ "Shetland Islands",
            council_code=="S12000028"~ "South Ayrshire",
            council_code=="S12000029"~ "South Lanarkshire",
            council_code=="S12000030"~ "Stirling",
            council_code=="S12000033"~ "Aberdeen City",
            council_code=="S12000034"~ "Aberdeenshire",
            council_code=="S12000035"~ "Argyll and Bute",
            council_code=="S12000036"~ "City of Edinburgh",
            council_code=="S12000038"~ "Renfrewshire",
            council_code=="S12000039"~ "West Dunbartonshire",
            council_code=="S12000040"~ "West Lothian",
            council_code=="S12000041"~ "Angus",
            council_code=="S12000042"~ "Dundee City",
            council_code=="S12000044"~ "North Lanarkshire",
            council_code=="S12000045"~ "East Dunbartonshire",
            council_code=="S12000046"~ "Glasgow City",
            council_code=="S12000047"~ "Fife",
            council_code=="S12000048"~ "Perth and Kinross",
            council_code=="S12000049"~ "Glasgow City",
            council_code=="S12000050"~ "North Lanarkshire")
} 



