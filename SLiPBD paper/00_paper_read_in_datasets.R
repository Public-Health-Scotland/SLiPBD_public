# -------------------------------------------------------------------------
# SLiPDB
# Read in data
# Denise Jennings, October 2023
# -------------------------------------------------------------------------
#updates <- list.files("/PHI_conf/SLiPBD/data/archive", pattern = "(.*)(.*)(..._)([0-9])([0-9])", full.names = TRUE)
#latest <- max(updates)

source(paste0(here::here() , "/SLiPBD paper/000_data_paths.r"))
slipbd_database <- readRDS(paste0(data_path , "archive/slipbd_database_old_2023_12.rds"))
all_variables <-  readRDS(paste0(data_path , "historic/final_dataset_all_variables.rds"))
antenatal_booking <-readRDS(paste0(data_path ,"historic/antenatal_booking.rds"))
smr01 <- readRDS(paste0(data_path ,"historic/smr01.rds"))
smr02_data <- readRDS(paste0(data_path ,"historic/smr02_data.rds"))
smr02_nonlive <- readRDS(paste0(data_path ,"historic/smr02_nonlive.rds"))
tops_aas <- readRDS(paste0(data_path ,"historic/tops_aas.rds"))
nrs_live_births <-readRDS(paste0(data_path ,"historic/nrs_live_births.rds"))
nrs_sb <- readRDS(paste0(data_path ,"historic/NRS_sb.rds"))
infant_deaths <- readRDS(paste0(data_path ,"historic/infant_deaths.rds"))
#---------------------------------------------------------------------------

