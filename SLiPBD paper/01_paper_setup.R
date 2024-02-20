# -------------------------------------------------------------------------
# SLiPDB
# Setup file
# Denise Jennings, October 2023
# -------------------------------------------------------------------------

# This file creates some new variables used in the analyses, creates subsets of the cohort that are used regularly, and also includes some functions.

# 1. Add derived variables------------------------------------------------

# Create year variables for key dates
slipbd_database <- slipbd_database %>% 
  mutate(conceptionyear = year(est_date_conception)) %>% 
  mutate(year_end = year(date_end_pregnancy)) %>% 
  mutate(booking_year = year(antenatal_booking_date))


# Create month/year variable for estimated date of conception
slipbd_database <- slipbd_database %>%
  mutate(estdate = format(est_date_conception, "%Y-%m"))


# Create month/year variable for date end pregnancya
slipbd_database <- slipbd_database %>%
  mutate(end_month = format(date_end_pregnancy, "%Y-%m"))


# Create BMI categories variable for live and still birth cohort for presentation in table
slipbd_database <- slipbd_database %>%
  mutate(maternal_bmi_cat = case_when(maternal_bmi <18.5 ~ "<18.5", 
                                      between(maternal_bmi, 18.5, 24.9) ~ "18.5-24.9", 
                                      between(maternal_bmi, 25, 29.9) ~ "25-29.9", 
                                      between(maternal_bmi, 30, 39.9)  ~ "30-39.9", 
                                      maternal_bmi >= 40 ~ "40+"))

levels_order <- c("<18.5", "18.5-24.9", "25-29.9", "30-39.9", "40+")

slipbd_database$maternal_bmi_cat <- factor(slipbd_database$maternal_bmi_cat, levels = levels_order)


# 2. Create filtered cohorts-----------------------------------------------

# Create cohort with ongoing pregnancies removed
#ongoing_removed <- slipbd_database %>% 
  #filter(fetus_outcome1 != "Ongoing")


# Create cohort for live and stillbirth only 
#live_still <- slipbd_database %>% 
  #filter(fetus_outcome1 == "Live birth" | fetus_outcome1 == "Stillbirth")


# Create cohort for live births only
#live_births_only <- slipbd_database %>% 
  #filter(fetus_outcome1 == "Live birth" | fetus_outcome2 == "Live birth" )


# Create cohort with live births only for previous cohort
#live_births_only_old <- slipbd_database_old %>% 
  #filter(fetus_outcome1 == "Live birth" | fetus_outcome2 == "Live birth" )


# Create cohort of completed pregnancies 
# For older data
#unknown <- c("Unknown", "Unknown - assumed early loss", "Unknown - emigrated", "Ongoing")

#completed_old <- slipbd_database_old %>% 
  #filter(!fetus_outcome1 %in% unknown)

# For updated data
#completed_new <- slipbd_database %>% 
  #filter(!fetus_outcome1 %in% unknown)


# Create cohort only including cutoff period 
#cutoff_cohort <- slipbd_database %>% 
  #filter(date_end_pregnancy >= cutoff_date)



# 3. Set functions---------------------------------------------------------

# Graph breaks function
every_nth = function(n) {
  return(function(x) {x[c(TRUE, rep(FALSE, n - 1))]})
}



# Theme for graphs
theme <-  theme(panel.background = element_blank(),
                panel.grid.major.y = element_line(colour = "light grey"))

# Unlinked graph function
unlinked_graph <- function(data, x, outcome, title){
  
  data %>% 
    ggplot(aes(x = !!sym(paste0(x)), y = total, group = !!sym(paste0(outcome)))) +
    geom_line(aes(colour = !!sym(paste0(outcome)))) +
    xlab("Month/year") +
    ylab("Number of events") +
    labs(colour = "Fetus outcome") +
    ggtitle(paste("Number of unlinked events from", title, "by month of admission", subtitle = "")) +
    scale_x_discrete(breaks = every_nth(n = 1)) +
    theme(axis.text.x = element_text(angle=45, vjust=1, hjust=1)) +
    scale_y_continuous(expand=c(0, 0), limits=c(0, NA))
}



# not in function
'%notin%' <- Negate(`%in%`)

# End of script -----------------------------------------------------------
