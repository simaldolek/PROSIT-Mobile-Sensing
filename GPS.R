#===============================================================================
# Script Name:     GPS.R
# Author:          Simal Dolek
# Created Date:    2025-02-18
# Last Modified:   2025-02-23
# Description:      
# 
# Usage:           Source this script in R to establish a connection.
#                  Ensure environment variables (PSQL_HOST, PSQL_DB, 
#                  PSQL_USER, PSQL_PASSWORD) are set in .Renviron before running.
#                  Use file.edit("~/.Renviron") and readRenviron("~/.Renviron").
#===============================================================================
library(geodist)
library(purrr)

# source functions.R script.
conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)

connect_to_prosit_database()

query <- "SELECT * FROM study_prositsm.location ORDER BY measuredat ASC;"
gps <- query_database(query)


#####################################################################
# Data Cleaning Starts
#####################################################################

# Extract latitude and add as a new column to gps
gps <- gps %>%
  mutate(lat = sapply(location_decrypted, extract_latitude))

# Extract longitude and add as a new column to gps
gps <- gps %>%
  mutate(lon = sapply(location_decrypted, extract_longitude))

# make sure lat and lon mutated properly
head(gps)


# Computing timezone offsets 

# convert measuredat to POSIXct type date
gps$measuredat_local <- as.POSIXct(gps$measuredat)

# Step 1: Assign timezone from timezone_id, 
#convert missing values and "Unknown_Timezone" into NAs
gps <- gps %>%
  mutate(
    timezone = ifelse(timezone_id == "Unknown_Timezone" | is.na(timezone_id), NA_character_, timezone_id)
  )


# Step 2: Compute timezone offset and save under a new column "timezone_difference"
gps$timezone_difference <- sapply(1:nrow(gps), function(i) {
  tz <- gps$timezone[i]
  dt <- gps$measuredat[i]
  
  if (is.na(tz)) {
    return(NA_real_)  # NA timezones are automatically NA
  }
  
  return(tz_offset(as.POSIXct(dt), tz = tz)$utc_offset_h)
})


# Step 3: Adjust measuredat_local using the computed offset
gps <- gps %>%
  mutate(measuredat_local = measuredat + as.difftime(timezone_difference, units = "hours"))



# Keep only necessary columns 
gps <- gps[
  , c(
    "participantid", "measuredat", "uploadedat", "value0", "lat", "lon",
    "measuredat_local", "timezone_difference"
  )
]



###############################################################################
##### Combine the GPS data with the Analytics to account for app crashes ######
###############################################################################

connect_to_prosit_database()

query <- "SELECT * FROM study_prositsm.analytics ORDER BY measuredat ASC;"
analytics <- query_database(query)

analytics_clean <- filter(analytics, value0 == "Terminating")

#Drop `_id` column 
analytics_clean <- analytics_clean %>%
  select(-`_id`) 

# prepare the analytics_clean format to exactly match gps so we can rowbind
analytics_clean <- analytics_clean %>%
  mutate(
    lat = NA_real_,  # Ensure NA is stored as numeric (double)
    lon = NA_real_,
    measuredat_local = NA_character_,
    timezone_difference = NA_real_,
    analytics = 1  # Column to differentiate analytics vs. gps rows
  )

analytics_clean <- analytics_clean %>%
  mutate(
    measuredat = as.POSIXct(measuredat, format = "%Y-%m-%d %H:%M:%S"),
    uploadedat = as.POSIXct(uploadedat, format = "%Y-%m-%d %H:%M:%S"),
    measuredat_local = as.POSIXct(measuredat_local, format = "%Y-%m-%d %H:%M:%S"),
    timezone_difference = as.numeric(timezone_difference),
    analytics = as.numeric(analytics)
  )

# add a new column called analytics to differentiate the two for further analysis
gps2 <- gps %>%
  mutate(analytics = 0)  

# Combine `gps` and `analytics_clean` into one dataset
gps_with_analytics <- rbind(gps2, analytics_clean)

# order rows by participant IDs and measured at time stamps
gps_with_analytics <- gps_with_analytics[with(gps_with_analytics, order(participantid, measuredat)),]

# remove the duplicated rows from the data frame
gps_with_analytics <- gps_with_analytics[!duplicated(gps_with_analytics[c(1:9)]), ]




##############################################################################
######## save the GPS data into PSQL database under personal schema ##########
##############################################################################
# For this section, we manually created tables in the PSQL database 
# under the user1_workspace schema, then uploaded the dataframes from R into PSQL
# Skip or edit details for your personal database 

# Change the format of gps so that it's compatible with PSQL
# We will switch it back to the original format during feature engineering
#gps_PSQL <- gps %>%
#  mutate(
#    participantid = as.character(participantid),
#    measuredat = as.POSIXct(measuredat, origin = "1970-01-01", tz = "UTC"),
#    uploadedat = as.POSIXct(uploadedat, origin = "1970-01-01", tz = "UTC"),
#    measuredat_local = as.POSIXct(measuredat_local, origin = "1970-01-01", tz = "UTC"),
#    value0 = as.character(value0),  # Add this if value0 exists
#    lat = as.numeric(lat),
#    lon = as.numeric(lon),
#    timezone_difference = as.integer(timezone_difference)
#  )


#append_to_db(gps_PSQL, "user1_workspace", "gps_v1")


# 
#gps_with_analytics <- gps_with_analytics %>%
#  mutate(
#    participantid = as.character(participantid),
#    measuredat = as.POSIXct(measuredat, origin = "1970-01-01", tz = "UTC"),
#    uploadedat = as.POSIXct(uploadedat, origin = "1970-01-01", tz = "UTC"),
#    measuredat_local = as.POSIXct(measuredat_local, origin = "1970-01-01", tz = "UTC"),
#    value0 = as.character(value0),
#    lat = as.numeric(lat),
#    lon = as.numeric(lon),
#    timezone_difference = as.integer(timezone_difference),  
#    analytics = as.integer(analytics)  
#  )

#append_to_db(gps_with_analytics, "user1_workspace", "gps_with_analytics")

###############################################################################

#rm(list=ls())

connect_to_prosit_database()

query <- "SELECT * FROM user1_workspace.gps_with_analytics ORDER BY measuredat ASC;"
gpsa <- query_database(query)


# Reconvert types from PSQL to R
gpsa$lat <- as.numeric(as.character(gpsa$lat))
gpsa$lon <- as.numeric(as.character(gpsa$lon))
gpsa$measuredat <- as.POSIXct(gpsa$measuredat)
gpsa$measuredat_local <- as.POSIXct(as.character(gpsa$measuredat_local))
gpsa$participantid <- as.character(gpsa$participantid)

###############################################################################
######################### Feature Engineering Starts ##########################
###############################################################################

# Location Variance Computation

# calculate the variance of latitude and longitude for PIDs with more than 1 data point
# take the log of the sum of the variances to compute location variance
gps_loc_var_daily <- gpsa %>%
  mutate(
    measured_date = as.Date(measuredat)        
  ) %>%
  group_by(participantid, measured_date) %>% 
  filter(n() > 1) %>%  
  summarize(
    location_variance = log(var(lat, na.rm = TRUE) + var(lon, na.rm = TRUE)),
    .groups = "drop"
  )




# Total Haversine Distance Computation

# Haversine distance = shortest distance between two points on a sphere
# calculate pairwise Haversine distances between consecutive points
# and sum them up to get total haversine distance per participant
gps_haversine_daily <- gpsa %>%
  mutate(
    measured_date = as.Date(measuredat)  
  ) %>%
  group_by(participantid, measured_date) %>%
  filter(n() > 1) %>%  
  summarise(
    total_haversine = sum(
      geodist(
        data.frame(lon = lon, lat = lat), 
        measure = "haversine", sequential = TRUE, pad = TRUE
      ),
      na.rm = TRUE
    ),
    .groups = "drop"
  )




# Detection of Stay Points

# calculate time diff between consecutive location points for each participant daily
# filter out locations where PID stayed less than 10 mins, keep only >= 10 mins
# calculate haversine distance between the all of the remaining location points 
# detect and record "stay clusters" where pariwise haversine dist <= 20 meters
detected_stay_points <- gpsa %>%
  mutate(
    measured_date = as.Date(measuredat), 
    time_spent_there = difftime(lead(measuredat_local), measuredat_local, units = "secs")
  ) %>%
  group_by(participantid, measured_date) %>%
  filter(time_spent_there >= 600) %>%  # (>=10 minutes)
  do({
    # pairwise Haversine distance 
    dist_matrix <- geodist(
      data.frame(lon = .$lon, lat = .$lat),
      measure = "haversine"
    )
    
    # stay clusters: TRUE if within 20m
    stay_cluster_vector <- rowSums(dist_matrix <= 20, na.rm = TRUE) > 1
    mutate(., stay_cluster = stay_cluster_vector)
  }) %>%
  ungroup()



# Now compute daily number of stay points per person
gps_stay_points_daily <- detected_stay_points %>%
  filter(stay_cluster == TRUE) %>%  
  mutate(
    stay_point_id = paste0(round(lat, 4), "_", round(lon, 4))  
  ) %>%
  group_by(participantid, measured_date) %>%
  summarise(
    number_of_stay_points = n_distinct(stay_point_id),  
    .groups = "drop"
  ) 


# Because we filtered the stay points by the stay clusters, there are less rows 
# in this data frame compared to the other features like gps_haversine_daily.
# In order to keep everything aligned (as we are working with identical PIDs and 
# dates across different GPS features), we will import the exact same days from 
# gps_haversine_daily for each of the participants. If a date for a participant 
# doesn't exist in the gps_stay_points_daily, we will add it where number_of_stay_points = NA.
gps_stay_points_daily <- gps_haversine_daily %>%
  select(participantid, measured_date) %>%  # Extract the exact same days
  left_join(gps_stay_points_daily, by = c("participantid", "measured_date")) %>%
  mutate(number_of_stay_points = ifelse(is.na(number_of_stay_points), NA, number_of_stay_points))  


# Location Entropy

# use the detected_stay_points to calculate the sum of the total time spent 
# across all stay points. If total time is greater than 0, compute probability 
# of each stay point, and use it to compute Shannon entropy for non-zero 
# probabilities
gps_loc_entropy_daily <- detected_stay_points %>%
  group_by(participantid, measured_date) %>%
  summarise(
    total_time_at_clusters = sum(as.numeric(time_spent_there), na.rm = TRUE),  
    location_entropy = ifelse(
      total_time_at_clusters > 0,
      -sum((as.numeric(time_spent_there) / total_time_at_clusters) *
             log(as.numeric(time_spent_there) / total_time_at_clusters), na.rm = TRUE),
      0
    ),
    .groups = "drop"
  )



# add missing days with entropy = NA
gps_loc_entropy_daily <- gps_haversine_daily %>%
  select(participantid, measured_date) %>%  # Extract the exact same days
  left_join(gps_loc_entropy_daily, by = c("participantid", "measured_date")) %>%
  mutate(location_entropy = ifelse(is.na(location_entropy), NA, location_entropy))  






# Merge all gps feature dataframes on `participantid` and `measured_date`
dataframes_list <- list(gps_haversine_daily,
                        gps_stay_points_daily,
                        gps_loc_entropy_daily,
                        gps_loc_var_daily)
gps_features_daily <- reduce(dataframes_list, left_join, by = c("participantid", "measured_date"))





##############################################################################
######## save the GPS data into PSQL database under personal schema ##########
##############################################################################

# Change the format so it's compatible with PSQL
#gps_features_daily <- gps_features_daily %>%
#  mutate(
#    measured_date = as.Date(measured_date, format = "%Y-%m-%d"),  # Ensure Date format
#    total_haversine = as.numeric(total_haversine),
#    number_of_stay_points = as.integer(number_of_stay_points),
#    total_time_at_clusters = as.numeric(total_time_at_clusters),
#    location_entropy = as.numeric(location_entropy),
#    location_variance = as.numeric(location_variance)
#  )

#append_to_db(gps_features_daily, "user1_workspace", "daily_gps_features")







