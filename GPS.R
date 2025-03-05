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

# Extract latitude 
gps <- gps %>%
  mutate(lat = sapply(location_decrypted, extract_latitude))

# Extract longitude 
gps <- gps %>%
  mutate(lon = sapply(location_decrypted, extract_longitude))

head(gps)


# COMPUTING TIMEZONE OFFSETS

# convert measuredat to POSIXct type date
gps$measuredat_local <- as.POSIXct(gps$measuredat)

# Step 1: Assign timezone <- timezone_id
# [if timezone is missing assign NA, otherwise assign timezone_id]
gps <- gps %>%
  mutate(
    timezone = ifelse(timezone_id == "Unknown_Timezone" | is.na(timezone_id), NA_character_, timezone_id)
  )


# Step 2: Compute timezone offset based on timezones
# save under a new column "timezone_difference"
gps$timezone_difference <- sapply(1:nrow(gps), function(i) {
  tz <- gps$timezone[i]
  dt <- gps$measuredat[i]
  
  if (is.na(tz)) {
    return(NA_real_)  # NA timezones are automatically NA
  }
  
  return(tz_offset(as.POSIXct(dt), tz = tz)$utc_offset_h)
})


# Step 3: Calculate measuredat_local by adding computed offset to UTC
gps <- gps %>%
  mutate(measuredat_local = measuredat + as.difftime(timezone_difference, units = "hours"))



# Keep only necessary columns 
gpsa <- gps[
  , c(
    "participantid", "measuredat", "uploadedat", "value0", "lat", "lon",
    "measuredat_local", "timezone_difference"
  )
]


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
  mutate(measured_date = as.Date(measuredat)) %>%
  group_by(participantid, measured_date) %>%
  filter(n() > 1) %>%
  filter(!is.na(lat) & !is.na(lon)) %>%  # Remove rows with missing lat or lon
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

# mark "stay clusters" where a participant has stayed within 20 meters of 
# another location for at least 10 minutes.
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

table(detected_stay_points$stay_cluster)

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




# Location Entropy

# use the detected_stay_points to calculate the sum of the total time spent 
# across all stay points. If total time is greater than 0, compute probability 
# of each stay point, and use it to compute Shannon entropy for non-zero 
# probabilities
gps_loc_entropy_daily <- detected_stay_points %>%
  filter(stay_cluster == TRUE) %>%
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


################################################################################

# Because we filtered some rows when computing GPS features, there are distinct  
# number of rows corresponding to 'participantid' - 'measured_date' combinations. 
# In order to keep everything aligned (as we are working with identical PIDs and 
# dates across different GPS features), we will match the number of rows for
# each feature, with values = NA for newly added rows.

################################################################################

gpsa$measured_date <- as.Date(gpsa$measuredat)

gpsa_PID_date_combos <- gpsa %>%
  select(participantid, measured_date) %>%
  distinct()

gps_entropy_combos <- gps_loc_entropy_daily %>%
  select(participantid, measured_date) %>%
  distinct()

gps_loc_combos <- gps_loc_var_daily %>%
  select(participantid, measured_date) %>%
  distinct()

gps_haversine_combos <- gps_haversine_daily  %>%
  select(participantid, measured_date) %>%
  distinct()

gps_stay_points_combos <- gps_stay_points_daily  %>%
  select(participantid, measured_date) %>%
  distinct()



# Step 2: Convert to vectors
gpsa_vector <- paste(gpsa_PID_date_combos$participantid, gpsa_PID_date_combos$measured_date, sep = "_")
gps_entropy_vector <- paste(gps_entropy_combos$participantid, gps_entropy_combos$measured_date, sep = "_")
gps_loc_var_vector <- paste(gps_loc_combos$participantid, gps_loc_combos$measured_date, sep = "_")
gps_haversine_vector <- paste(gps_haversine_combos$participantid, gps_haversine_combos$measured_date, sep = "_")
gps_stay_points_vector <- paste(gps_stay_points_combos$participantid, gps_stay_points_combos$measured_date, sep = "_")



# Step 3: Find missing combinations
missing_combos_entropy <- setdiff(gpsa_vector, gps_entropy_vector)
missing_combos_loc_var <- setdiff(gpsa_vector, gps_loc_var_vector)
missing_combos_haversine <- setdiff(gpsa_vector, gps_haversine_vector)
missing_combos_stay_points <- setdiff(gpsa_vector, gps_stay_points_vector)



# Step 4: Convert missing combos back to a data frame
missing_combos_df_entropy <- data.frame(
  participantid = sub("_.*", "", missing_combos_entropy),
  measured_date = as.Date(sub(".*_", "", missing_combos_entropy)),
  total_time_at_clusters = NA,  
  location_entropy = NA          
)


missing_combos_df_loc_var <- data.frame(
  participantid = sub("_.*", "", missing_combos_loc_var),
  measured_date = as.Date(sub(".*_", "", missing_combos_loc_var)),
  location_variance = NA          
)


missing_combos_df_haversine <- data.frame(
  participantid = sub("_.*", "", missing_combos_haversine),
  measured_date = as.Date(sub(".*_", "", missing_combos_haversine)),
  total_haversine = NA          
)


missing_combos_df_stay_points <- data.frame(
  participantid = sub("_.*", "", missing_combos_stay_points),
  measured_date = as.Date(sub(".*_", "", missing_combos_stay_points)),
  number_of_stay_points = NA          
)



# Step 5: Append missing rows to gps_loc_entropy_daily
gps_loc_entropy_daily <- rbind(gps_loc_entropy_daily, missing_combos_df_entropy)
gps_loc_var_daily <- rbind(gps_loc_var_daily, missing_combos_df_loc_var)
gps_haversine_daily <- rbind(gps_haversine_daily, missing_combos_df_haversine)
gps_stay_points_daily <- rbind(gps_stay_points_daily, missing_combos_df_stay_points)



# Step 6: Order by PID and measured_date
gps_loc_entropy_daily <- gps_loc_entropy_daily %>% arrange(participantid, measured_date)
gps_loc_var_daily <- gps_loc_var_daily  %>% arrange(participantid, measured_date)
gps_haversine_daily <- gps_haversine_daily  %>% arrange(participantid, measured_date)
gps_stay_points_daily <- gps_stay_points_daily  %>% arrange(participantid, measured_date)



################################################################################
############### Merge all Features into a Single Dataframe #####################
################################################################################


# Merge all gps feature dataframes on `participantid` and `measured_date`
features_list <- list(gps_haversine_daily,
                        gps_stay_points_daily,
                        gps_loc_entropy_daily,
                        gps_loc_var_daily)

# Merge all data frames by "participantid" and "measured_date"
gps_features_daily <- reduce(features_list, inner_join, by = c("participantid", "measured_date"))


##############################################################################
######## save the GPS data into PSQL database under personal schema ##########
##############################################################################

# Change the format so it's compatible with PSQL
gps_features_daily <- gps_features_daily %>%
    mutate(
    measured_date = as.Date(measured_date, format = "%Y-%m-%d"),  # Ensure Date format
    total_haversine = as.numeric(total_haversine),
    number_of_stay_points = as.integer(number_of_stay_points),
    total_time_at_clusters = as.numeric(total_time_at_clusters),
    location_entropy = as.numeric(location_entropy),
    location_variance = as.numeric(location_variance)
  )

append_to_db(gps_features_daily, "user1_workspace", "daily_gps_features")




