#===============================================================================
# Script Name:     gpsCleaning.R
# Author:          Simal Dolek
# Created Date:    2025-02-18
# Last Modified:   2025-02-23
# Description:     This script fetches raw GPS data (recorded via the PROSIT
#                  mobile sensing app) and carries out data cleaning to 
#                  prepare the dataframe for mobility related feature 
#                  extraction. 
# 
# Usage:           Source this script in R to establish a connection.
#                  Ensure environment variables (PSQL_HOST, PSQL_DB, 
#                  PSQL_USER, PSQL_PASSWORD) are set in .Renviron before running.
#                  Use file.edit("~/.Renviron") and readRenviron("~/.Renviron").
#===============================================================================

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
gps <- gps %>%
  mutate(analytics = 0)  

# Combine `gps` and `analytics_clean` into one dataset
gps_with_analytics <- rbind(gps, analytics_clean)

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

library(RPostgres)  

# personal details
database_connection <- dbConnect(
  RPostgres::Postgres(),  # RPostgres instead of PostgreSQL()!!!
  dbname = Sys.getenv("PSQL_DB"),
  host = Sys.getenv("PSQL_HOST"),   
  port = 5432,  
  user = Sys.getenv("PSQL_USER"),
  password = Sys.getenv("PSQL_PASSWORD")
)


# Change the format of gps so that it's compatible with PSQL
# We will switch it back to the original format during feature engineering
gps_cleaned <- gps %>%
  mutate(
    participantid = as.character(participantid),
    measuredat = as.POSIXct(measuredat, origin = "1970-01-01", tz = "UTC"),
    uploadedat = as.POSIXct(uploadedat, origin = "1970-01-01", tz = "UTC"),
    measuredat_local = as.POSIXct(measuredat_local, origin = "1970-01-01", tz = "UTC"),
    value0 = as.character(value0),  # Add this if value0 exists
    lat = as.numeric(lat),
    lon = as.numeric(lon),
    timezone_difference = as.integer(timezone_difference)
  )


# save gps only data as gps_v1 into the PSQL database
dbAppendTable(
  conn = database_connection, 
  name = Id(schema = "user1_workspace", name = "gps_v1"), 
  value = gps_cleaned
)

# 
gps_with_analytics <- gps_with_analytics %>%
  mutate(
    participantid = as.character(participantid),
    measuredat = as.POSIXct(measuredat, origin = "1970-01-01", tz = "UTC"),
    uploadedat = as.POSIXct(uploadedat, origin = "1970-01-01", tz = "UTC"),
    measuredat_local = as.POSIXct(measuredat_local, origin = "1970-01-01", tz = "UTC"),
    value0 = as.character(value0),
    lat = as.numeric(lat),
    lon = as.numeric(lon),
    timezone_difference = as.integer(timezone_difference),  
    analytics = as.integer(analytics)  
  )



dbAppendTable(
  conn = database_connection, 
  name = Id(schema = "user1_workspace", name = "gps_with_analytics"), 
  value = gps_with_analytics
)


###############################################################################

