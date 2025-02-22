#===============================================================================
# Script Name:     functions.R
# Author:          Simal Dolek
# Created Date:    2025-02-18
# Last Modified:   2025-02-18
# Description:     This script defines necessary functions that are used in the
#                  subsequent data cleaning and feature engineering scripts. 
#                  Make sure to source this script before running others in 
#                  PROSIT-Mobile-Sensing repository. 
# 
# Usage:           Source this script in R to establish a connection.
#                  Ensure environment variables (PSQL_HOST, PSQL_DB, 
#                  PSQL_USER, PSQL_PASSWORD) are set in .Renviron before running.
#                  Use file.edit("~/.Renviron") and readRenviron("~/.Renviron").
#===============================================================================

library(tidyverse)
library(DBI)
library(RPostgreSQL)


###############################################################################
##### Functions for Database Connection (used in every subsequent script) #####
###############################################################################


# This function connects to the database 
connect_to_prosit_database <- function() {
  
  # Connection details
  db_host <- Sys.getenv("PSQL_HOST")
  db_port <- 5432
  db_name <- Sys.getenv("PSQL_DB")
  db_user <- Sys.getenv("PSQL_USER")
  db_password <- Sys.getenv("PSQL_PASSWORD")
  
  con <- dbConnect(
    dbDriver("PostgreSQL"),
    dbname = db_name,
    host = db_host,
    port = db_port,
    user = db_user,
    password = db_password
  )
  assign("database_connection", con, envir = .GlobalEnv)
}


# This function disconnects from the database 
disconnect_from_prosit_database <- function() {
  
  return(dbDisconnect(database_connection))
  
}


# This function takes in SQL query and returns the data table
query_database <- function(query) {
  return(dbGetQuery(database_connection, query))
}


# This function saves the data into user1_workspace schema in the PSQL database
psql_save <- function(data, table_name) {
  # first check database connection 
  if (!exists("database_connection") || is.null(database_connection)) {
    stop("Database connection is not active. Run connect_to_prosit_database() first.")
  }
  
  schema_name <- "user1_workspace"
  full_table_name <- DBI::SQL(paste0(schema_name, ".", table_name))
  
  dbWriteTable(database_connection, full_table_name, data, 
               append = TRUE, row.names = FALSE)
  
  message("Data successfully saved to: ", schema_name, ".", table_name)
}


# This function raeds data from user1_workspace schema in the PSQL database
psql_read <- function(table_name) {
  # Ensure the database connection is active
  if (!exists("database_connection") || is.null(database_connection)) {
    stop("Database connection is not active. Run connect_to_prosit_database() first.")
  }
  
  schema_name <- "user1_workspace"
  query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
  
  df <- dbGetQuery(database_connection, query)
  return(df)
}




###############################################################################
####################### Functions for gpsCleaning.R ###########################
###############################################################################
library(lutz)    # Provides tz_lookup_coords()
library(sftime)  # Provides tz_offset()


# These functions extract the longitude and latitude coordinates 
# return NA for missing values, and convert all non-NA values to numeric
# based on "latitude | longitude | ...." format
extract_latitude <- function(input) {
  parts <- strsplit(as.character(input), "\\|")[[1]]
  lat <- trimws(parts[1])
  
  if (lat == "None" || lat == "" || is.na(lat)) {
    return(NA)  
  } else {
    return(as.numeric(lat))  
  }
}


extract_longitude <- function(input) {
  parts <- strsplit(as.character(input), "\\|")[[1]]
  lon <- trimws(parts[2])
  
  if (lon == "None" || lon == "" || is.na(lon)) {
    return(NA)  
  } else {
    return(as.numeric(lon))  
  }
}



# This function calculates the UTC offset (in hours) for a given date-time and location.
# 
# Inputs:
# - date_time: A character string representing the date-time (must be convertible to POSIXct).
# - latitude: Numeric, the latitude of the location.
# - longitude: Numeric, the longitude of the location.
# - timezone_id: Optional, a character string specifying the time zone.
# 
# Outputs:
# - A numeric value representing the UTC offset in hours (e.g., -5 for New York, +2 for Paris in summer).
# - If timezone_id is given, it is used directly.
# - If timezone_id is missing, the function looks up the timezone based on latitude and longitude.
# - If latitude and/or longitude are missing, it returns NA.
timezone_offset <- function(date_time, latitude, longitude, timezone_id) {
  date_time_POSIXct <- as.POSIXct(date_time, tz = "UTC")
  
  # If timezone_id is available, use it instead of lookup
  if (!is.na(timezone_id) && timezone_id != "Unknown_Timezone") {
    timezone <- timezone_id
  } else {
    if (is.na(latitude) || is.na(longitude)) {
      return(NA)  # Return NA if coordinates are missing
    }
    timezone <- tz_lookup_coords(lat = latitude, lon = longitude, method = "fast")
  }
  
  return(tz_offset(date_time_POSIXct, tz = timezone)$utc_offset_h)
}




# This function converts a given UTC date-time to local time.
# 
# Inputs:
# - date_time: A character string representing the date-time (must be convertible to POSIXct).
# - latitude: Numeric, the latitude of the location.
# - longitude: Numeric, the longitude of the location.
# - timezone_id: Optional, a character string specifying the time zone.
# 
# Outputs:
# - A POSIXct object representing the local time.
# - If timezone_id is given, it is used directly.
# - If timezone_id is missing, the function looks up the timezone based on latitude and longitude.
# - If the coordinates are missing or timezone lookup fails, it returns NA.

utc_to_local_time <- function(date_time, latitude, longitude, timezone_id) {
  utc_time <- as.POSIXct(date_time, tz = "UTC")
  
  raw_offset <- timezone_offset(date_time, latitude, longitude, timezone_id)
  
  if (is.na(raw_offset)) {
    return(NA)  # Return NA if offset is missing
  }
  
  timezone_difference <- as.difftime(raw_offset, units = "hours")
  return(utc_time + timezone_difference)
} 









