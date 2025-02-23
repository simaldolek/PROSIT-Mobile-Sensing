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
library(RPostgres)


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


# Function to establish connection
connect_to_db <- function() {
  dbConnect(
    RPostgres::Postgres(),
    dbname = Sys.getenv("PSQL_DB"),
    host = Sys.getenv("PSQL_HOST"),
    port = 5432,
    user = Sys.getenv("PSQL_USER"),
    password = Sys.getenv("PSQL_PASSWORD")
  )
}

# This function appends a dataframe to an existing table in the PSQL database
# example use: append_to_db(gps_cleaned, "user1_workspace", "gps_v1")
append_to_db <- function(data, schema, table) {
  conn <- connect_to_db()  
  
  tryCatch({
    dbAppendTable(
      conn = conn,
      name = DBI::Id(schema = schema, name = table),
      value = data
    )
    message(paste("✅ Data successfully appended to", schema, ".", table))
  }, error = function(e) {
    message(paste("❌ Error appending data:", e$message))
  }, finally = {
    dbDisconnect(conn) 
  })
}


###############################################################################
####################### Functions for gpsCleaning.R ###########################
###############################################################################
library(lutz)    # Provides tz_lookup_coords()
library(sftime)  # Provides tz_offset()


# These functions extract the longitude and latitude coordinates 
# return NA for missing values, and convert all non-NA values to numeric
# based on "latitude | longitude | ...." format
# Extract the longitude and latitude coordinates
extract_latitude <- function(input) {
  # Split the input string by '|'
  parts <- str_split(input, " \\| ", simplify = TRUE)
  
  # Check if the first value is "None" or missing
  if (length(parts) < 1 || parts[1] == "None" || is.na(parts[1])) {
    return(NA)
  }
  # Convert to numeric
  return(as.numeric(parts[1]))
}


extract_longitude <- function(input) {

  parts <- str_split(input, " \\| ", simplify = TRUE)
  
  if (length(parts) < 2 || parts[2] == "None" || is.na(parts[2])) {
    return(NA)
  }
  return(as.numeric(parts[2]))
}


