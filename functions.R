#===============================================================================
# Script Name:     functions.R
# Author:          Simal Dolek
# Created Date:    2025-02-18
# Last Modified:   2025-02-18
# Description:     This script defines necessary functions that are used in the
#                  subsequent data cleaning and feature engineering scripts. 
#                  Make sure to run this script first before running others in 
#                  PROSIT-Mobile-Sensing repository. 
# 
# Usage:           Source this script in R to establish a connection.
#                  Ensure environment variables (PSQL_HOST, PSQL_DB, 
#                  PSQL_USER, PSQL_PASSWORD) are set in .Renviron before running.
#                  Use file.edit("~/.Renviron") and readRenviron("~/.Renviron").
#===============================================================================


library(DBI)
library(RPostgreSQL)


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



