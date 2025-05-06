
# source functions.R script.
conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)


connect_to_prosit_database()

query <- "SELECT * FROM user1_workspace.daily_gps_features;"
gps <- query_database(query)

query <- "SELECT * FROM user1_workspace.daily_screen_features_android;"
screen_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.daily_screen_features_ios;"
screen_ios <-  query_database(query)

query <- "SELECT * FROM user1_workspace.daily_call_features_android;"
call_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.daily_call_features_ios;"
call_ios <-  query_database(query)

query <- "SELECT * FROM user1_workspace.derived_sleep_features;"
sleep <-  query_database(query)

query <- "SELECT * FROM user1_workspace.derived_android_daily_activity_summary;"
activity_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.derived_ios_daily_activity_summary;"
activity_ios <-  query_database(query)


# minute to second conversions
# (Note: GPS is already in seconds)

sleep <- select(sleep, -total_sleep_hours)
sleep$total_sleep_minutes <- sleep$total_sleep_minutes*60
sleep$total_night_screen_minutes <- sleep$total_night_screen_minutes*60

activity_and$non_vigorous_pa_minutes <- activity_and$non_vigorous_pa_minutes*60
activity_and$vigorous_pa_minutes <- activity_and$vigorous_pa_minutes*60
activity_and$total_active_minutes <- activity_and$total_active_minutes*60
activity_and$total_inactive_minutes <- activity_and$total_inactive_minutes*60
activity_and$day_minutes <- activity_and$day_minutes*60
activity_and$evening_minutes <- activity_and$evening_minutes*60
activity_and$night_minutes <- activity_and$night_minutes*60

activity_ios$non_vigorous_pa_minutes <- activity_ios$non_vigorous_pa_minutes*60
activity_ios$vigorous_pa_minutes <- activity_ios$vigorous_pa_minutes*60
activity_ios$total_active_minutes <- activity_ios$total_active_minutes*60
activity_ios$total_inactive_minutes <- activity_ios$total_inactive_minutes*60
activity_ios$day_minutes <- activity_ios$day_minutes*60
activity_ios$evening_minutes <- activity_ios$evening_minutes*60
activity_ios$night_minutes <- activity_ios$night_minutes*60


################################################################################
###################### Android vs. iOS Separation ##############################
################################################################################

sleep_and <- sleep %>% filter(device_type == "android")
sleep_ios <- sleep %>% filter(device_type == "ios")


android_ids <- unique(c(
  screen_and$participantid,
  call_and$participantid,
  activity_and$participantid,
  sleep_and$participantid
))


ios_ids <- unique(c(
  screen_ios$participantid,
  call_ios$participantid,
  activity_ios$participantid,
  sleep_ios$participantid
))


# EXCLUDE PARTICIPANTS WHO SWITCH BETWEEN IOS AND ANDROID DEVICES
# android vs. ios lookup dataframe
android_df <- data.frame(participantid = android_ids, device_type = "android")
ios_df <- data.frame(participantid = ios_ids, device_type = "ios")
intersect(android_ids, ios_ids)

# id's that switch between ios and android
bad_ids <- c(
  "prositsm14", "prositsm2097", "prositsm3272", "prositsm5493", "prositsm5703", "prositsm5701", "prositsm5705",
  "prositsm6436", "prositsm6430", "prositsm6434", "prositsm6635", "prositsm6636", "prositsm6637", "prositsm6639",
  "prositsm6640", "prositsm6641", "prositsm6642", "prositsm6643", "prositsm6645", "prositsm6651", "prositsm6665",
  "prositsm6726", "prositsm6729", "prositsm6749", "prositsm6850", "prositsm4650", "prositsm5427", "prositsm6435",
  "prositsm2618"
)


gps <- gps[!gps$participantid %in% bad_ids, ]
sleep_and <- sleep_and[!sleep_and$participantid %in% bad_ids,]
sleep_ios <- sleep_ios[!sleep_ios$participantid %in% bad_ids,]
screen_and <- screen_and[!screen_and$participantid %in% bad_ids, ]
screen_ios <- screen_ios[!screen_ios$participantid %in% bad_ids, ]
activity_and <- activity_and[!activity_and$participantid %in% bad_ids, ]
activity_ios <- activity_ios[!activity_ios$participantid %in% bad_ids, ]
call_and <- call_and[!call_and$participantid %in% bad_ids, ]
call_ios <- call_ios[!call_ios$participantid %in% bad_ids, ]


device_lookup <- unique(rbind(android_df, ios_df))

# add device type by PID to gps
gps$device_type <- NA
gps$device_type[gps$participantid %in% android_ids] <- "android"
gps$device_type[gps$participantid %in% ios_ids] <- "ios"


anyNA(gps$device_type)

gps %>% group_by(participantid) %>% filter(is.na(device_type))

# PIDs that only have GPS data
unmatched_ids <- gps$participantid[!gps$participantid %in% device_lookup$participantid]
length(unique(unmatched_ids))
cat(unique(unmatched_ids), sep = "\n")

# manually assign device type based on PSQL
gps$device_type[gps$participantid == "prositsm400"] <- "ios"

gps$device_type[gps$participantid %in% c(
  "prositsm2210", "prositsm6530", "prositsm6806", "prositsm6818", "prositsm729"
)] <- "android"

anyNA(gps$device_type)


gps_and <- gps %>% filter(device_type == "android")
gps_ios <- gps %>% filter(device_type == "ios")

table(gps_and$device_type)
table(gps_ios$device_type)




################################################################################
#################### Handling "Missing" versus "No" Events #####################
################################################################################

####################################
####### PART 1: Pre-Merge ##########
####################################


# FOLLOWS SANDRA'S LOGIC EXCEL SHEET. SEE LOGIC.XLSX

# Call columns to auto-set to 0
cols_set_0 <- c(
  "num_all_calls",
  "duration_all_calls",
  "num_call_made",
  "duration_calls_made",
  "num_calls_received",
  "duration_calls_received",
  "num_missed_calls",
  "num_rejected_calls"
)

summary(call_and[cols_set_0])
summary(call_ios[cols_set_0])

call_and[cols_set_0] <- lapply(call_and[cols_set_0], function(col) {
  col[is.na(col)] <- 0
  return(col)
})

call_ios[cols_set_0] <- lapply(call_ios[cols_set_0], function(col) {
  col[is.na(col)] <- 0
  return(col)
})

summary(call_and[cols_set_0])
summary(call_ios[cols_set_0])


# auto-set sleep_screen_interruptions to zero
anyNA(sleep_and$screen_interruptions) # no NAs, we're good
anyNA(sleep_ios$screen_interruptions) # no NAs, we're good


# Columns to set 0 if total_screen_time not missing
cols_set_0_if_screentime <- c(
  "daytime_screen_time",
  "evening_screen_time",
  "nighttime_screen_time"
)

summary(screen_and[cols_set_0_if_screentime])
summary(screen_ios[cols_set_0_if_screentime])

for (col in cols_set_0_if_screentime) {
  idx <- is.na(screen_and[[col]]) & !is.na(screen_and$total_screen_time)
  screen_and[[col]][idx] <- 0
}

for (col in cols_set_0_if_screentime) {
  idx <- is.na(screen_ios[[col]]) & !is.na(screen_ios$total_screen_time)
  screen_ios[[col]][idx] <- 0
}

summary(screen_and[cols_set_0_if_screentime])
summary(screen_ios[cols_set_0_if_screentime])


# Columns to set 0 if screen_num_of_events_total not missing
cols_set_0_if_screennum <- c(
  "num_of_events_daytime",
  "num_of_events_evening",
  "num_of_events_nighttime"
)

summary(screen_and[cols_set_0_if_screennum])
summary(screen_ios[cols_set_0_if_screennum])

for (col in cols_set_0_if_screennum) {
  idx <- is.na(screen_and[[col]]) & !is.na(screen_and$num_of_events_total)
  screen_and[[col]][idx] <- 0
}

for (col in cols_set_0_if_screennum) {
  idx <- is.na(screen_ios[[col]]) & !is.na(screen_ios$num_of_events_total)
  screen_ios[[col]][idx] <- 0
}

summary(screen_and[cols_set_0_if_screennum])
summary(screen_ios[cols_set_0_if_screennum])



# Columns to set 0 if activity_total_inactive_minutes not missing
cols_set_0_if_total_inactive <- c(
  "non_vigorous_pa_minutes",
  "vigorous_pa_minutes",
  "total_active_minutes"
)

summary(activity_and[cols_set_0_if_total_inactive])
summary(activity_ios[cols_set_0_if_total_inactive])
anyNA(activity_and[cols_set_0_if_total_inactive]) # no NA's, we good
anyNA(activity_ios[cols_set_0_if_total_inactive]) # no NA's, we good



# Columns to set 0 if activity_total_active_minutes not missing
cols_set_0_if_total_active <- c(
  "day_minutes",
  "evening_minutes",
  "night_minutes"
)


summary(activity_and[cols_set_0_if_total_active])
summary(activity_ios[cols_set_0_if_total_active])
anyNA(activity_and[cols_set_0_if_total_active]) # no NA's, we good
anyNA(activity_ios[cols_set_0_if_total_active]) # no NA's, we good



################################################################################
###################### Winsorize the Outliers + Normalize ######################
################################################################################

winsorize_col_verbose <- function(x) {
  Q1 <- quantile(x, 0.25, na.rm = TRUE)
  Q3 <- quantile(x, 0.75, na.rm = TRUE)
  IQR <- Q3 - Q1
  lower <- Q1 - 1.5 * IQR
  upper <- Q3 + 1.5 * IQR
  
  # Copy original
  x_orig <- x
  
  # Winsorize
  x[x < lower] <- lower
  x[x > upper] <- upper
  
  # Count clipped values
  clipped <- sum(x != x_orig, na.rm = TRUE)
  
  return(list(values = x, clipped = clipped))
}



winsorize_and_scale_df <- function(df, cols_to_process, df_name) {
  for (col in cols_to_process) {
    cat(sprintf("Processing %s$%s...\n", df_name, col))
    x <- df[[col]]
    
    # Handle infinite values
    x[is.infinite(x)] <- NA
    
    # Winsorize
    result <- winsorize_col_verbose(x)
    cat(sprintf("  Clipped %d values\n", result$clipped))
    
    # Scale
    df[[col]] <- as.numeric(scale(result$values))
  }
  assign(df_name, df, envir = .GlobalEnv)
  summary(df)
}


winsorize_and_scale_df(gps_and, c(
  "total_haversine", "number_of_stay_points", "total_time_at_clusters",
  "location_entropy", "location_variance"
), "gps_and")


winsorize_and_scale_df(gps_ios, c(
  "total_haversine", "number_of_stay_points", "total_time_at_clusters",
  "location_entropy", "location_variance"
), "gps_ios")


winsorize_and_scale_df(screen_and, c(
  "total_screen_time", "num_of_events_total", "daytime_screen_time",
  "num_of_events_daytime", "evening_screen_time", "num_of_events_evening",
  "nighttime_screen_time", "num_of_events_nighttime"
), "screen_and")


winsorize_and_scale_df(screen_ios, c(
  "total_screen_time", "num_of_events_total", "daytime_screen_time",
  "num_of_events_daytime", "evening_screen_time", "num_of_events_evening",
  "nighttime_screen_time", "num_of_events_nighttime"
), "screen_ios")


winsorize_and_scale_df(activity_and, c(
  "non_vigorous_pa_minutes", "vigorous_pa_minutes", "total_active_minutes",
  "total_inactive_minutes", "percent_sedentary", "day_minutes", "evening_minutes",
  "night_minutes", "avg_euclidean_norm", "max_euclidean_norm", "activity_variability"
), "activity_and")


winsorize_and_scale_df(activity_ios, c(
  "non_vigorous_pa_minutes", "vigorous_pa_minutes", "total_active_minutes",
  "total_inactive_minutes", "percent_sedentary", "day_minutes", "evening_minutes",
  "night_minutes", "avg_euclidean_norm", "max_euclidean_norm", "activity_variability"
), "activity_ios")


winsorize_and_scale_df(call_and, c(
  "num_all_calls", "duration_all_calls", "num_call_made", "duration_calls_made",
  "num_calls_received", "duration_calls_received", "num_missed_calls", "num_rejected_calls"
), "call_and")


winsorize_and_scale_df(call_ios, c(
  "num_all_calls", "duration_all_calls", "num_call_made", "duration_calls_made",
  "num_calls_received", "duration_calls_received", "num_missed_calls", "num_rejected_calls"
), "call_ios")


winsorize_and_scale_df(sleep_and, c(
  "total_sleep_minutes", "total_night_screen_minutes", "screen_interruptions"
), "sleep_and")


winsorize_and_scale_df(sleep_ios, c(
  "total_sleep_minutes", "total_night_screen_minutes", "screen_interruptions"
), "sleep_ios")


################################################################################
############### Merging Android and IOS + Grand Merge ##########################
################################################################################

names(screen_and)
names(screen_ios)
screen <- rbind(screen_and, screen_ios)

names(call_and)
names(call_ios)
call <- rbind(call_and, call_ios)

names(activity_and)
names(activity_ios)
activity <- rbind(activity_and, activity_ios)

names(gps_and)
names(gps_ios)
gps <- rbind(gps_and, gps_ios)

names(sleep_and)
names(sleep_ios)
sleep <- rbind(sleep_and, sleep_ios)


head(gps)
head(screen)
head(call)
head(activity)
head(sleep)


summary(gps)
summary(screen)
summary(call)
summary(sleep)
summary(activity)


# Step 1: Standardize column names (rename all date columns to "date")
gps <- gps %>% rename(date = measured_date)
screen <- screen %>% rename(date = measuredat)
call <- call %>% rename(date = date_local)
sleep <- sleep %>% rename(date = sleep_date)
activity <- activity %>% rename(date = activity_date)


# Step 2: Sort all dataframes by participantid and date
dfs <- list(gps, screen, call, sleep, activity)
dfs <- lapply(dfs, function(df) df %>% arrange(participantid, date))

# make sure there are no duplicate days per PID
gps %>%
  count(participantid, date) %>%
  filter(n > 1)

screen %>%
  count(participantid, date) %>%
  filter(n > 1)

call %>%
  count(participantid, date) %>%
  filter(n > 1)

sleep %>%
  count(participantid, date) %>%
  filter(n > 1)

activity %>%
  count(participantid, date) %>%
  filter(n > 1)


# Step 3: Prep for merge, to prevent column name collisions, add prefixes
names(dfs) <- c("gps", "screen", "call", "sleep", "activity")


# GRAND MERGE
dfs <- imap(dfs, function(df, dfname) {
  df %>%
    rename_with(~ paste0(dfname, "_", .x), .cols = -c(participantid, date))
})

# Reduce with full_join by participantid and date
merged_data <- reduce(dfs, full_join, by = c("participantid", "date"))

names(merged_data)
summary(merged_data)


################################################################################
################### Excluding column(s) to handle NA's ###########################
################################################################################

# Strategy: Count NAs per column
# Check how many rows become complete (no NA) when each column is removed
# Sort columns by how much their exclusion helps recover rows


# Step 1: Count how many NAs are in each column
na_counts <- colSums(is.na(merged_data))

# Step 2: For each column, compute how many rows would be complete if we removed just that column
na_impact <- sapply(names(merged_data), function(col) {
  temp <- merged_data[, setdiff(names(merged_data), col)]
  sum(complete.cases(temp))
})

# Step 3: Compare with current number of complete rows
n_complete_all <- sum(complete.cases(merged_data))

# Step 4: Create a summary data frame
na_summary <- data.frame(
  column = names(na_impact),
  na_count = na_counts[names(na_impact)],
  complete_rows_if_removed = na_impact,
  gain_vs_all_cols = na_impact - n_complete_all
)

# Step 5: Sort by most gain in complete rows
na_summary_sorted <- na_summary[order(-na_summary$gain_vs_all_cols), ]

# Keep only columns that are contributing to incompleteness
na_summary_filtered <- na_summary_sorted[na_summary_sorted$na_count > 0 & na_summary_sorted$gain_vs_all_cols > 0, ]

# View top contributors to NA-driven row exclusions
head(na_summary_filtered, 10)

# remove call_num_rejected_calls column

merged_data <- select(merged_data, -call_num_rejected_calls)


################################################################################
####################### Add the SDQ Subscale Scores to the Table ###############
################################################################################

SDQ <- read.csv("SDQ_SubscaleScores.csv")

SDQ <- select(SDQ, redcap_survey_identifier, emo_symptoms_baseline, hyperactivity_baseline)
head(SDQ)
summary(SDQ)

sdq_clean <- SDQ %>%
  mutate(participantid = tolower(redcap_survey_identifier)) 

# Step 2: Merge into merged_data
merged_data_final <- merged_data %>%
  left_join(sdq_clean, by = "participantid")


merged_final <- select(merged_data_final, -c(redcap_survey_identifier, gps_device_type, sleep_device_type, activity_weekday_local))
names(merged_final)

summary(merged_final)

data <- merged_final

################################################################################
############################# Complete Cases and Save  #########################
################################################################################

length(unique(data$participantid))

# remove all rows with NAs
data <- data[complete.cases(data), ]

# 259 participants with at least one dat of full data
length(unique(data$participantid))

clean_days_per_pid <- data %>%
  group_by(participantid) %>%
  summarize(days = n_distinct(date)) %>%
  filter(days >= 3)

clean_data <- data %>%
  filter(participantid %in% clean_days_per_pid$participantid)

# 165 participants with at least 3 days of full data
length(unique(clean_data$participantid))

summary(clean_data)

write.csv(clean_data, "SM_Preprocessed_Cleaned_May6.csv")






