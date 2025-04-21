
# source functions.R script.
conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)


# features only: 
# normalize ios vs android separately 
# outlier detection - 38D


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

length(is.na(gps_and$device_type))
length(is.na(gps_ios$device_type))
length(is.na(gps$device_type))




gpsand_count <- gps_and %>% count(participantid, measured_date)
gpsios_count <- gps_ios %>% count(participantid, measured_date)
screenand_count <- screen_and %>% count(participantid,measuredat)
screenios_count <- screen_ios %>% count(participantid,measuredat)
calland_count <- call_and %>% count(participantid, date_local)
callios_count <- call_ios %>% count(participantid, date_local)
sleepand_count <- sleep_and %>% count(participantid, sleep_date)
sleepios_count <- sleep_ios %>% count(participantid, sleep_date)
activityand_count <- activity_and %>% count(participantid, activity_date)
activityios_count <- activity_ios %>% count(participantid, activity_date)

nrow(gpsand_count)
nrow(gpsios_count)
nrow(screenand_count)
nrow(screenios_count)
nrow(calland_count)
nrow(callios_count)
nrow(sleepand_count)
nrow(sleepios_count)
nrow(activityand_count)
nrow(activityios_count)


################################################################################
####################### Normalization of Features ##############################
################################################################################

# min-max normalization function
#normalize_minmax <- function(x) {
#  rng <- range(x, na.rm = TRUE)
#  if (diff(rng) == 0) return(rep(0, length(x)))  # if division by zero, assign zero
#  (x - rng[1]) / diff(rng)
#}



gps_and$total_haversine <- scale(gps_and$total_haversine)
gps_and$number_of_stay_points <- scale(gps_and$number_of_stay_points)
gps_and$total_time_at_clusters <- scale(gps_and$total_time_at_clusters)
gps_and$location_entropy <- scale(gps_and$location_entropy)
gps_and$location_variance[is.infinite(gps_and$location_variance)] <- NA
gps_and$location_variance <- scale(gps_and$location_variance)

summary(gps_and)


gps_ios$total_haversine <- scale(gps_ios$total_haversine)
gps_ios$number_of_stay_points <- scale(gps_ios$number_of_stay_points)
gps_ios$total_time_at_clusters <- scale(gps_ios$total_time_at_clusters)
gps_ios$location_entropy <- scale(gps_ios$location_entropy)
gps_ios$location_variance[is.infinite(gps_ios$location_variance)] <- NA
gps_ios$location_variance <- scale(gps_ios$location_variance)

summary(gps_ios)


screen_and$total_screen_time <- scale(screen_and$total_screen_time)
screen_and$num_of_events_total <- scale(screen_and$num_of_events_total)
screen_and$daytime_screen_time <- scale(screen_and$daytime_screen_time)
screen_and$num_of_events_daytime <- scale(screen_and$num_of_events_daytime)
screen_and$evening_screen_time <- scale(screen_and$evening_screen_time)
screen_and$num_of_events_evening <- scale(screen_and$num_of_events_evening)
screen_and$nighttime_screen_time <- scale(screen_and$nighttime_screen_time)
screen_and$num_of_events_nighttime <- scale(screen_and$num_of_events_nighttime)

summary(screen_and)


screen_ios$total_screen_time <- scale(screen_ios$total_screen_time)
screen_ios$num_of_events_total <- scale(screen_ios$num_of_events_total)
screen_ios$daytime_screen_time <- scale(screen_ios$daytime_screen_time)
screen_ios$num_of_events_daytime <- scale(screen_ios$num_of_events_daytime)
screen_ios$evening_screen_time <- scale(screen_ios$evening_screen_time)
screen_ios$num_of_events_evening <- scale(screen_ios$num_of_events_evening)
screen_ios$nighttime_screen_time <- scale(screen_ios$nighttime_screen_time)
screen_ios$num_of_events_nighttime <- scale(screen_ios$num_of_events_nighttime)

summary(screen_and)


activity_and$non_vigorous_pa_minutes <- scale(activity_and$non_vigorous_pa_minutes)
activity_and$vigorous_pa_minutes <- scale(activity_and$vigorous_pa_minutes)
activity_and$total_active_minutes <- scale(activity_and$total_active_minutes)
activity_and$total_inactive_minutes <- scale(activity_and$total_inactive_minutes)
activity_and$percent_sedentary <- scale(activity_and$percent_sedentary)
activity_and$day_minutes <- scale(activity_and$day_minutes)  
activity_and$evening_minutes <- scale(activity_and$evening_minutes)  
activity_and$night_minutes <- scale(activity_and$night_minutes)
activity_and$avg_euclidean_norm <- scale(activity_and$avg_euclidean_norm) 
activity_and$max_euclidean_norm <- scale(activity_and$max_euclidean_norm)
activity_and$activity_variability <- scale(activity_and$activity_variability) 

summary(activity_and)


activity_ios$non_vigorous_pa_minutes <- scale(activity_ios$non_vigorous_pa_minutes)
activity_ios$vigorous_pa_minutes <- scale(activity_ios$vigorous_pa_minutes)
activity_ios$total_active_minutes <- scale(activity_ios$total_active_minutes)
activity_ios$total_inactive_minutes <- scale(activity_ios$total_inactive_minutes)
activity_ios$percent_sedentary <- scale(activity_ios$percent_sedentary)
activity_ios$day_minutes <- scale(activity_ios$day_minutes)  
activity_ios$evening_minutes <- scale(activity_ios$evening_minutes)  
activity_ios$night_minutes <- scale(activity_ios$night_minutes)
activity_ios$avg_euclidean_norm <- scale(activity_ios$avg_euclidean_norm) 
activity_ios$max_euclidean_norm <- scale(activity_ios$max_euclidean_norm)
activity_ios$activity_variability <- scale(activity_ios$activity_variability) 

summary(activity_ios)


call_and$num_all_calls <- scale(call_and$num_all_calls)
call_and$duration_all_calls <- scale(call_and$duration_all_calls)
call_and$num_call_made <- scale(call_and$num_call_made)
call_and$duration_calls_made <- scale(call_and$duration_calls_made)
call_and$num_calls_received <- scale(call_and$num_calls_received)    
call_and$duration_calls_received <- scale(call_and$duration_calls_received)
call_and$num_missed_calls <- scale(call_and$num_missed_calls)
call_and$num_rejected_calls <- scale(call_and$num_rejected_calls)   

summary(call_and)
  
 
call_ios$num_all_calls <- scale(call_ios$num_all_calls)
call_ios$duration_all_calls <- scale(call_ios$duration_all_calls)
call_ios$num_call_made <- scale(call_ios$num_call_made)
call_ios$duration_calls_made <- scale(call_ios$duration_calls_made)
call_ios$num_calls_received <- scale(call_ios$num_calls_received)    
call_ios$duration_calls_received <- scale(call_ios$duration_calls_received)
call_ios$num_missed_calls <- scale(call_ios$num_missed_calls)
call_ios$num_rejected_calls <- scale(call_ios$num_rejected_calls) 
 
summary(call_ios)


sleep_and$total_sleep_minutes <- scale(sleep_and$total_sleep_minutes)
sleep_and$total_night_screen_minutes <- scale(sleep_and$total_night_screen_minutes)
sleep_and$screen_interruptions <- scale(sleep_and$screen_interruptions)

summary(sleep_and)  
  
  
sleep_ios$total_sleep_minutes <- scale(sleep_ios$total_sleep_minutes)
sleep_ios$total_night_screen_minutes <- scale(sleep_ios$total_night_screen_minutes)
sleep_ios$screen_interruptions <- scale(sleep_ios$screen_interruptions)

summary(sleep_ios) 




################################################################################
####################### Merging All Features into a Table ######################
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


# Step 3: Merge all dataframes using full_join recursively
# To prevent column name collisions, add prefixes
names(dfs) <- c("gps", "screen", "call", "sleep", "activity")

dfs <- imap(dfs, function(df, dfname) {
  df %>%
    rename_with(~ paste0(dfname, "_", .x), .cols = -c(participantid, date))
})


# Step 4: Reduce with full_join by participantid and date
merged_data <- reduce(dfs, full_join, by = c("participantid", "date"))

#View(merged_data)

names(merged_data)
# View the result
head(merged_data)
summary(merged_data)


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

merged_data_final <- select(merged_data_final, -redcap_survey_identifier)

#View(merged_data_final)

summary(merged_data_final)





################################################################################
############################# Winsorize the Outliers ###########################
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


features_to_winsorize <- c(
  "gps_total_haversine", "gps_number_of_stay_points", "gps_total_time_at_clusters",
  "gps_location_entropy", "gps_location_variance",
  "screen_total_screen_time", "screen_num_of_events_total", "screen_daytime_screen_time",
  "screen_num_of_events_daytime", "screen_evening_screen_time", "screen_num_of_events_evening",
  "screen_nighttime_screen_time", "screen_num_of_events_nighttime",
  "call_num_all_calls", "call_duration_all_calls", "call_num_call_made", "call_duration_calls_made",
  "call_num_calls_received", "call_duration_calls_received", "call_num_missed_calls", "call_num_rejected_calls",
  "sleep_total_sleep_minutes", "sleep_total_night_screen_minutes", "sleep_screen_interruptions",
  "activity_non_vigorous_pa_minutes", "activity_vigorous_pa_minutes", "activity_total_active_minutes",
  "activity_total_inactive_minutes", "activity_percent_sedentary", "activity_day_minutes",
  "activity_evening_minutes", "activity_night_minutes", "activity_avg_euclidean_norm",
  "activity_max_euclidean_norm", "activity_activity_variability"
)


# to store clipped counts
clipped_counts <- list()

# Apply the winsorization and extract both values and counts
for (col in features_to_winsorize) {
  result <- winsorize_col_verbose(merged_data_final[[col]])
  merged_data_final[[col]] <- result$values
  clipped_counts[[col]] <- result$clipped
}

# View how many values were clipped per column
clipped_counts_df <- data.frame(
  feature = names(clipped_counts),
  n_clipped = unlist(clipped_counts)
)

print(clipped_counts_df)

merged_final<- select(merged_data_final, -c(gps_device_type, sleep_device_type, activity_weekday_local))
names(merged_final)

summary(merged_final)

write.csv(merged_final, "SM_TabularData_Apr21.csv")

data <- merged_final

################################################################################
############################# Handling Missing Values ##########################
################################################################################

na_summary <- sapply(data[c(features_to_winsorize, "emo_symptoms_baseline", "hyperactivity_baseline")], 
                     function(col) sum(is.na(col)))
na_summary

# remove all rows with NAs
clean_data <- data %>%
  filter(if_all(.cols = all_of(c(features_to_winsorize, "emo_symptoms_baseline", "hyperactivity_baseline")), ~ !is.na(.)))

length(unique(clean_data$participantid))

clean_days_per_pid <- clean_data %>%
  group_by(participantid) %>%
  summarize(days = n_distinct(date)) %>%
  filter(days >= 5)

clean_data <- clean_data %>%
  filter(participantid %in% clean_days_per_pid$participantid)

length(unique(clean_data$participantid))

summary(clean_data)


write.csv(clean_data, "SM_CleanTabularData_Apr21.csv")

max(clean_days_per_pid$days)





