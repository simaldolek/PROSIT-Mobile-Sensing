


# source functions.R script.
conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)


connect_to_prosit_database()

query <- "SELECT * FROM user1_workspace.feature_daily_gps_pyspark_20250818;"
gps <- query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_screen_android_timeofday_4parts_psql_20250818;"
screen_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_screen_ios_timeofday_4parts_psql_20250818;"
screen_ios <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_calls_android_psql_20250818;"
call_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_calls_ios_psql_20250818;"
call_ios <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_sleep_pyspark_20250818;"
sleep <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_accelerometer_android_timeofday_4parts_psql_20250;"
activity_and <-  query_database(query)

query <- "SELECT * FROM user1_workspace.feature_daily_activity_ios_timeofday_4parts_psql_20250818;"
activity_ios <-  query_database(query)

query <- "SELECT * FROM study_prositsm.device;"
device <-  query_database(query)

names(gps)
names(screen_and)
names(screen_ios)
names(call_and)
names(call_ios)
names(sleep)
names(activity_and)
names(activity_ios)



# minute to second conversions
head(sleep)

sleep$total_night_screen_seconds <- sleep$total_night_screen_minutes*60
sleep$total_inactive_seconds <- sleep$total_inactive_minutes*60
sleep <- dplyr::select(sleep, -c(total_inactive_minutes, total_night_screen_minutes))

head(sleep)
head(gps)
head(screen_and)
head(screen_ios)
head(call_and)
head(call_ios)
head(sleep)
head(activity_and)
head(activity_ios)



################################################################################
###################### Android vs. iOS Separation ##############################
################################################################################


android_ids <- unique(c(
  screen_and$participantid,
  call_and$participantid,
  activity_and$participantid
))


ios_ids <- unique(c(
  screen_ios$participantid,
  call_ios$participantid,
  activity_ios$participantid
))


# EXCLUDE PARTICIPANTS WHO SWITCH BETWEEN IOS AND ANDROID DEVICES
# android vs. ios lookup dataframe
android_df <- data.frame(participantid = android_ids, device_type = "android")
ios_df <- data.frame(participantid = ios_ids, device_type = "ios")
bad_ids <- intersect(android_ids, ios_ids)


gps <- gps[!gps$participantid %in% bad_ids, ]
sleep <- sleep[!sleep$participantid %in% bad_ids,]
screen_and <- screen_and[!screen_and$participantid %in% bad_ids, ]
screen_ios <- screen_ios[!screen_ios$participantid %in% bad_ids, ]
activity_and <- activity_and[!activity_and$participantid %in% bad_ids, ]
activity_ios <- activity_ios[!activity_ios$participantid %in% bad_ids, ]
call_and <- call_and[!call_and$participantid %in% bad_ids, ]
call_ios <- call_ios[!call_ios$participantid %in% bad_ids, ]


device_lookup <- unique(rbind(android_df, ios_df))

# add device type by PID to gps
gps$device_type <- NA
sleep$device_type <- NA

gps$device_type[gps$participantid %in% android_ids] <- "android"
gps$device_type[gps$participantid %in% ios_ids] <- "ios"

sleep$device_type[sleep$participantid %in% android_ids] <- "android"
sleep$device_type[sleep$participantid %in% ios_ids] <- "ios"

anyNA(gps$device_type)
anyNA(sleep$device_type)

gps %>% group_by(participantid) %>% filter(is.na(device_type))
sleep %>% group_by(participantid) %>% filter(is.na(device_type))

"prositsm6512" %in% device$participantid
device[device$participantid=="prositsm6512",]

# manually assign device type based on PSQL
sleep$device_type[sleep$participantid == "prositsm6512"] <- "ios"
gps$device_type[gps$participantid == "prositsm400"] <- "ios"

anyNA(sleep$device_type)
anyNA(gps$device_type)

gps_and <- gps %>% filter(device_type == "android")
gps_ios <- gps %>% filter(device_type == "ios")

table(gps_and$device_type)
table(gps_ios$device_type)

sleep_and <- sleep %>% filter(device_type == "android")
sleep_ios <- sleep %>% filter(device_type == "ios")

table(sleep_and$device_type)
table(sleep_ios$device_type)




################################################################################
#################### Fix Continuous Variable Types to Numeric #################
################################################################################

str(gps_and)

gps_and$total_haversine_meters <- as.numeric(gps_and$total_haversine_meters)
gps_and$location_entropy <- as.numeric(gps_and$location_entropy)
gps_and$location_variance <- as.numeric(gps_and$location_variance)
gps_and$radius_of_gyration_meters <- as.numeric(gps_and$radius_of_gyration_meters) 

str(gps_ios)

gps_ios$total_haversine_meters <- as.numeric(gps_ios$total_haversine_meters)
gps_ios$location_entropy <- as.numeric(gps_ios$location_entropy)
gps_ios$location_variance <- as.numeric(gps_ios$location_variance)
gps_ios$radius_of_gyration_meters <- as.numeric(gps_ios$radius_of_gyration_meters) 


str(call_and)
str(call_ios)
str(screen_and)
str(screen_ios)
str(sleep_and)
str(sleep_ios)
str(activity_and)
str(activity_ios)

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
  "duration_all_calls_seconds",
  "num_calls_made",
  "duration_calls_made_seconds",
  "num_calls_received",
  "duration_calls_received_seconds",
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
anyNA(sleep_and$total_night_screen_interruptions) # no NAs, we're good
anyNA(sleep_ios$total_night_screen_interruptions) # no NAs, we're good


# Columns to set 0 if total_screen_time not missing
cols_set_0_if_screentime <- c(
  "morning_screen_time_in_seconds",
  "afternoon_screen_time_in_seconds",
  "evening_screen_time_in_seconds",
  "nighttime_screen_time_in_seconds"
)

summary(screen_and[cols_set_0_if_screentime])
summary(screen_ios[cols_set_0_if_screentime])

for (col in cols_set_0_if_screentime) {
  idx <- is.na(screen_and[[col]]) & !is.na(screen_and$total_screen_time_in_seconds)
  screen_and[[col]][idx] <- 0
}

for (col in cols_set_0_if_screentime) {
  idx <- is.na(screen_ios[[col]]) & !is.na(screen_ios$total_screen_time_in_seconds)
  screen_ios[[col]][idx] <- 0
}

summary(screen_and[cols_set_0_if_screentime])
summary(screen_ios[cols_set_0_if_screentime])


# Columns to set 0 if screen_num_of_events_total not missing
cols_set_0_if_screennum <- c(
  "num_of_events_morning",
  "num_of_events_afternoon",
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
  "non_vigorous_pa_seconds",
  "vigorous_pa_seconds",
  "total_active_seconds"
)

summary(activity_and[cols_set_0_if_total_inactive])
summary(activity_ios[cols_set_0_if_total_inactive])
anyNA(activity_and[cols_set_0_if_total_inactive]) # no NA's, we good
anyNA(activity_ios[cols_set_0_if_total_inactive]) # no NA's, we good



# Columns to set 0 if activity_total_active_minutes not missing
cols_set_0_if_total_active <- c(
  "morning_pa_seconds",
  "afternoon_pa_seconds",
  "evening_pa_seconds"
)


summary(activity_and[cols_set_0_if_total_active])
summary(activity_ios[cols_set_0_if_total_active])
anyNA(activity_and[cols_set_0_if_total_active]) # no NA's, we good
anyNA(activity_ios[cols_set_0_if_total_active]) # no NA's, we good



################################################################################
###################### Detect Outliers + Examine ###############################
################################################################################


# Core: detect outliers in a SINGLE vector (auto-coerces to numeric)
detect_outliers <- function(x, na.rm = TRUE, digits = 3) {
  
  Q1 <- quantile(x, 0.25, na.rm = na.rm)
  Q3 <- quantile(x, 0.75, na.rm = na.rm)
  IQR <- Q3 - Q1
  lower <- Q1 - 1.5 * IQR
  upper <- Q3 + 1.5 * IQR
  
  lower_idx <- which(x < lower)
  upper_idx <- which(x > upper)
  idx <- which(x < lower | x > upper)
  
  cat(sprintf("Lower bound = %.*f | lower outliers: %d\n",digits, lower, length(lower_idx)))
  if (length(lower_idx)) {print(data.frame(index = lower_idx))}
  
  cat(sprintf("Upper bound = %.*f | upper outliers: %d\n",digits, upper, length(upper_idx)))
  if (length(upper_idx)) {print(data.frame(index = upper_idx))}
  
  return(idx)
}



# GPS ANDROID
gps_and_thm_o <- detect_outliers(gps_and$total_haversine_meters) # 1003 (only 25 above 2000000)
length(gps_and_thm_o)
hist(log(gps_and$total_haversine_meters[gps_and_thm_o]))

gps_and_nsp_o <- detect_outliers(gps_and$number_of_stay_points) # 311
length(gps_and_nsp_o)
hist(gps_and$number_of_stay_points)

gps_and_ttac_o <-detect_outliers(gps_and$total_time_at_clusters_seconds) # 3
length(gps_and_ttac_o)
hist(gps_and$total_time_at_clusters_seconds)

gps_and_le_o <-detect_outliers(gps_and$location_entropy) # 0
length(gps_and_le_o)
hist(gps_and$location_entropy)

gps_and_lv_o <-detect_outliers(gps_and$location_variance) # 537
length(gps_and_lv_o)
hist(log(gps_and$location_variance))

gps_and_rogm_o <-detect_outliers(gps_and$radius_of_gyration_meters) # 1301
length(gps_and_rogm_o)
hist(log(gps_and$radius_of_gyration_meters))

# no one can travel for more than 2000 kms per day, even with flights.. 
max_realistic <- 2000000
gps_and$total_haversine_meters <- ifelse(gps_and$total_haversine_meters > max_realistic, NA, gps_and$total_haversine_meters)



# GPS IOS
gps_ios_thm_o <- detect_outliers(gps_ios$total_haversine_meters) # 2008 (only 27 above 2000000)
length(gps_ios_thm_o)
hist(log(gps_ios$total_haversine_meters[gps_ios_thm_o]))

gps_ios_nsp_o <- detect_outliers(gps_ios$number_of_stay_points) # 1302
length(gps_ios_nsp_o)
hist(gps_ios$number_of_stay_points)

gps_ios_ttac_o <-detect_outliers(gps_ios$total_time_at_clusters_seconds) # 36
length(gps_ios_ttac_o)
hist(gps_ios$total_time_at_clusters_seconds)

gps_ios_le_o <-detect_outliers(gps_ios$location_entropy) # 167
length(gps_ios_le_o)
hist(gps_ios$location_entropy)

gps_ios_lv_o <-detect_outliers(gps_ios$location_variance) # 1521
length(gps_ios_lv_o)
hist(log(gps_ios$location_variance))

gps_ios_rogm_o <-detect_outliers(gps_ios$radius_of_gyration_meters) # 2052
length(gps_ios_rogm_o)
hist(log(gps_ios$radius_of_gyration_meters))

gps_ios$total_haversine_meters <- ifelse(gps_ios$total_haversine_meters > max_realistic, NA, gps_ios$total_haversine_meters)
sum(is.na(gps_ios$total_haversine_meters)) # 35 above 2000 km


# SCREEN ANDROID
screen_and_tstis_o <- detect_outliers(screen_and$total_screen_time_in_seconds) # 41
length(screen_and_tstis_o)
hist(screen_and$total_screen_time_in_seconds)
range(screen_and$total_screen_time_in_seconds) # anything more than 86400 secs (24 hrs) is impossible 

screen_and_nuet_o <- detect_outliers(screen_and$num_of_events_total) # 427
length(screen_and_nuet_o)
hist(screen_and$num_of_events_total)

screen_and_mstis_o <- detect_outliers(screen_and$morning_screen_time_in_seconds) # 1069
length(screen_and_mstis_o)
hist(screen_and$morning_screen_time_in_seconds) # anything more than 21600 sec (6hrs) is impossible as each timeslot is 6 hrs
range(screen_and$morning_screen_time_in_seconds)

screen_and_nuem_o <- detect_outliers(screen_and$num_of_events_morning) # 761
length(screen_and_nuem_o)
hist(screen_and$num_of_events_morning)

screen_and_astis_o <- detect_outliers(screen_and$afternoon_screen_time_in_seconds) # 363 (only 4 is above 21600)
length(screen_and_astis_o)
hist(screen_and$afternoon_screen_time_in_seconds) # anything more than 21600 sec (6hrs) is impossible as each timeslot is 6 hrs
range(screen_and$afternoon_screen_time_in_seconds)
screen_and$afternoon_screen_time_in_seconds <- ifelse(screen_and$afternoon_screen_time_in_seconds > 21600, NA, screen_and$afternoon_screen_time_in_seconds)

screen_and_noea_o <- detect_outliers(screen_and$num_of_events_afternoon) # 849
length(screen_and_noea_o)
hist(screen_and$num_of_events_afternoon)

screen_and_estis_o <- detect_outliers(screen_and$evening_screen_time_in_seconds) # 96
length(screen_and_estis_o)
hist(screen_and$evening_screen_time_in_seconds) # anything more than 21600 sec (6hrs) is impossible 
range(screen_and$evening_screen_time_in_seconds)

screen_and_noee_o <- detect_outliers(screen_and$num_of_events_evening) # 476
length(screen_and_noee_o)
hist(screen_and$num_of_events_evening)

screen_and_nstis_o <- detect_outliers(screen_and$nighttime_screen_time_in_seconds) # 142
length(screen_and_nstis_o)
hist(screen_and$nighttime_screen_time_in_seconds)
range(screen_and$nighttime_screen_time_in_seconds)

screen_and_noen_o <- detect_outliers(screen_and$num_of_events_nighttime) # 756
length(screen_and_noen_o)
hist(screen_and$num_of_events_nighttime)



# SCREEN IOS
screen_ios_tstis_o <- detect_outliers(screen_ios$total_screen_time_in_seconds) # 156
length(screen_ios_tstis_o)
hist(screen_ios$total_screen_time_in_seconds)
range(screen_ios$total_screen_time_in_seconds) # anything more than 86400 secs (24 hrs) is impossible 

screen_ios_nuet_o <- detect_outliers(screen_ios$num_of_events_total) # 491
length(screen_ios_nuet_o)
hist(screen_ios$num_of_events_total)

screen_ios_mstis_o <- detect_outliers(screen_ios$morning_screen_time_in_seconds) # 2276
length(screen_ios_mstis_o)
hist(screen_ios$morning_screen_time_in_seconds) # anything more than 21600 sec (6hrs) is impossible as each timeslot is 6 hrs
range(screen_ios$morning_screen_time_in_seconds)

screen_ios_nuem_o <- detect_outliers(screen_ios$num_of_events_morning) # 2144
length(screen_ios_nuem_o)
hist(screen_ios$num_of_events_morning)

screen_ios_astis_o <- detect_outliers(screen_ios$afternoon_screen_time_in_seconds) # 480
length(screen_ios_astis_o)
hist(screen_ios$afternoon_screen_time_in_seconds) 
range(screen_ios$afternoon_screen_time_in_seconds)

screen_ios_noea_o <- detect_outliers(screen_ios$num_of_events_afternoon) # 759
length(screen_ios_noea_o)
hist(screen_ios$num_of_events_afternoon)

screen_ios_estis_o <- detect_outliers(screen_ios$evening_screen_time_in_seconds) # 270
length(screen_ios_estis_o)
hist(screen_ios$evening_screen_time_in_seconds) # anything more than 21600 sec (6hrs) is impossible 
range(screen_ios$evening_screen_time_in_seconds)

screen_ios_noee_o <- detect_outliers(screen_ios$num_of_events_evening) # 591
length(screen_ios_noee_o)
hist(screen_ios$num_of_events_evening)

screen_ios_nstis_o <- detect_outliers(screen_ios$nighttime_screen_time_in_seconds) # 558 (2 is above 6 hrs)
length(screen_ios_nstis_o)
hist(screen_ios$nighttime_screen_time_in_seconds)
range(screen_ios$nighttime_screen_time_in_seconds, na.rm=TRUE)
screen_ios$nighttime_screen_time_in_seconds <- ifelse(screen_ios$nighttime_screen_time_in_seconds > 21600, NA, screen_ios$nighttime_screen_time_in_seconds)

screen_ios_noen_o <- detect_outliers(screen_ios$num_of_events_nighttime) # 1193
length(screen_ios_noen_o)
hist(screen_ios$num_of_events_nighttime)



# ACTIVITY ANDROID
activity_and_nvps_o <- detect_outliers(activity_and$non_vigorous_pa_seconds) # 1614
length(activity_and_nvps_o)
hist(log(activity_and$non_vigorous_pa_seconds))
range(activity_and$non_vigorous_pa_seconds)

activity_and_vps_o <- detect_outliers(activity_and$vigorous_pa_seconds) # 2029 (5 above 24 hrs)
length(activity_and_vps_o)
hist(log(activity_and$vigorous_pa_seconds))
range(activity_and$vigorous_pa_seconds,na.rm = TRUE) # can't be more than 24 hrs - 86400 secs
activity_and$vigorous_pa_seconds <- ifelse(activity_and$vigorous_pa_seconds > 86400, NA, activity_and$vigorous_pa_seconds)

activity_and_tas_o <- detect_outliers(activity_and$total_active_seconds) # 2016 (7 above 24 hrs)
length(activity_and_tas_o)
hist(log(activity_and$total_active_seconds))
range(activity_and$total_active_seconds,na.rm = TRUE)
activity_and$total_active_seconds <- ifelse(activity_and$total_active_seconds > 86400, NA, activity_and$total_active_seconds)

activity_and_pvad_o <- detect_outliers(activity_and$percentage_vigorous_active_daily) # 2034 (5 above 100)
length(activity_and_pvad_o)
hist(log(activity_and$percentage_vigorous_active_daily))
range(activity_and$percentage_vigorous_active_daily,na.rm = TRUE)
activity_and$percentage_vigorous_active_daily <- ifelse(activity_and$percentage_vigorous_active_daily > 100, NA, activity_and$percentage_vigorous_active_daily)

activity_and_mps_o <- detect_outliers(activity_and$morning_pa_seconds) # 2774 (12 above 6 hrs)
length(activity_and_mps_o)
hist(log(activity_and$morning_pa_seconds))
range(activity_and$morning_pa_seconds,na.rm = TRUE)
activity_and$morning_pa_seconds <- ifelse(activity_and$morning_pa_seconds > 21600, NA, activity_and$morning_pa_seconds)

activity_and_aps_o <- detect_outliers(activity_and$afternoon_pa_seconds) # 2776 (13 above 6 hrs)
length(activity_and_aps_o)
hist(log(activity_and$afternoon_pa_seconds))
range(activity_and$afternoon_pa_seconds,na.rm = TRUE)
activity_and$afternoon_pa_seconds <- ifelse(activity_and$afternoon_pa_seconds > 21600, NA, activity_and$afternoon_pa_seconds)

activity_and_eps_o <- detect_outliers(activity_and$evening_pa_seconds) # 2171 (6 above 6 hrs)
length(activity_and_eps_o)
hist(log(activity_and$evening_pa_seconds))
range(activity_and$evening_pa_seconds,na.rm = TRUE)
activity_and$evening_pa_seconds <- ifelse(activity_and$evening_pa_seconds > 21600, NA, activity_and$evening_pa_seconds)

activity_and_nps_o <- detect_outliers(activity_and$nighttime_pa_seconds) # 2920 (1 above 6 hrs)
length(activity_and_nps_o)
hist(log(activity_and$nighttime_pa_seconds))
range(activity_and$nighttime_pa_seconds,na.rm = TRUE)
activity_and$nighttime_pa_seconds <- ifelse(activity_and$nighttime_pa_seconds > 21600, NA, activity_and$nighttime_pa_seconds)

activity_and_aen_o <- detect_outliers(activity_and$avg_euclidean_norm) # 286
length(activity_and_aen_o)
hist(activity_and$avg_euclidean_norm)

activity_and_men_o <- detect_outliers(activity_and$max_euclidean_norm) # 74
length(activity_and_men_o)
hist(activity_and$max_euclidean_norm)

activity_and_av_o <- detect_outliers(activity_and$activity_variability) # 3099
length(activity_and_av_o)
hist(activity_and$activity_variability)



# ACTIVITY IOS
activity_ios_nvps_o <- detect_outliers(activity_ios$non_vigorous_pa_seconds) # 679 (2 is above 24 hrs)
length(activity_ios_nvps_o)
hist(log(activity_ios$non_vigorous_pa_seconds))
range(activity_ios$non_vigorous_pa_seconds)
activity_ios$non_vigorous_pa_seconds <- ifelse(activity_ios$non_vigorous_pa_seconds > 86400, NA, activity_ios$non_vigorous_pa_seconds)


activity_ios_vps_o <- detect_outliers(activity_ios$vigorous_pa_seconds) # 1625
length(activity_ios_vps_o)
hist(log(activity_ios$vigorous_pa_seconds))
range(activity_ios$vigorous_pa_seconds,na.rm = TRUE)

activity_ios_tas_o <- detect_outliers(activity_ios$total_active_seconds) # 750 (3 is above 24 hrs)
length(activity_ios_tas_o)
hist(log(activity_ios$total_active_seconds))
range(activity_ios$total_active_seconds,na.rm = TRUE)
activity_ios$total_active_seconds <- ifelse(activity_ios$total_active_seconds > 86400, NA, activity_ios$total_active_seconds)

activity_ios_pvad_o <- detect_outliers(activity_ios$percentage_vigorous_active_daily) # 1629
length(activity_ios_pvad_o)
hist(log(activity_ios$percentage_vigorous_active_daily))
range(activity_ios$percentage_vigorous_active_daily,na.rm = TRUE)

activity_ios_mps_o <- detect_outliers(activity_ios$morning_pa_seconds) # 2204 - 10 above 6 hrs
length(activity_ios_mps_o)
hist(log(activity_ios$morning_pa_seconds))
range(activity_ios$morning_pa_seconds,na.rm = TRUE)
activity_ios$morning_pa_seconds <- ifelse(activity_ios$morning_pa_seconds > 21600, NA, activity_ios$morning_pa_seconds)

activity_ios_aps_o <- detect_outliers(activity_ios$afternoon_pa_seconds) # 1391 (18 above 6 hrs)
length(activity_ios_aps_o)
hist(log(activity_ios$afternoon_pa_seconds))
range(activity_ios$afternoon_pa_seconds,na.rm = TRUE)
activity_ios$afternoon_pa_seconds <- ifelse(activity_ios$afternoon_pa_seconds > 21600, NA, activity_ios$afternoon_pa_seconds)

activity_ios_eps_o <- detect_outliers(activity_ios$evening_pa_seconds) # 1911 (14 above 6 hrs)
length(activity_ios_eps_o)
hist(log(activity_ios$evening_pa_seconds))
range(activity_ios$evening_pa_seconds,na.rm = TRUE)
activity_ios$evening_pa_seconds <- ifelse(activity_ios$evening_pa_seconds > 21600, NA, activity_ios$evening_pa_seconds)

activity_ios_nps_o <- detect_outliers(activity_ios$nighttime_pa_seconds) # 4202 
length(activity_ios_nps_o)
hist(log(activity_ios$nighttime_pa_seconds))
range(activity_ios$nighttime_pa_seconds,na.rm = TRUE)

activity_ios_aen_o <- detect_outliers(activity_ios$avg_euclidean_norm) # 1315
length(activity_ios_aen_o)
hist(activity_ios$avg_euclidean_norm)

activity_ios_men_o <- detect_outliers(activity_ios$max_euclidean_norm) # 422
length(activity_ios_men_o)
hist(activity_ios$max_euclidean_norm)

activity_ios_av_o <- detect_outliers(activity_ios$activity_variability) # 1452
length(activity_ios_av_o)
hist(activity_ios$activity_variability)



# CALL ANDROID
call_and_nac_o <- detect_outliers(call_and$num_all_calls) # 1092
length(call_and_nac_o)
hist(call_and$num_all_calls)

call_and_dacs_o <- detect_outliers(call_and$duration_all_calls_seconds) # 1234
length(call_and_dacs_o)
hist(log(call_and$duration_all_calls_seconds))
range(call_and$duration_all_calls_seconds)

call_and_ncm_o <- detect_outliers(call_and$num_calls_made) # 974
length(call_and_ncm_o)
hist(log(call_and$num_calls_made))

call_and_dcms_o <- detect_outliers(call_and$duration_calls_made_seconds) # 1342
length(call_and_dcms_o)
hist(log(call_and$duration_calls_made_seconds))
range(call_and$duration_calls_made_seconds)

call_and_ncr_o <- detect_outliers(call_and$num_calls_received) # 1025
length(call_and_ncr_o)
hist(log(call_and$num_calls_received))

call_and_dcrs_o <- detect_outliers(call_and$duration_calls_received_seconds) # 1423
length(call_and_dcrs_o)
hist(log(call_and$duration_calls_received_seconds))
range(call_and$duration_calls_received_seconds)

call_and_nmc_o <- detect_outliers(call_and$num_missed_calls) # 1049
length(call_and_nmc_o)
hist(log(call_and$num_missed_calls))

call_and_nrc_o <- detect_outliers(call_and$num_rejected_calls) # 1584
length(call_and_nrc_o)
hist(log(call_and$num_rejected_calls))



# CALL IOS
call_ios_nac_o <- detect_outliers(call_ios$num_all_calls) # 875
length(call_ios_nac_o)
hist(log(call_ios$num_all_calls))

call_ios_dacs_o <- detect_outliers(call_ios$duration_all_calls_seconds) # 1501
length(call_ios_dacs_o)
hist(log(call_ios$duration_all_calls_seconds))
range(call_ios$duration_all_calls_seconds)

call_ios_ncm_o <- detect_outliers(call_ios$num_calls_made) # 673
length(call_ios_ncm_o)
hist(log(call_ios$num_calls_made))

call_ios_dcms_o <- detect_outliers(call_ios$duration_calls_made_seconds) # 1732
length(call_ios_dcms_o)
hist(log(call_ios$duration_calls_made_seconds))
range(call_ios$duration_calls_made_seconds)

call_ios_ncr_o <- detect_outliers(call_ios$num_calls_received) # 472
length(call_ios_ncr_o)
hist(log(call_ios$num_calls_received))

call_ios_dcrs_o <- detect_outliers(call_ios$duration_calls_received_seconds) # 1699
length(call_ios_dcrs_o)
hist(log(call_ios$duration_calls_received_seconds))
range(call_ios$duration_calls_received_seconds)

call_ios_nmc_o <- detect_outliers(call_ios$num_missed_calls) # 1137
length(call_ios_nmc_o)
hist(log(call_ios$num_missed_calls))

call_ios_nrc_o <- detect_outliers(call_ios$num_rejected_calls) # 585
length(call_ios_nrc_o)
hist(log(call_ios$num_rejected_calls))



# SLEEP ANDROID
sleep_and_tnsi_o <- detect_outliers(sleep_and$total_night_screen_interruptions) # 735
length(sleep_and_tnsi_o)
hist(log(sleep_and$total_night_screen_interruptions))

sleep_and_tnss_o <- detect_outliers(sleep_and$total_night_screen_seconds) # 602
length(sleep_and_tnss_o)
hist(log(sleep_and$total_night_screen_seconds))
range(sleep_and$total_night_screen_seconds)

sleep_and_tis_o <- detect_outliers(sleep_and$total_inactive_seconds) # 602
length(sleep_and_tis_o)
hist(log(sleep_and$total_night_screen_seconds))
range(sleep_and$total_night_screen_seconds)



# SLEEP IOS
sleep_ios_tnsi_o <- detect_outliers(sleep_ios$total_night_screen_interruptions) # 815
length(sleep_ios_tnsi_o)
hist(log(sleep_ios$total_night_screen_interruptions))

sleep_ios_tnss_o <- detect_outliers(sleep_ios$total_night_screen_seconds) # 702
length(sleep_ios_tnss_o)
hist(log(sleep_ios$total_night_screen_seconds))
range(sleep_ios$total_night_screen_seconds)

sleep_ios_tis_o <- detect_outliers(sleep_ios$total_inactive_seconds) # 702
length(sleep_ios_tis_o)
hist(log(sleep_ios$total_night_screen_seconds))
range(sleep_ios$total_night_screen_seconds)



################################################################################
############### Decide Whether to Take the Log Systematically #################
################################################################################
# Handles skeweness

library(e1071)  

# checks the skeweness and mena/median ratio to decide whether we should take the log of a feature
need_log <- function(x, skew_cutoff = 1, mean_median_ratio = 2) {
  x <- as.numeric(x)
  if (any(x < 0, na.rm=TRUE)) return(FALSE)  # don't log negative
  sk <- e1071::skewness(x, na.rm=TRUE)
  mm <- mean(x, na.rm=TRUE) / median(x, na.rm=TRUE)
  return(sk > skew_cutoff | mm > mean_median_ratio)
}


huber_normalize <- function(z) {
  z[!is.finite(z)] <- NA
  
  h <- MASS::huber(z)
  (z - h$mu) / h$s
}



huber_normalize_fallback <- function(z) {
  z <- as.numeric(z)
  z[!is.finite(z)] <- NA
  if (all(is.na(z))) return(z)
  
  MAD <- mad(z, na.rm = TRUE)
  if (isTRUE(all.equal(MAD, 0))) {
    sc <- IQR(z, na.rm = TRUE) / 1.349
    if (!is.finite(sc) || sc == 0) sc <- sd(z, na.rm = TRUE)
    if (!is.finite(sc) || sc == 0) return(rep(0, length(z)))  # truly constant
    mu <- median(z, na.rm = TRUE)
    return((z - mu) / sc)
  }
  
  h <- MASS::huber(z)   # requires library(MASS)
  (z - h$mu) / h$s
}

# plots raw versus log values then the final normalized form
# to examine distributions
plot_raw_vs_log <- function(x, bins = 50) {
  feature_name <- deparse(substitute(x))  # capture variable name
  
  x <- x[!is.na(x)]  # drop NAs
  
  # avoid log(0) issues
  x_log   <- log1p(x)
  x_huber <- huber_normalize(x_log)
  
  op <- par(mfrow = c(1, 3))  # reserve 1 row, 3 columns
  on.exit(par(op))            # reset after plotting
  
  hist(x, breaks = bins,
       main = paste(feature_name),
       xlab = "Raw values", col = "skyblue")
  
  hist(x_log, breaks = bins,
       main = paste("- log1p"),
       xlab = "log(values)", col = "tomato")
  
  hist(x_huber, breaks = bins,
       main = paste( "- huber norm"),
       xlab = "norm(log values)", col = "forestgreen")
  
  
}




plot_no_log <- function(x, bins = 50) {
  feature_name <- deparse(substitute(x))  # capture variable name
  
  x <- x[!is.na(x)]  # drop NAs
  
  # avoid log(0) issues
  x_huber <- huber_normalize(x)
  x_log <- log1p(x)
  
  op <- par(mfrow = c(1, 3))  # reserve 1 row, 3 columns
  on.exit(par(op))            # reset after plotting
  
  hist(x, breaks = bins,
       main = paste(feature_name),
       xlab = "Raw values", col = "skyblue")
  
  hist(x_log, breaks = bins,
       main = paste("Log - NOT USED"),
       xlab = "log values", col = "tomato")
  
  hist(x_huber, breaks = bins,
       main = paste( "- huber norm"),
       xlab = "norm(raw values)", col = "forestgreen")
  
  
}


#Since many of your features (screen time, distances, PA seconds) have lots of zeros, 
# using log1p avoids infinities that would mess up plots and normalization.

sapply(gps_and, need_log)
plot_raw_vs_log(gps_and$total_haversine_meters)
plot_raw_vs_log(gps_and$number_of_stay_points)
plot_no_log(gps_and$total_time_at_clusters_seconds)
plot_no_log(gps_and$location_entropy)
plot_raw_vs_log(gps_and$location_variance)
plot_raw_vs_log(gps_and$radius_of_gyration_meters)

sapply(gps_ios, need_log)
plot_raw_vs_log(gps_ios$total_haversine_meters)
plot_raw_vs_log(gps_ios$number_of_stay_points)
plot_no_log(gps_and$total_time_at_clusters_seconds)
plot_no_log(gps_and$location_entropy)
plot_raw_vs_log(gps_and$location_variance)
plot_raw_vs_log(gps_and$radius_of_gyration_meters)

sapply(screen_and, need_log)
plot_no_log(screen_and$total_screen_time_in_seconds)
plot_raw_vs_log(screen_and$num_of_events_total)
plot_raw_vs_log(screen_and$morning_screen_time_in_seconds)
plot_raw_vs_log(screen_and$num_of_events_morning)
plot_raw_vs_log(screen_and$afternoon_screen_time_in_seconds)
plot_raw_vs_log(screen_and$num_of_events_afternoon)
plot_raw_vs_log(screen_and$evening_screen_time_in_seconds)
plot_raw_vs_log(screen_and$num_of_events_evening)
plot_raw_vs_log(screen_and$nighttime_screen_time_in_seconds)
plot_raw_vs_log(screen_and$num_of_events_nighttime)

sapply(screen_ios, need_log)
plot_no_log(screen_ios$total_screen_time_in_seconds)
plot_raw_vs_log(screen_ios$num_of_events_total)
plot_raw_vs_log(screen_ios$morning_screen_time_in_seconds)
plot_raw_vs_log(screen_ios$num_of_events_morning)
plot_raw_vs_log(screen_ios$afternoon_screen_time_in_seconds)
plot_raw_vs_log(screen_ios$num_of_events_afternoon)
plot_raw_vs_log(screen_ios$evening_screen_time_in_seconds)
plot_raw_vs_log(screen_ios$num_of_events_evening)
plot_raw_vs_log(screen_ios$nighttime_screen_time_in_seconds)
plot_raw_vs_log(screen_ios$num_of_events_nighttime)

sapply(call_and, need_log)
plot_raw_vs_log(call_and$num_all_calls)
plot_raw_vs_log(call_and$duration_all_calls_seconds)
plot_raw_vs_log(call_and$num_calls_made)
plot_raw_vs_log(call_and$duration_calls_made_seconds)
plot_raw_vs_log(call_and$num_calls_received)
plot_raw_vs_log(call_and$duration_calls_received_seconds)
plot_raw_vs_log(call_and$num_missed_calls)
plot_raw_vs_log(call_and$num_rejected_calls)


sapply(call_ios, need_log)
plot_raw_vs_log(call_ios$num_all_calls)
plot_raw_vs_log(call_ios$duration_all_calls_seconds)
plot_raw_vs_log(call_ios$num_calls_made)
plot_raw_vs_log(call_ios$duration_calls_made_seconds)
plot_raw_vs_log(call_ios$num_calls_received)
plot_raw_vs_log(call_ios$duration_calls_received_seconds)
plot_raw_vs_log(call_ios$num_missed_calls)
plot_raw_vs_log(call_ios$num_rejected_calls)

sapply(sleep_and, need_log)
plot_raw_vs_log(sleep_and$total_night_screen_interruptions)
plot_raw_vs_log(sleep_and$total_night_screen_seconds)
plot_no_log(sleep_and$total_inactive_seconds)

sapply(sleep_ios, need_log)
plot_raw_vs_log(sleep_ios$total_night_screen_interruptions)
plot_raw_vs_log(sleep_ios$total_night_screen_seconds)
plot_no_log(sleep_ios$total_inactive_seconds)

sapply(activity_and, need_log)
plot_raw_vs_log(activity_and$non_vigorous_pa_seconds)
plot_raw_vs_log(activity_and$vigorous_pa_seconds)
plot_raw_vs_log(activity_and$total_active_seconds)
plot_raw_vs_log(activity_and$percentage_vigorous_active_daily)
plot_raw_vs_log(activity_and$morning_pa_seconds)
plot_raw_vs_log(activity_and$afternoon_pa_seconds)
plot_raw_vs_log(activity_and$evening_pa_seconds)
plot_raw_vs_log(activity_and$nighttime_pa_seconds)
plot_raw_vs_log(activity_and$avg_euclidean_norm)
plot_raw_vs_log(activity_and$max_euclidean_norm)
plot_raw_vs_log(activity_and$activity_variability)

sapply(activity_ios, need_log)
plot_raw_vs_log(activity_ios$non_vigorous_pa_seconds)
plot_raw_vs_log(activity_ios$vigorous_pa_seconds)
plot_raw_vs_log(activity_ios$total_active_seconds)
plot_raw_vs_log(activity_ios$percentage_vigorous_active_daily)
plot_raw_vs_log(activity_ios$morning_pa_seconds)
plot_raw_vs_log(activity_ios$afternoon_pa_seconds)
plot_raw_vs_log(activity_ios$evening_pa_seconds)
plot_raw_vs_log(activity_ios$nighttime_pa_seconds)
plot_raw_vs_log(activity_ios$avg_euclidean_norm)
plot_raw_vs_log(activity_ios$max_euclidean_norm)
plot_raw_vs_log(activity_ios$activity_variability)


###########################################################
############### Log Transformations  ######################
###########################################################

# Will take the log of strictly positive and highly right skewed (mostly zeros) features first
# after systematically examining them

# --- Transform function as defined by the previıus section ---
transform_gps <- function(df) {
  # drop unwanted cols
  df <- subset(df, select = -c(location_variance, radius_of_gyration_meters))
  
  for (col in names(df)) {
    if (col %in% c("participantid", "date_utc", "date_local", "device_type")) {
      next  # skip identifiers and metadata
  
    } else {
      # log 
      new_col <- paste0(col, "_log")
      df[[new_col]] <- (log1p(df[[col]]))
    }
  }
  
  return(df)
}

gps_and <- transform_gps(gps_and)
gps_ios <- transform_gps(gps_ios)



transform_screen <- function(df) {
  
  for (col in names(df)) {
    if (col %in% c("participantid", "weekday_local", "date_local")) {
      next  # skip identifiers and metadata
    }
    
     else {
      # log 
      new_col <- paste0(col, "_log")
      df[[new_col]] <- (log1p(df[[col]]))
    }
  }
  
  return(df)
}

screen_and <- transform_screen(screen_and)
screen_ios <- transform_screen(screen_ios)



transform_call <- function(df) {
  # drop unwanted cols
  df <- subset(df, select = -c(num_missed_calls, num_rejected_calls))
  
  for (col in names(df)) {
    if (col %in% c("participantid", "weekday_local", "date_local")) {
      next  # skip identifiers and metadata
    }
    
    else {
      # log
      new_col <- paste0(col, "_log")
      df[[new_col]] <- (log1p(df[[col]]))
    }
  }
  
  return(df)
}

call_and <- transform_call(call_and)
call_ios <- transform_call(call_ios)


transform_sleep <- function(df) {
  
  for (col in names(df)) {
    if (col %in% c("participantid", "date_local", "device_type")) {
      next  # skip identifiers and metadata
    }
    else {
      # log 
      new_col <- paste0(col, "_log")
      df[[new_col]] <- (log1p(df[[col]]))
    }
  }
  
  return(df)
}

sleep_and <- transform_sleep(sleep_and)
sleep_ios <- transform_sleep(sleep_ios)



transform_activity <- function(df) {
  # drop unwanted cols
  df <- subset(df, select = -c(nighttime_pa_seconds))
  
  for (col in names(df)) {
    if (col %in% c("participantid", "date_local", "weekday_local")) {
      next  # skip identifiers and metadata
    }
  
    else {
      # log 
      new_col <- paste0(col, "_log")
      df[[new_col]] <- (log1p(df[[col]]))
    }
  }
  
  return(df)
}

activity_and <- transform_activity(activity_and)
activity_ios <- transform_activity(activity_ios)


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



# Sort all dataframes by participantid and date
dfs <- list(gps, screen, call, sleep, activity)
dfs <- lapply(dfs, function(df) df %>% arrange(participantid, date_local))

# make sure there are no duplicate days per PID
gps %>%
  count(participantid, date_local) %>%
  filter(n > 2)

screen %>%
  count(participantid, date_local) %>%
  filter(n > 2)

call %>%
  count(participantid, date_local) %>%
  filter(n > 2)

sleep %>%
  count(participantid, date_local) %>%
  filter(n > 2)

activity %>%
  count(participantid, date_local) %>%
  filter(n > 2)


# Step 3: Prep for merge, to prevent column name collisions, add prefixes
names(dfs) <- c("gps", "screen", "call", "sleep", "activity")


# GRAND MERGE
dfs <- imap(dfs, function(df, dfname) {
  df %>%
    rename_with(~ paste0(dfname, "_", .x), .cols = -c(participantid, date_local))
})

# Reduce with full_join by participantid and date
merged_data <- reduce(dfs, full_join, by = c("participantid", "date_local"))

names(merged_data)
summary(merged_data)




################################################################################
####################### Add the SDQ Subscale Scores to the Table ###############
################################################################################

SDQ <- read.csv("SDQ_SubscaleScores.csv")

SDQ <- dplyr::select(SDQ, redcap_survey_identifier, emo_symptoms_baseline, 
              hyperactivity_baseline,conduct_probs_baseline, peer_probs_baseline,
              prosocial_baseline, emo_symptoms_followup, hyperactivity_followup, conduct_probs_followup,
              peer_probs_followup, prosocial_followup
              )
head(SDQ)
summary(SDQ)

sdq_clean <- SDQ %>%
  mutate(participantid = tolower(redcap_survey_identifier)) 

# Step 2: Merge into merged_data
merged_data_final <- merged_data %>%
  left_join(sdq_clean, by = "participantid")


merged_final <- dplyr::select(merged_data_final, -c(redcap_survey_identifier, gps_device_type, sleep_device_type, activity_weekday_local))
names(merged_final)

summary(merged_final)

data <- merged_final

################################################################################
############################# Complete Cases and Save  #########################
################################################################################

length(unique(data$participantid))

# remove all rows with NAs
#data <- data[complete.cases(data), ]
data <- data[complete.cases(data[ , !(names(data) %in% 
                                        c("emo_symptoms_baseline", 
                                          "hyperactivity_baseline","conduct_probs_baseline", "peer_probs_baseline",
                                          "prosocial_baseline", "emo_symptoms_followup", "hyperactivity_followup", "conduct_probs_followup",
                                          "peer_probs_followup", "prosocial_followup"))]), ]


# participants with at least one day of full data
length(unique(data$participantid))

clean_days_per_pid <- data %>%
  group_by(participantid) %>%
  summarize(days = n_distinct(date_local)) %>%
  filter(days >= 5)

clean_data <- data %>%
  filter(participantid %in% clean_days_per_pid$participantid)

# 448 participants with at least 5 days of full data
length(unique(clean_data$participantid))

summary(clean_data)

write.csv(clean_data, "SMMS_5days_SD_Aug20.csv")




