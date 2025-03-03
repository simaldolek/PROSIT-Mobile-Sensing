#===============================================================================
# Script Name:     screenTimeCleaning.R
# Author:          Simal Dolek
# Created Date:    2025-02-24
# Last Modified:   2025-02-23
# Description:     
# 
# Usage:           
#===============================================================================
library(splitstackshape)

# source functions.R script.
conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)

connect_to_prosit_database()

# extract the lock state data (iOS)
query <- "SELECT * FROM study_prositsm.lock_state ORDER BY measuredat ASC;"
LockStateiOS <- query_database(query)

# extract the analytics data (iOS)
query <- "SELECT * FROM study_prositsm.analytics ORDER BY measuredat ASC;"
AnalyticsDataiOS <- query_database(query)

# extract the power state data (android)
query <- "SELECT * FROM study_prositsm.powerstate ORDER BY measuredat ASC;"
PowerStateAnd <- query_database(query)

# extract the timezone lookup table
query <- "SELECT * FROM study_prositsm.lookup_timezone;"
LookUpTimezone<- query_database(query)

################################################################################
################################## iOS #########################################
################################################################################

# filter out the "Terminating" values from analytics
terminating <- filter(AnalyticsDataiOS, value0 == "Terminating")

# attach analytics data frame at the end of lock state data frame
LockStateiOS <- rbind(LockStateiOS, terminating)

# remove the duplicated rows from the data frame
LockStateiOS <- LockStateiOS[!duplicated(LockStateiOS[c(1:5)]), ]

# order rows by participant IDs and measured at time stamps
LockStateiOS <- LockStateiOS[with(LockStateiOS, order(participantid, measuredat)),]

# split the values in measuredat into dates and time stamps as YYYY-MM-DD HH-MM-SS
timecolumns= cSplit(LockStateiOS, "measuredat", "T", direction = "wide", fixed = TRUE,
                    drop = TRUE, stripWhite = TRUE, makeEqual = NULL,
                    type.convert = TRUE)

# update the measuredat values in the original data frame as above
LockStateiOS<-as.data.frame(timecolumns)

# convert time stamps into seconds
IOSTimes <- LockStateiOS$measuredat_1
lo<-strptime(IOSTimes, "%Y-%m-%d %H:%M:%S")
LockStateiOS$time <- (hour(lo) * 3600) + (minute(lo) * 60) + second(lo)

# calculate the number of days passed since the origin til measuredat_1 
LockStateiOS$DateNumber <- as.numeric(as.Date(LockStateiOS$measuredat_1, origin="1970-01-01"))

# convert the number of days passed since the origin into seconds, store in Date2 column
LockStateiOS$Date2 <- (LockStateiOS$DateNumber*24*60*60)

# add the seconds passed til the origin til the measureat day, with the seconds passed in the specific measureadat day
LockStateiOS$dateinsec<-LockStateiOS$Date2+LockStateiOS$time
head(LockStateiOS)


# check the number of LOCKED, UNLOCKED and Terminating values (generally locked-unlocked is not equal but should be)
table(LockStateiOS$value0)


# every time "UNLOCKED" is followed by "LOCKED" (and the values belong to the same participant),
# put the dateinsec value of the LOCKED row on the UNLOCKED row under a new column called "correct"
LockStateiOS$correct<-ifelse(LockStateiOS$value == "UNLOCKED" & lead(LockStateiOS$value == "LOCKED") & LockStateiOS$participantid == lead(LockStateiOS$participantid), lead(LockStateiOS$dateinsec),0)


# calculate the screen time by subtracting UNLOCKED time from the LOCKED time, store under the new column "timescreen"
LockStateiOS$timescreen <-ifelse(LockStateiOS$value == "UNLOCKED" & LockStateiOS$correct > 0, LockStateiOS$correct-LockStateiOS$dateinsec,0)


# summarize time screen results (CAUTION: this includes a bunch of zero values that didn't count as correct episodes)
# expect a median of zero and a lower mean than the actual mean
summary(LockStateiOS$timescreen)

# filter out any episode duration longer than 10 hrs
test<-filter(LockStateiOS, timescreen  >36000 )

# create a table of participantid counts that belong to people with 10+ hrs of screen time
participantid_counts <- table(test$participantid)

# Extract participantids which consistently show more than 10 hrs of screen time
# (people who have more than 10 episodes wherein they have 10+ hrs of screen time)
problematicParticipants <- names(participantid_counts[participantid_counts > 10])

# View the list of problematic participants
print(problematicParticipants)

# remove the problematic participants from the original data frame 
LockStateiOS <- LockStateiOS %>%
  filter(!participantid %in% problematicParticipants)


# only keep the time screens that are smaller than or equal to 10 hrs and update the data set accordingly
LockStateiOS <-filter(LockStateiOS, timescreen  <= 36000 )

summary(LockStateiOS$timescreen)

# filter out the incorrect episodes (these are episodes that are 0 seconds long)
LockStateiOS <- filter(LockStateiOS, timescreen != 0)



################################################################################
############### Import Local Timzeones from Timezone_Lookup Table ##############
################################################################################

# Convert datetime columns to POSIXct for comparison
LockStateiOS <- LockStateiOS %>%
  mutate(measuredat_1 = as.POSIXct(measuredat_1, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))

LookUpTimezone <- LookUpTimezone %>%
  mutate(
    start_time = as.POSIXct(start_time, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    end_time = as.POSIXct(end_time, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
  )

# Apply function to LockStateiOS
LockStateiOS <- LockStateiOS %>%
  rowwise() %>%
  mutate(timezone = assign_timezone(participantid, measuredat_1)) %>%
  ungroup()


# Compute timezone offset based on timezones
# save under a new column "timezone_difference"
LockStateiOS$timezone_difference <- sapply(1:nrow(LockStateiOS), function(i) {
  tz <- LockStateiOS$timezone[i]
  dt <- LockStateiOS$measuredat_1[i]
  
  if (is.na(tz)) {
    return(NA_real_)  # NA timezones are automatically NA
  }
  
  return(tz_offset(as.POSIXct(dt), tz = tz)$utc_offset_h)
})


# Calculate measuredat_local by adding computed offset to UTC
LockStateiOS <- LockStateiOS %>%
  mutate(measuredat_local = measuredat_1 + as.difftime(timezone_difference, units = "hours"))


# keep only necessary rows
cleanLockStateiOS <- LockStateiOS %>%
  select(participantid, measuredat_1, DateNumber, measuredat_local, timescreen)

################################################################################
########################### Extract Final Features #############################
################################################################################


# Extract hour
cleanLockStateiOS <- cleanLockStateiOS %>% 
  mutate(hour = as.numeric(format(measuredat_local, "%H")))

# Create three separate dataframes based on time of day
daytime <-cleanLockStateiOS %>% filter(hour >= 7 & hour < 17)
evening <- cleanLockStateiOS %>% filter(hour >= 17 & hour < 23)
nighttime <- cleanLockStateiOS %>% filter(hour >= 23 | hour < 7)


# aggregate the total, daytime, evening,and nighttime timescreen for each participant per day
totalScreenTime <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateiOS, sum)
daytimeScreenTime <- aggregate(timescreen ~ DateNumber+participantid, daytime, sum)
eveningScreenTime <- aggregate(timescreen ~ DateNumber+participantid, evening, sum)
nighttimeScreenTime <- aggregate(timescreen ~ DateNumber+participantid, nighttime, sum)

# calculate the number of times screens were unlocked per day
NumberOfEvents <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateiOS, length)
NumberOfEvents <- NumberOfEvents %>% rename(numOfEvents = timescreen)

NumberOfEvents_daytime <- aggregate(timescreen ~ DateNumber+participantid, daytime, length)
NumberOfEvents_daytime <- NumberOfEvents_daytime %>% rename(numOfEvents_daytime = timescreen)

NumberOfEvents_evening <- aggregate(timescreen ~ DateNumber+participantid, evening, length)
NumberOfEvents_evening <- NumberOfEvents_evening %>% rename(numOfEvents_evening = timescreen)

NumberOfEvents_nighttime <- aggregate(timescreen ~ DateNumber+participantid, nighttime, length)
NumberOfEvents_nighttime <- NumberOfEvents_nighttime %>% rename(numOfEvents_nighttime = timescreen)



# combine screen time and number of unlocks/events in on dataframe 
totalFeaturesiOS <- cbind(totalScreenTime,NumberOfEvents$numOfEvents)
daytimeFeaturesiOS <- cbind(daytimeScreenTime,NumberOfEvents_daytime$numOfEvents_daytime)
eveningFeaturesiOS <- cbind(eveningScreenTime,NumberOfEvents_evening$numOfEvents_evening)
nighttimeFeaturesiOS <- cbind(nighttimeScreenTime,NumberOfEvents_nighttime$numOfEvents_nighttime)



#convert DateNumber back into Dates and remove DateNumber columns
totalFeaturesiOS$measuredat <- as.Date(totalFeaturesiOS$DateNumber, origin = "1970-01-01")
totalFeaturesiOS <- select(totalFeaturesiOS, -DateNumber)

daytimeFeaturesiOS$measuredat <- as.Date(daytimeFeaturesiOS$DateNumber, origin = "1970-01-01")
daytimeFeaturesiOS <- select(daytimeFeaturesiOS, -DateNumber)

eveningFeaturesiOS$measuredat <- as.Date(eveningFeaturesiOS$DateNumber, origin = "1970-01-01")
eveningFeaturesiOS <- select(eveningFeaturesiOS, -DateNumber)

nighttimeFeaturesiOS$measuredat <- as.Date(nighttimeFeaturesiOS$DateNumber, origin = "1970-01-01")
nighttimeFeaturesiOS <- select(nighttimeFeaturesiOS, -DateNumber)



# rename columns for clarity
totalFeaturesiOS <- totalFeaturesiOS %>%
  rename(numOfEvents_total = `NumberOfEvents$numOfEvents`,  # Rename 'numOfEvents' to 'NumberOfEvents'
         totalScreenTime = timescreen)  # Rename 'totalScreenTime' to 'timescreen'

head(totalFeaturesiOS)


daytimeFeaturesiOS <- daytimeFeaturesiOS %>%
  rename(numOfEvents_daytime = `NumberOfEvents_daytime$numOfEvents_daytime`,  
         daytimeScreenTime = timescreen)  

head(daytimeFeaturesiOS)


eveningFeaturesiOS <- eveningFeaturesiOS %>%
  rename(numOfEvents_evening = `NumberOfEvents_evening$numOfEvents_evening`,  
         eveningScreenTime = timescreen)  

head(eveningFeaturesiOS)


nighttimeFeaturesiOS <- nighttimeFeaturesiOS %>%
  rename(numOfEvents_nighttime = `NumberOfEvents_nighttime$numOfEvents_nighttime`,  
         nighttimeScreenTime = timescreen)  

head(nighttimeFeaturesiOS)


################################################################################

# Because we filtered rows based on hours when computing daytime, evening, and 
# nighttime features, there are distinct number of rows corresponding to unique
# 'participantid' - 'measured_date' combinations. In order to keep everything 
# aligned (as we are working with identical PIDs and dates across different 
# screentime features) we will match the number of rows for
# each feature, with values = NA for newly added rows.

################################################################################


# Extract unique participantid-measured_date combinations
total_combos <- totalFeaturesiOS %>% 
  select(participantid, measuredat) %>% 
  distinct()

daytime_combos <- daytimeFeaturesiOS %>% 
  select(participantid, measuredat) %>% 
  distinct()

evening_combos <- eveningFeaturesiOS %>% 
  select(participantid, measuredat) %>% 
  distinct()

nighttime_combos <- nighttimeFeaturesiOS %>% 
  select(participantid, measuredat) %>% 
  distinct()

# Convert to vectors
total_vector <- paste(total_combos$participantid, total_combos$measuredat, sep = "_")
daytime_vector <- paste(daytime_combos$participantid, daytime_combos$measuredat, sep = "_")
evening_vector <- paste(evening_combos$participantid, evening_combos$measuredat, sep = "_")
nighttime_vector <- paste(nighttime_combos$participantid, nighttime_combos$measuredat, sep = "_")

# Find missing combinations
missing_combos_daytime <- setdiff(total_vector, daytime_vector)
missing_combos_evening <- setdiff(total_vector, evening_vector)
missing_combos_nighttime <- setdiff(total_vector, nighttime_vector)

# Convert missing combos back to a data frame
missing_combos_df_daytime <- data.frame(
  participantid = sub("_.*", "", missing_combos_daytime),
  measuredat = as.Date(sub(".*_", "", missing_combos_daytime)),
  daytimeScreenTime = NA,
  numOfEvents_daytime = NA
)

missing_combos_df_evening <- data.frame(
  participantid = sub("_.*", "", missing_combos_evening),
  measuredat = as.Date(sub(".*_", "", missing_combos_evening)),
  eveningScreenTime = NA,
  numOfEvents_evening = NA
)

missing_combos_df_nighttime <- data.frame(
  participantid = sub("_.*", "", missing_combos_nighttime),
  measuredat = as.Date(sub(".*_", "", missing_combos_nighttime)),
  nighttimeScreenTime = NA,
  numOfEvents_nighttime = NA
)



# arrange the column order to match each other before rowbinding 
daytimeFeaturesiOS <- daytimeFeaturesiOS %>%
  select(participantid, measuredat, everything())

eveningFeaturesiOS <- eveningFeaturesiOS %>%
  select(participantid, measuredat, everything())

nighttimeFeaturesiOS <- nighttimeFeaturesiOS %>%
  select(participantid, measuredat, everything())

totalFeaturesiOS <- totalFeaturesiOS %>%
  select(participantid, measuredat, everything())


# Append missing rows
daytimeFeaturesiOS <- rbind(daytimeFeaturesiOS, missing_combos_df_daytime)
eveningFeaturesiOS <- rbind(eveningFeaturesiOS, missing_combos_df_evening)
nighttimeFeaturesiOS <- rbind(nighttimeFeaturesiOS, missing_combos_df_nighttime)


# Order by PID and measured_date
daytimeFeaturesiOS <- daytimeFeaturesiOS %>% arrange(participantid, measuredat)
eveningFeaturesiOS <- eveningFeaturesiOS %>% arrange(participantid, measuredat)
nighttimeFeaturesiOS <- nighttimeFeaturesiOS %>% arrange(participantid, measuredat)


# Merge all features into a single dataframe
finalScreenTimeFeaturesiOS <- reduce(list(totalFeaturesiOS, daytimeFeaturesiOS, eveningFeaturesiOS, nighttimeFeaturesiOS), 
                        full_join, by = c("participantid", "measuredat"))



##############################################################################
######## save the screen time features in the PSQL database ##################
##############################################################################

# Rename columns in the dataframe to match SQL table column names
colnames(finalScreenTimeFeaturesiOS) <- c(
  "participantid", 
  "measuredat", 
  "total_screen_time", 
  "num_of_events_total", 
  "daytime_screen_time", 
  "num_of_events_daytime", 
  "evening_screen_time", 
  "num_of_events_evening", 
  "nighttime_screen_time", 
  "num_of_events_nighttime"
)

# Check the structure to confirm changes
str(finalScreenTimeFeaturesiOS)


append_to_db(finalScreenTimeFeaturesiOS, "user1_workspace", "daily_screen_features_ios")






################################################################################
############################# Android Cleaning #################################
################################################################################

# remove the duplicate values 
PowerStateAnd <- PowerStateAnd[!duplicated(PowerStateAnd[c(2,3,4,5)]), ]

# order the data frame by participant ID and measureat values
PowerStateAnd <- PowerStateAnd[with(PowerStateAnd, order(participantid, measuredat)),]

# convert the measuredat values to date format 
timecolumns= cSplit(PowerStateAnd, "measuredat", "T", direction = "wide", fixed = TRUE,
                    drop = TRUE, stripWhite = TRUE, makeEqual = NULL,
                    type.convert = TRUE)

PowerStateAnd<-as.data.frame(timecolumns)
head(PowerStateAnd)


# convert the time stamps into seconds
AndTimes <- PowerStateAnd$measuredat_1
lo<-strptime(AndTimes, "%Y-%m-%d %H:%M:%S")
PowerStateAnd$time <- (hour(lo) * 3600) + (minute(lo) * 60) + second(lo)
head(PowerStateAnd)

# calculate date number since origin, date2 (in secs) since the origin, and dateinsec which is the exact measureat from the origin in secs
PowerStateAnd$DateNumber <- as.numeric(as.Date(PowerStateAnd$measuredat_1))
PowerStateAnd$Date2 <- (PowerStateAnd$DateNumber*24*60*60)
PowerStateAnd$dateinsec<-PowerStateAnd$Date2+PowerStateAnd$time
head(PowerStateAnd)


# use the same logic we used for iOS to detect correct episodes
PowerStateAnd$correct<-ifelse(PowerStateAnd$value0 == 'screen_on' & lead(PowerStateAnd$value0 == 'screen_off') & PowerStateAnd$participantid == lead(PowerStateAnd$participantid), lead(PowerStateAnd$dateinsec),0)

# calculate the duration of correct screen time, store under "time screen" column
PowerStateAnd$timescreen <-ifelse(PowerStateAnd$value0 == 'screen_on' & PowerStateAnd$correct > 0, PowerStateAnd$correct-PowerStateAnd$dateinsec,0)

# summarize screen time (will include lots of zeros due to the inclusion of incorrect episodes, DW!)
summary(PowerStateAnd$timescreen)

test<-filter(PowerStateAnd, timescreen  >36000 )

# create a table of participantid counts that belong to people with 10+ hrs of screen time
participantid_counts <- table(test$participantid)

# Extract participantids which consistently show more than 10 hrs of screen time
# (people who have more than 10 episodes wherein they have 10+ hrs of screen time)
problematicParticipants <- names(participantid_counts[participantid_counts > 10])

# View the list of problematic participants
print(problematicParticipants)

# remove the problematic participants from the original data frame 
PowerStateAnd <- PowerStateAnd %>%
  filter(!participantid %in% problematicParticipants)


PowerStateAnd <-filter(PowerStateAnd, timescreen  <= 36000 )

# filter out the incorrect episodes (these are episodes that are 0 seconds long)
LockStateAnd <- filter(PowerStateAnd, timescreen != 0)


################################################################################
############### Import Local Timzeones from Timezone_Lookup Table ##############
################################################################################

# Convert datetime columns to POSIXct for comparison
LockStateAnd <- LockStateAnd %>%
  mutate(measuredat_1 = as.POSIXct(measuredat_1, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))

LookUpTimezone <- LookUpTimezone %>%
  mutate(
    start_time = as.POSIXct(start_time, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    end_time = as.POSIXct(end_time, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
  )

# Apply function to LockStateAnd
LockStateAnd <- LockStateAnd %>%
  rowwise() %>%
  mutate(timezone = assign_timezone(participantid, measuredat_1)) %>%
  ungroup()


# Compute timezone offset based on timezones
# save under a new column "timezone_difference"
LockStateAnd$timezone_difference <- sapply(1:nrow(LockStateAnd), function(i) {
  tz <- LockStateAnd$timezone[i]
  dt <- LockStateAnd$measuredat_1[i]
  
  if (is.na(tz)) {
    return(NA_real_)  # NA timezones are automatically NA
  }
  
  return(tz_offset(as.POSIXct(dt), tz = tz)$utc_offset_h)
})


# Calculate measuredat_local by adding computed offset to UTC
LockStateAnd <- LockStateAnd %>%
  mutate(measuredat_local = measuredat_1 + as.difftime(timezone_difference, units = "hours"))


# keep only necessary rows
cleanLockStateAnd <- LockStateAnd %>%
  select(participantid, measuredat_1, DateNumber, measuredat_local, timescreen)

################################################################################
########################### Extract Final Features #############################
################################################################################


# Extract hour
cleanLockStateAnd <- cleanLockStateAnd %>% 
  mutate(hour = as.numeric(format(measuredat_local, "%H")))

# Create three separate dataframes based on time of day
daytime <-cleanLockStateAnd %>% filter(hour >= 7 & hour < 17)
evening <- cleanLockStateAnd %>% filter(hour >= 17 & hour < 23)
nighttime <- cleanLockStateAnd %>% filter(hour >= 23 | hour < 7)


# aggregate the total, daytime, evening,and nighttime timescreen for each participant per day
totalScreenTime <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateAnd, sum)
daytimeScreenTime <- aggregate(timescreen ~ DateNumber+participantid, daytime, sum)
eveningScreenTime <- aggregate(timescreen ~ DateNumber+participantid, evening, sum)
nighttimeScreenTime <- aggregate(timescreen ~ DateNumber+participantid, nighttime, sum)

# calculate the number of times screens were unlocked per day
NumberOfEvents <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateAnd, length)
NumberOfEvents <- NumberOfEvents %>% rename(numOfEvents = timescreen)

NumberOfEvents_daytime <- aggregate(timescreen ~ DateNumber+participantid, daytime, length)
NumberOfEvents_daytime <- NumberOfEvents_daytime %>% rename(numOfEvents_daytime = timescreen)

NumberOfEvents_evening <- aggregate(timescreen ~ DateNumber+participantid, evening, length)
NumberOfEvents_evening <- NumberOfEvents_evening %>% rename(numOfEvents_evening = timescreen)

NumberOfEvents_nighttime <- aggregate(timescreen ~ DateNumber+participantid, nighttime, length)
NumberOfEvents_nighttime <- NumberOfEvents_nighttime %>% rename(numOfEvents_nighttime = timescreen)



# combine screen time and number of unlocks/events in on dataframe 
totalFeaturesAnd <- cbind(totalScreenTime,NumberOfEvents$numOfEvents)
daytimeFeaturesAnd <- cbind(daytimeScreenTime,NumberOfEvents_daytime$numOfEvents_daytime)
eveningFeaturesAnd <- cbind(eveningScreenTime,NumberOfEvents_evening$numOfEvents_evening)
nighttimeFeaturesAnd <- cbind(nighttimeScreenTime,NumberOfEvents_nighttime$numOfEvents_nighttime)



#convert DateNumber back into Dates and remove DateNumber columns
totalFeaturesAnd$measuredat <- as.Date(totalFeaturesAnd$DateNumber, origin = "1970-01-01")
totalFeaturesAnd <- select(totalFeaturesAnd, -DateNumber)

daytimeFeaturesAnd$measuredat <- as.Date(daytimeFeaturesAnd$DateNumber, origin = "1970-01-01")
daytimeFeaturesAnd <- select(daytimeFeaturesAnd, -DateNumber)

eveningFeaturesAnd$measuredat <- as.Date(eveningFeaturesAnd$DateNumber, origin = "1970-01-01")
eveningFeaturesAnd <- select(eveningFeaturesAnd, -DateNumber)

nighttimeFeaturesAnd$measuredat <- as.Date(nighttimeFeaturesAnd$DateNumber, origin = "1970-01-01")
nighttimeFeaturesAnd <- select(nighttimeFeaturesAnd, -DateNumber)



# rename columns for clarity
totalFeaturesAnd <- totalFeaturesAnd %>%
  rename(numOfEvents_total = `NumberOfEvents$numOfEvents`,  # Rename 'numOfEvents' to 'NumberOfEvents'
         totalScreenTime = timescreen)  # Rename 'totalScreenTime' to 'timescreen'

head(totalFeaturesAnd)


daytimeFeaturesAnd <- daytimeFeaturesAnd %>%
  rename(numOfEvents_daytime = `NumberOfEvents_daytime$numOfEvents_daytime`,  
         daytimeScreenTime = timescreen)  

head(daytimeFeaturesAnd)


eveningFeaturesAnd <- eveningFeaturesAnd %>%
  rename(numOfEvents_evening = `NumberOfEvents_evening$numOfEvents_evening`,  
         eveningScreenTime = timescreen)  

head(eveningFeaturesAnd)


nighttimeFeaturesAnd <- nighttimeFeaturesAnd %>%
  rename(numOfEvents_nighttime = `NumberOfEvents_nighttime$numOfEvents_nighttime`,  
         nighttimeScreenTime = timescreen)  

head(nighttimeFeaturesAnd)


################################################################################

# Because we filtered rows based on hours when computing daytime, evening, and 
# nighttime features, there are distinct number of rows corresponding to unique
# 'participantid' - 'measured_date' combinations. In order to keep everything 
# aligned (as we are working with identical PIDs and dates across different 
# screentime features) we will match the number of rows for
# each feature, with values = NA for newly added rows.

################################################################################


# Extract unique participantid-measured_date combinations
total_combos <- totalFeaturesAnd %>% 
  select(participantid, measuredat) %>% 
  distinct()

daytime_combos <- daytimeFeaturesAnd %>% 
  select(participantid, measuredat) %>% 
  distinct()

evening_combos <- eveningFeaturesAnd %>% 
  select(participantid, measuredat) %>% 
  distinct()

nighttime_combos <- nighttimeFeaturesAnd %>% 
  select(participantid, measuredat) %>% 
  distinct()

# Convert to vectors
total_vector <- paste(total_combos$participantid, total_combos$measuredat, sep = "_")
daytime_vector <- paste(daytime_combos$participantid, daytime_combos$measuredat, sep = "_")
evening_vector <- paste(evening_combos$participantid, evening_combos$measuredat, sep = "_")
nighttime_vector <- paste(nighttime_combos$participantid, nighttime_combos$measuredat, sep = "_")

# Find missing combinations
missing_combos_daytime <- setdiff(total_vector, daytime_vector)
missing_combos_evening <- setdiff(total_vector, evening_vector)
missing_combos_nighttime <- setdiff(total_vector, nighttime_vector)

# Convert missing combos back to a data frame
missing_combos_df_daytime <- data.frame(
  participantid = sub("_.*", "", missing_combos_daytime),
  measuredat = as.Date(sub(".*_", "", missing_combos_daytime)),
  daytimeScreenTime = NA,
  numOfEvents_daytime = NA
)

missing_combos_df_evening <- data.frame(
  participantid = sub("_.*", "", missing_combos_evening),
  measuredat = as.Date(sub(".*_", "", missing_combos_evening)),
  eveningScreenTime = NA,
  numOfEvents_evening = NA
)

missing_combos_df_nighttime <- data.frame(
  participantid = sub("_.*", "", missing_combos_nighttime),
  measuredat = as.Date(sub(".*_", "", missing_combos_nighttime)),
  nighttimeScreenTime = NA,
  numOfEvents_nighttime = NA
)



# arrange the column order to match each other before rowbinding 
daytimeFeaturesAnd <- daytimeFeaturesAnd %>%
  select(participantid, measuredat, everything())

eveningFeaturesAnd <- eveningFeaturesAnd %>%
  select(participantid, measuredat, everything())

nighttimeFeaturesAnd <- nighttimeFeaturesAnd %>%
  select(participantid, measuredat, everything())

totalFeaturesAnd <- totalFeaturesAnd %>%
  select(participantid, measuredat, everything())


# Append missing rows
daytimeFeaturesAnd <- rbind(daytimeFeaturesAnd, missing_combos_df_daytime)
eveningFeaturesAnd <- rbind(eveningFeaturesAnd, missing_combos_df_evening)
nighttimeFeaturesAnd <- rbind(nighttimeFeaturesAnd, missing_combos_df_nighttime)


# Order by PID and measured_date
daytimeFeaturesAnd <- daytimeFeaturesAnd %>% arrange(participantid, measuredat)
eveningFeaturesAnd <- eveningFeaturesAnd %>% arrange(participantid, measuredat)
nighttimeFeaturesAnd <- nighttimeFeaturesAnd %>% arrange(participantid, measuredat)


# Merge all features into a single dataframe
finalScreenTimeFeaturesAnd <- reduce(list(totalFeaturesAnd, daytimeFeaturesAnd, eveningFeaturesAnd, nighttimeFeaturesAnd), 
                                     full_join, by = c("participantid", "measuredat"))

##############################################################################
######## save the screen time features in the PSQL database ##################
##############################################################################

# Rename columns in the dataframe to match SQL table column names
colnames(finalScreenTimeFeaturesiOS) <- c(
  "participantid", 
  "measuredat", 
  "total_screen_time", 
  "num_of_events_total", 
  "daytime_screen_time", 
  "num_of_events_daytime", 
  "evening_screen_time", 
  "num_of_events_evening", 
  "nighttime_screen_time", 
  "num_of_events_nighttime"
)

# Check the structure to confirm changes
str(finalScreenTimeFeaturesiOS)


append_to_db(finalScreenTimeFeaturesiOS, "user1_workspace", "daily_screen_features_ios")























# keep only necessary rows
cleanLockStateAnd <- LockStateAnd %>%
  select(participantid, measuredat_1, DateNumber, timescreen)

# aggregate the total timescreen for each participant per day
totalScreenTime <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateAnd, sum)

# store date number, participant IDs and number of unlocks per day in a data frame called "UnlocksPerDay"
NumberOfEvents <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateAnd, length)
NumberOfEvents <- NumberOfEvents %>% rename(numOfEvents = timescreen)

# comine total screen time and number of unlocks/events in on dataframe 
screenTimeFeaturesAnd <- cbind(totalScreenTime,NumberOfEvents$numOfEvents)

#convert DateNumber back into Dates and remove DateNumber columns
screenTimeFeaturesAnd$measuredat <- as.Date(screenTimeFeaturesAnd$DateNumber, origin = "1970-01-01")
screenTimeFeaturesAnd <- select(screenTimeFeaturesAnd, -DateNumber)

# rename columns for clarity
screenTimeFeaturesAnd <- screenTimeFeaturesAnd %>%
  rename(numofevents = `NumberOfEvents$numOfEvents`,  # Rename 'numOfEvents' to 'NumberOfEvents'
         totalscreentime = timescreen)  # Rename 'totalScreenTime' to 'timescreen'

head(screenTimeFeaturesAnd)

#combine Android and iOS
screenTimeFeatures <- rbind(screenTimeFeaturesAnd, screenTimeFeaturesiOS)

head(screenTimeFeatures)



##############################################################################
######## Save the Screen Time Features data into PSQL database ###############  
##############################################################################
# Change the format so it's compatible with PSQL

screenTimeFeatures <- screenTimeFeatures %>%
  mutate(
    measuredat = as.Date(measuredat, format = "%Y-%m-%d"),  # Ensure Date format
    totalscreentime = as.numeric(totalscreentime),
    numofevents = as.integer(numofevents)
  )


append_to_db(screenTimeFeatures, "user1_workspace", "daily_screen_features")











