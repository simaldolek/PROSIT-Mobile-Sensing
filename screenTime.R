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


################################################################################
################################## iOS #########################################
################################################################################

# filter out the "Terminating" values from analytics
test2 <- filter(AnalyticsDataiOS, value0 == "Terminating")

# attach analytics data frame at the end of lock state data frame
LockStateiOS <- rbind(LockStateiOS, test2)

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

# keep only necessary rows
cleanLockStateiOS <- LockStateiOS %>%
  select(participantid, measuredat_1, DateNumber, timescreen)

# aggregate the total timescreen for each participant per day
totalScreenTime <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateiOS, sum)

# store date number, participant IDs and number of unlocks per day in a data frame called "UnlocksPerDay"
NumberOfEvents <- aggregate(timescreen ~ DateNumber+participantid, cleanLockStateiOS, length)
NumberOfEvents <- NumberOfEvents %>% rename(numOfEvents = timescreen)

# comine total screen time and number of unlocks/events in on dataframe 
screenTimeFeaturesiOS <- cbind(totalScreenTime,NumberOfEvents$numOfEvents)

#convert DateNumber back into Dates and remove DateNumber columns
screenTimeFeaturesiOS$measuredat <- as.Date(screenTimeFeaturesiOS$DateNumber, origin = "1970-01-01")
screenTimeFeaturesiOS <- select(screenTimeFeaturesiOS, -DateNumber)

# rename columns for clarity
screenTimeFeaturesiOS <- screenTimeFeaturesiOS %>%
  rename(numOfEvents = `NumberOfEvents$numOfEvents`,  # Rename 'numOfEvents' to 'NumberOfEvents'
         totalScreenTime = timescreen)  # Rename 'totalScreenTime' to 'timescreen'

head(screenTimeFeaturesiOS)





################################################################################
################################## Android #####################################
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











