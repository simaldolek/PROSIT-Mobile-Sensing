library(dplyr)
library(MASS)      # for huber
library(entropy)   # for entropy calculation


conn <- url("https://raw.githubusercontent.com/simaldolek/PROSIT-Mobile-Sensing/refs/heads/main/functions.R")
source(conn)
close(conn)

data <- read.csv("SMMS_5days_SD_Aug22.csv")

str(data)
summary(data)



################################################################################
########### Define Functions to Extract Secondary Features  ####################
################################################################################

# helper: RMSSD
rmssd <- function(x) {
  x <- na.omit(x)
  if (length(x) < 2) return(NA)
  sqrt(mean(diff(x)^2))
}

# helper: Shannon entropy (discretize into bins)
shannon_entropy <- function(x, bins = 10) {
  x <- na.omit(x)
  if (length(x) == 0) return(NA)
  counts <- hist(x, breaks = bins, plot = FALSE)$counts
  entropy::entropy(counts, unit = "log2")
}

extract_features <- function(df, id_col = "participantid") {
  # Columns where we just want the first available value per participant
  take_first_cols <- c("device_type",
    "emo_symptoms_baseline", "hyperactivity_baseline", "conduct_probs_baseline",
    "peer_probs_baseline", "prosocial_baseline",
    "emo_symptoms_followup", "hyperactivity_followup", "conduct_probs_followup",
    "peer_probs_followup", "prosocial_followup"
  )
  
  # Numeric columns (excluding ID, dropped, and take_first ones)
  numeric_cols <- names(df)[sapply(df, is.numeric)]
  numeric_cols <- setdiff(numeric_cols, c(id_col, "X", take_first_cols))
  
  df %>%
    group_by(.data[[id_col]]) %>%
    summarise(
      available_days = n(),
      
      # Aggregates for numeric columns
      across(all_of(numeric_cols), list(
        mean   = ~mean(., na.rm = TRUE),
        median = ~median(., na.rm = TRUE),
        sd     = ~sd(., na.rm = TRUE),
        iqr    = ~IQR(., na.rm = TRUE),
        cv     = ~ifelse(mean(., na.rm = TRUE) == 0, NA, 
                         sd(., na.rm = TRUE)/mean(., na.rm = TRUE)),
        rmssd  = ~rmssd(.),
        mad    = ~mad(., na.rm = TRUE),
        range  = ~ifelse(all(is.na(.)), NA, max(., na.rm = TRUE) - min(., na.rm = TRUE)),
        above_median = ~sum(. > median(., na.rm = TRUE), na.rm = TRUE),
        below_median = ~sum(. < median(., na.rm = TRUE), na.rm = TRUE),
        entropy = ~shannon_entropy(.)
      ), .names = "{.col}_{.fn}"),
      
      # First available values for baseline/followup
      across(all_of(take_first_cols), ~first(.[!is.na(.)]), .names = "{.col}")
    )
}


################################################################################
########### Aggregated Data over Days per Participant  #########################
################################################################################

aggregated_data <- extract_features(data, id_col = "participantid")

names(aggregated_data)
summary(aggregated_data)


################################################################################
########### Huber Normalization on Aggregated Features  ########################
################################################################################

# Simple Huber normalization for one vector (no fallback; preserves NAs)
huber_normalize_simple <- function(x) {
  x <- as.numeric(x)
  idx <- is.finite(x) & !is.na(x)
  # huber() cannot handle NA/Inf, so fit on finite, non-NA values only
  h <- huber(x[idx])                    # may error if MAD == 0 etc.
  z <- x
  z[idx] <- (x[idx] - h$mu) / h$s
  z
}

# Apply to all numeric columns in a data.frame
# - Overwrites columns in df
# - Skips columns that error; prints their names
# - Returns list with updated df + vector of skipped columns


huber_normalize_df <- function(df, exclude = c(
  "available_days", "emo_symptoms_baseline","hyperactivity_baseline","conduct_probs_baseline",
  "peer_probs_baseline","prosocial_baseline",
  "emo_symptoms_followup","hyperactivity_followup","conduct_probs_followup",
  "peer_probs_followup","prosocial_followup"
)) {
  numeric_cols <- setdiff(names(df)[sapply(df, is.numeric)], exclude)
  
  skipped <- character(0)
  normalized <- character(0)
  
  for (col in numeric_cols) {
    vals <- df[[col]]
    
    # Explicit check for constant or all-NA columns
    if (all(is.na(vals)) || mad(vals, na.rm = TRUE) == 0) {
      message(sprintf("Skipping column '%s': all NA or MAD = 0", col))
      skipped <- c(skipped, col)
      next
    }
    
    # Try normalization, catch real errors
    tryCatch({
      df[[col]] <- huber_normalize_simple(vals)
      normalized <- c(normalized, col)
    }, error = function(e) {
      message(sprintf("Skipping column '%s': %s", col, conditionMessage(e)))
      skipped <- c(skipped, col)
    })
  }
  
  message(sprintf("Huber-normalized %d numeric columns. Skipped %d.",
                  length(unique(normalized)), length(unique(skipped))))
  
  list(
    data = df,
    skipped = unique(skipped),
    normalized = unique(normalized),
    excluded = intersect(exclude, names(df))
  )
}



aggregated_data_and <- dplyr::filter(aggregated_data, device_type == "android")
aggregated_data_ios <- dplyr::filter(aggregated_data, device_type == "ios")

res_and <-huber_normalize_df(aggregated_data_and)
res_ios <-huber_normalize_df(aggregated_data_ios)

data_norm_and <- res_and$data
data_norm_ios <- res_ios$data

res_and$skipped
res_ios$skipped # columns where huber() failed (e.g., MAD==0)
cols_to_remove <- union(res_and$skipped, res_ios$skipped)
cols_to_remove

# remove all skipped features since MAD = 0 (not valuable for classification)
data_norm_and_clean <- data_norm_and[ , !(names(data_norm_and) %in% cols_to_remove)]
data_norm_ios_clean <- data_norm_ios[ , !(names(data_norm_ios) %in% cols_to_remove)]

data_norm <- bind_rows(
  data_norm_and_clean,
  data_norm_ios_clean
)

# Sort by participantid 
data_norm <- data_norm %>%
  arrange(participantid)

View(data_norm)

################################################################################
########### SDQ Clinical Cutoffs to Categorize PIDs into Groups  ###############
################################################################################

# helpers for each SDQ scale
sdq_cat_emo <- function(x) dplyr::case_when(
  is.na(x)                ~ NA_character_,
  x >= 0 & x <= 3         ~ "normal",
  x >= 4 & x <= 5         ~ "borderline",
  x >= 6 & x <= 10        ~ "abnormal",
  TRUE                    ~ NA_character_
)

sdq_cat_hyper <- function(x) dplyr::case_when(
  is.na(x)                ~ NA_character_,
  x >= 0 & x <= 5         ~ "normal",
  x >= 6 & x <= 7         ~ "borderline",
  x >= 8 & x <= 10        ~ "abnormal",
  TRUE                    ~ NA_character_
)

sdq_cat_conduct <- function(x) dplyr::case_when(
  is.na(x)                ~ NA_character_,
  x >= 0 & x <= 2         ~ "normal",
  x == 3                  ~ "borderline",
  x >= 4 & x <= 10        ~ "abnormal",
  TRUE                    ~ NA_character_
)

# peer problems has same cutoffs as conduct problems
sdq_cat_peer <- sdq_cat_conduct

sdq_cat_prosocial <- function(x) dplyr::case_when(
  is.na(x)                ~ NA_character_,
  x >= 7 & x <= 10        ~ "normal",
  x == 6                  ~ "borderline",
  x >= 0 & x <= 5         ~ "abnormal",
  TRUE                    ~ NA_character_
)

# add 10 categorical columns at the end
data_norm <- data_norm %>%
  mutate(
    emo_symptoms_baseline_cat    = factor(sdq_cat_emo(emo_symptoms_baseline),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    emo_symptoms_followup_cat    = factor(sdq_cat_emo(emo_symptoms_followup),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    hyperactivity_baseline_cat   = factor(sdq_cat_hyper(hyperactivity_baseline),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    hyperactivity_followup_cat   = factor(sdq_cat_hyper(hyperactivity_followup),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    conduct_probs_baseline_cat   = factor(sdq_cat_conduct(conduct_probs_baseline),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    conduct_probs_followup_cat   = factor(sdq_cat_conduct(conduct_probs_followup),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    peer_probs_baseline_cat      = factor(sdq_cat_peer(peer_probs_baseline),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    peer_probs_followup_cat      = factor(sdq_cat_peer(peer_probs_followup),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    prosocial_baseline_cat       = factor(sdq_cat_prosocial(prosocial_baseline),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE),
    prosocial_followup_cat       = factor(sdq_cat_prosocial(prosocial_followup),
                                          levels = c("normal","borderline","abnormal"), ordered = TRUE)
  )


View(data_norm)

write.csv(data_norm, "SMMS_aggregated_full_features_SD_Aug22.csv")

