rm(list=ls())

library(dplyr)
library(purrr)
library(stringr)

data <- read.csv("SMMS_aggregated_full_features_SD_Aug22.csv")

################################################################################
####################### Target: Emo_symptoms_baseline ##########################
################################################################################
# hyperactivity, conduct

# remove targets that are not of interest
data <- dplyr::select(data, -c(X, emo_symptoms_baseline, hyperactivity_baseline, conduct_probs_baseline,
                               peer_probs_baseline, prosocial_baseline, emo_symptoms_followup, hyperactivity_followup,
                               conduct_probs_followup,peer_probs_followup,prosocial_followup, emo_symptoms_followup_cat,
                               hyperactivity_baseline_cat, hyperactivity_followup_cat, conduct_probs_baseline_cat,
                               conduct_probs_followup_cat, peer_probs_baseline_cat, peer_probs_followup_cat,
                               prosocial_baseline_cat, prosocial_followup_cat
                               ))

for (i in seq_along(data)) {
  cat(names(data)[i], ":", sum(is.na(data[[i]])), "\n")
}

dim(data)
data <- data[!is.na(data$emo_symptoms_baseline_cat), ]
dim(data) # removed 81 people who are missing emo-baseline score



################################################################################
####################### Feature Selection : Phase 1 ############################
################################################################################

# MISSINGNESS PER COL
# top 20 missingness for cols
na_prop <- colMeans(is.na(data))
sort(na_prop, decreasing = TRUE)[1:20]   

# drop cols with at least 20% missingness 
drop_cols <- names(na_prop)[na_prop > 0.20]
drop_cols
data <- data[ , !(names(data) %in% drop_cols)]
#(2 cols dropped: "gps_location_variance_cv"   "call_num_rejected_calls_cv")



# MISSINGNESS PER PARTICIPANT
# compute missingness per participant %
row_na_perc <- rowMeans(is.na(data)) * 100

missingness_df <- data.frame(
  participantid = data$participantid,
  na_percent = row_na_perc
)
missingness_df

# filter participants with >20% missing
bad_participants <- missingness_df[missingness_df$na_percent > 20, ]
bad_participants$participantid # no participants with at least 20% missing - good!



# CLUSTER-WISE CORRELATION

# (there are 1920 very highly correlated pairs so we're taking a cluster-wise approach)

#The cluster-wise approach groups highly correlated variables together 
#(based on 1−|correlation| as a distance), then keeps one representative from each group 
#to avoid redundancy. This reduces dimensionality while preserving the diversity of information
#across clusters. Essentially, each cluster represents a “feature family,” and the
#chosen variable acts as its proxy.
cor_mat <- cor(
  data[sapply(data, is.numeric)], 
  method = "spearman", 
  use = "pairwise.complete.obs"
)


dist_mat <- as.dist(1 - abs(cor_mat))
hc <- hclust(dist_mat, method = "average")
clusters <- cutree(hc, h = 0.1)
table(clusters)

# make a dataframe mapping each variable to its cluster
cluster_df <- data.frame(
  variable = names(clusters),
  cluster = clusters
)

# pick 1 representative per cluster (here: the first variable)
rep_features <- cluster_df %>%
  group_by(cluster) %>%
  slice(1) %>%
  pull(variable)

length(rep_features)   # number of final features
# add back target + participant ID if they exist in the data
keep_vars <- c(rep_features, "emo_symptoms_baseline_cat", "participantid")

# keep only these columns (and drop the rest)
data_reduced <- data[ , colnames(data) %in% keep_vars]

# check dimensions
dim(data)         # 
dim(data_reduced) # 188 features removed

# examine excluded features
excluded_features <- setdiff(names(data), keep_vars)
excluded_features





# IMPUTE MISSING DATA
# impute NAs with column medians (numeric columns only)
# if we skip imputation, we will end up removing 35 participants
# who are only missing less than 1% of their data
# so instead we replace their NAs with the medians of those features
for (col in names(data_reduced)) {
  if (is.numeric(data_reduced[[col]])) {
    med <- median(data_reduced[[col]], na.rm = TRUE)
    data_reduced[[col]][is.na(data_reduced[[col]])] <- med
  }
}

# remove all remaining NAs - ZERO participants excluded!! YAY!
data_reduced <- data_reduced[complete.cases(data_reduced), ]
data <- data_reduced



# KRUSKAL WALLIS ON TRAINING SET
# non-parametric ANOVA alternative as not all features are normally distributed
# tells us if the distributions of a numeric feature differ significantly across groups.
outcome <- "emo_symptoms_baseline_cat"
num_cols <- setdiff(names(data)[sapply(data, is.numeric)], c(outcome, "participantid"))

kw_stats <- sapply(num_cols, function(v) {
  kt <- kruskal.test(data[[v]] ~ data[[outcome]])
  n <- sum(complete.cases(data[[v]], data[[outcome]]))
  k <- nlevels(data[[outcome]])
  eps2 <- max(0, (kt$statistic - (k-1)) / (n-1))   # epsilon squared
  c(H = unname(kt$statistic), p = kt$p.value, eps2 = eps2)
})

# put results in a dataframe + correct p-values for multiple comparisons 
# H (KW test statistic)
# p (raw p-value)
# q (FDR-adjusted p-value)
# eps2 (effect size: 0 = no effect, ~0.01 small, ~0.06 medium, ~0.14+ large)

kw_df <- as.data.frame(t(kw_stats))
kw_df$q <- p.adjust(kw_df$p, method = "fdr")
kw_df$feature <- rownames(kw_df)
View(kw_df)


# select features and rank
selected <- kw_df$feature[kw_df$q < 0.05]
# no significant features... 

selected <- kw_df$feature[kw_df$eps2 >= 0.14]
# no features with large enough effect size..

# rank by effect size (descending)
kw_df_desc <- kw_df[order(-kw_df$eps2), ]
View(kw_df_desc)

# select top 70
top70 <- head(kw_df_desc$feature, 70)

# iterate num of feat 30 - 80

length(top70) 
top70  # preview remaining features 
keep_vars <- c(top70, "participantid", "emo_symptoms_baseline_cat")
data <- data[, keep_vars, drop = FALSE]
dim(data)




################################################################################
####################### Multinomial Logistic Regression ########################
################################################################################

library(glmnet)
library(caret)

# Remove ID column
X <- data[, setdiff(names(data), c("participantid", "emo_symptoms_baseline_cat"))]
y <- data$emo_symptoms_baseline_cat

# Convert outcome to factor (glmnet needs numeric matrix + factor outcome)
X <- as.matrix(X) 
y <- factor(y)


# NESTED CV
# Outer Loop 5 folds 
library(glmnet)

set.seed(26)
folds <- sample(rep(1:10, length.out = length(y)))   # 5 outer folds
alpha_grid <- c(0.1, 0.5, 1)                        # Elastic Net (mostly ridge, mix, pure LASSO)

results <- lapply(1:5, function(outer_fold) {
  # ---- Split train/test ----
  train_idx <- which(folds != outer_fold)
  test_idx  <- which(folds == outer_fold)
  
  X_train <- X[train_idx, ]
  y_train <- droplevels(y[train_idx])
  X_test  <- X[test_idx, ]
  y_test  <- droplevels(y[test_idx])
  
  # ---- Inner CV: tune alpha + lambda ----
  inner_results <- lapply(alpha_grid, function(a) {
    cv_fit <- cv.glmnet(
      X_train, y_train,
      family = "multinomial",
      alpha = a,
      type.measure = "class",
      maxit = 1e6,
      nlambda = 100,
      lambda.min.ratio = 0.01
    )
    list(alpha = a, cv_fit = cv_fit, cvm = min(cv_fit$cvm))
  })
  
  # Pick best alpha (lowest CV error)
  best_inner <- inner_results[[which.min(sapply(inner_results, `[[`, "cvm"))]]
  best_alpha <- best_inner$alpha
  best_lambda <- best_inner$cv_fit$lambda.min
  
  # ---- Fit final model with best α + λ ----
  final_fit <- glmnet(
    X_train, y_train,
    family = "multinomial",
    alpha = best_alpha,
    lambda = best_lambda,
    maxit = 1e6
  )
  
  # Predictions on test set
  preds <- predict(final_fit, newx = X_test, s = best_lambda, type = "class")
  acc <- mean(preds == y_test)
  
  # ---- Extract nonzero features ----
  coefs <- coef(final_fit)  # list of coefficient matrices (per class)
  selected_features <- unique(unlist(
    lapply(coefs, function(mat) rownames(mat)[mat[,1] != 0])
  ))
  selected_features <- setdiff(selected_features, "(Intercept)")
  
  list(
    fold = outer_fold,
    acc = acc,
    best_alpha = best_alpha,
    best_lambda = best_lambda,
    nonzero_total = length(selected_features),
    selected = selected_features
  )
})

# ---- Diagnostics ----
sapply(results, function(r) r$acc)           # accuracy per fold
sapply(results, function(r) r$best_alpha)    # best alpha per fold
sapply(results, function(r) r$nonzero_total) # number of features kept






