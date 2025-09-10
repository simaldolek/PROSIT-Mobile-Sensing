rm(list=ls())

library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(tibble)
library(glmnet)
library(caret)
library(MASS)
library(randomForest)
library(xgboost)

data <- read.csv("SMMS_aggregated_full_features_SD_Aug28.csv")

################################################################################
####################### Target: hyperactivity_baseline ##########################
################################################################################

# remove targets that are not of interest 
data <- dplyr::select(data, -c(X, emo_symptoms_baseline, hyperactivity_baseline, conduct_probs_baseline,
                               peer_probs_baseline, prosocial_baseline, emo_symptoms_followup, hyperactivity_followup,
                               conduct_probs_followup,peer_probs_followup,prosocial_followup, emo_symptoms_followup_cat,
                               emo_symptoms_baseline_cat, hyperactivity_followup_cat, conduct_probs_baseline_cat,
                               conduct_probs_followup_cat, peer_probs_baseline_cat, peer_probs_followup_cat,
                               prosocial_baseline_cat, prosocial_followup_cat
                               ))

for (i in seq_along(data)) {
  cat(names(data)[i], ":", sum(is.na(data[[i]])), "\n")
}

dim(data)
data <- data[!is.na(data$hyperactivity_baseline_cat), ]
dim(data) # removed 78 people who are missing hyperactivity-baseline score



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
keep_vars <- c(rep_features, "hyperactivity_baseline_cat", "participantid", "device_type")

# keep only these columns (and drop the rest)
data_reduced <- data[ , colnames(data) %in% keep_vars]

# check dimensions
dim(data)         # 
dim(data_reduced) # 231 features removed

# examine excluded features
excluded_features <- setdiff(names(data), keep_vars)
excluded_features






# KRUSKAL WALLIS ON TRAINING SET
# non-parametric ANOVA alternative as not all features are normally distributed
# tells us if the distributions of a numeric feature differ significantly across groups.
outcome <- "hyperactivity_baseline_cat"
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





################################################################################
################################ HELPER FUNCTIONS ##############################
################################################################################


# Safe Huber fit (returns mu, s)
# If too few points or zero spread, fall back to median and MAD
huber_fit <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) < 5 || all(is.na(x)) || mad(x, na.rm = TRUE) == 0)
    return(list(mu = median(x, na.rm = TRUE), s = mad(x, na.rm = TRUE) + 1e-8))
  h <- huber(x)
  list(mu = h$mu, s = ifelse(h$s <= 0, mad(x, na.rm = TRUE) + 1e-8, h$s))
}

# Apply Huber z
huber_apply <- function(x, mu, s) (x - mu) / s

# Winsorize by Huber z 
# if a point’s robust z > 3 (or < −3), clip it to ±3, then transform back to original scale.
winsorize_by_huber <- function(x, mu, s, zcap = 3) {
  z <- (x - mu) / s
  z[z >  zcap] <-  zcap
  z[z < -zcap] <- -zcap
  z * s + mu
}

# Fit Huber params separately for device type on TRAIN; 
# transform TRAIN & TEST; drop columns that error (due to MAD==0 etc.)
fit_huber_by_device <- function(df_train, df_test, device_col = "device_type", numeric_cols) {
  devices <- unique(df_train[[device_col]])
  params <- lapply(devices, function(dev) {
    dsub <- df_train[df_train[[device_col]] == dev, , drop = FALSE]
    setNames(lapply(numeric_cols, function(col) huber_fit(dsub[[col]])), numeric_cols)
  })
  names(params) <- devices
  
  transform_df <- function(df) {
    out <- df
    dropped <- character(0)
    for (dev in devices) {
      idx <- which(out[[device_col]] == dev)
      if (!length(idx)) next
      for (col in numeric_cols) {
        if (!col %in% colnames(out)) next
        mu <- params[[dev]][[col]]$mu
        s  <- params[[dev]][[col]]$s
        res <- try({
          out[idx, col] <- winsorize_by_huber(out[idx, col], mu, s, zcap = 3)
          out[idx, col] <- huber_apply(out[idx, col], mu, s)
        }, silent = TRUE)
        if (inherits(res, "try-error") ||
            all(!is.finite(out[idx, col])) || all(is.na(out[idx, col]))) {
          dropped <- c(dropped, col)
        }
      }
    }
    list(df = out, dropped = unique(dropped))
  }
  
  tr_res <- transform_df(df_train)
  te_res <- transform_df(df_test)
  
  all_drop <- sort(unique(c(tr_res$dropped, te_res$dropped)))
  if (length(all_drop)) {
    tr_res$df <- tr_res$df[, setdiff(colnames(tr_res$df), all_drop), drop = FALSE]
    te_res$df <- te_res$df[, setdiff(colnames(te_res$df), all_drop), drop = FALSE]
  }
  
  list(train = tr_res$df, test = te_res$df, dropped = all_drop, params = params)
}

# Median imputation using TRAIN medians; apply to TEST
impute_by_train_median <- function(df_train, df_test, cols) {
  meds <- vapply(cols, function(cn) median(df_train[[cn]], na.rm = TRUE), numeric(1))
  if (any(!is.finite(meds))) {
    # if a median is NA/Inf (all-NA column), we'll handle by dropping later
    meds[!is.finite(meds)] <- 0
  }
  # train
  for (cn in cols) {
    idx <- which(is.na(df_train[[cn]]))
    if (length(idx)) df_train[[cn]][idx] <- meds[cn]
  }
  # test
  for (cn in cols) {
    idx <- which(is.na(df_test[[cn]]))
    if (length(idx)) df_test[[cn]][idx] <- meds[cn]
  }
  list(train = df_train, test = df_test, medians = meds)
}

# Upsample TRAIN by (class × device) interaction
# this ensures that our inner/outer splits have all 6 groups present (ios/android x 3 categories)
upsample_by_interaction <- function(df, y_col = "hyperactivity_baseline_cat", device_col = "device_type") {
  stopifnot(is.factor(df[[y_col]]))
  df$.combo <- interaction(df[[y_col]], df[[device_col]], drop = TRUE)
  tab <- table(df$.combo)
  if (!length(tab)) return(df)
  max_n <- max(tab)
  up_df <- do.call(rbind, lapply(names(tab), function(k) {
    block <- df[df$.combo == k, , drop = FALSE]
    if (!nrow(block)) return(NULL)
    block[sample(seq_len(nrow(block)), size = max_n, replace = TRUE), , drop = FALSE]
  }))
  up_df$.combo <- NULL
  rownames(up_df) <- NULL
  up_df
}

# Extract a 1D importance vector (names = features), prefer MeanDecreaseGini
get_imp_vec <- function(rf_obj) {
  imp <- try(importance(rf_obj), silent = TRUE)
  if (inherits(imp, "try-error")) return(setNames(numeric(0), character(0)))
  if (is.matrix(imp) && "MeanDecreaseGini" %in% colnames(imp)) {
    v <- imp[, "MeanDecreaseGini"]
  } else if (is.matrix(imp)) {
    v <- imp[, ncol(imp)]
  } else {
    v <- imp
  }
  v[is.na(v)] <- 0
  v
}

# Average multiple importance vectors (union features; missing → 0)
average_importance <- function(imp_list) {
  all_feats <- unique(unlist(lapply(imp_list, names)))
  if (length(all_feats) == 0) return(setNames(numeric(0), character(0)))
  M <- do.call(cbind, lapply(imp_list, function(v) {
    out <- setNames(numeric(length(all_feats)), all_feats)
    out[names(v)] <- v
    out
  }))
  rowMeans(M, na.rm = TRUE)
}

# Elbow via cumulative-importance threshold (≥ threshold), enforce min_k and ≥ mtry
compute_k_elbow <- function(imp_vec, threshold = 0.90, min_k = 10, mtry = 1) {
  if (length(imp_vec) == 0) return(0L)
  s <- sort(imp_vec, decreasing = TRUE); s[is.na(s)] <- 0
  tot <- sum(s); if (tot <= 0) return(0L)
  cs <- cumsum(s) / tot
  k  <- which(cs >= threshold)[1]; if (is.na(k)) k <- length(s)
  k  <- max(k, min_k, mtry)
  k  <- min(k, length(s))
  as.integer(k)
}

# Confusion matrix helper: ensure same factor levels for pred & truth
make_cm <- function(pred, truth) {
  # For multiclass, caret::confusionMatrix handles without 'positive'
  caret::confusionMatrix(factor(pred, levels = levels(truth)), truth)
}


# Elbow via cumulative-importance threshold (≥ threshold), enforce min_k
compute_k_elbow <- function(imp_named_vec, threshold = 0.90, min_k = 10) {
  if (length(imp_named_vec) == 0) return(0L)
  s <- sort(imp_named_vec, decreasing = TRUE); s[!is.finite(s)] <- 0
  tot <- sum(s); if (tot <= 0) return(0L)
  cs <- cumsum(s) / tot
  k  <- which(cs >= threshold)[1]; if (is.na(k)) k <- length(s)
  k  <- max(k, min_k); k <- min(k, length(s))
  as.integer(k)
}


# Extract a named importance vector (Gain) from an xgb model
xgb_gain_vec <- function(model, feature_names) {
  imp <- try(xgb.importance(model = model, feature_names = feature_names), silent = TRUE)
  if (inherits(imp, "try-error") || is.null(imp) || !nrow(imp)) return(setNames(numeric(0), character(0)))
  setNames(imp$Gain, imp$Feature)
}


################################################################################
####################### Multinomial Logistic Regression ########################
################################################################################
# Multinomial Elastic Net with nested CV, topN KW features, fold-internal Huber,
# train-median imputation, and class×device upsampling (no leakage)

set.seed(26)

# -----------------------------
# Preconditions / setup
# -----------------------------
# Ensure outcome is a factor with consistent ordering
data$hyperactivity_baseline_cat <- factor(
  data$hyperactivity_baseline_cat,
  levels = c("normal", "borderline", "abnormal")
)

# Force device_type to be a simple two-level factor 
data$device_type <- factor(as.character(data$device_type))

#  precomputed KW ranking (descending by eps2)
ordered_feats <- kw_df_desc$feature

# Keep only those KW-ranked features that are actually numeric in current `data`
all_numeric <- names(data)[sapply(data, is.numeric)]
ordered_numeric <- ordered_feats[ordered_feats %in% all_numeric]

# Stratification label: outcome × device
data$y_dev <- interaction(data$hyperactivity_baseline_cat, data$device_type, drop = TRUE)

# -----------------------------
# Nested CV
# -----------------------------
K_OUTER    <- 10
K_INNER    <- 3
alpha_grid <- c(0.1, 0.5, 1.0)
topN_grid  <- c(30, 40, 50, 60, 70, 80)

outer_folds <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)

results <- vector("list", length = K_OUTER)
excluded_huber <- vector("list", length = K_OUTER)

for (o in seq_len(K_OUTER)) {
  message(sprintf("=== OUTER FOLD %d/%d ===", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  
  d_train0 <- data[train_idx, , drop = FALSE]
  d_test0  <- data[test_idx,  , drop = FALSE]
  
  # Inner CV splits on TRAIN only (stratified)
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  grid_scores  <- data.frame(alpha = numeric(), topN = integer(), err = numeric())
  grid_dropped <- list()  # key "a{alpha}_top{N}" -> per-inner-fold list of dropped features
  
  for (a in alpha_grid) {
    for (topN in topN_grid) {
      sel_feats <- head(ordered_numeric, topN)
      fold_err <- c()
      fold_dropped <- list()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        # Restrict to selected features that exist in this split
        sel_i <- sel_feats[sel_feats %in% colnames(tr)]
        
        # Huber outlier handling + normalization per device (fit on TR, apply to TR & VL)
        trans <- fit_huber_by_device(
          tr[, c(sel_i, "hyperactivity_baseline_cat", "device_type"), drop = FALSE],
          vl[, c(sel_i, "hyperactivity_baseline_cat", "device_type"), drop = FALSE],
          device_col = "device_type",
          numeric_cols = sel_i
        )
        
        tr2 <- trans$train
        vl2 <- trans$test
        
        # Features surviving Huber transform on both TR and VL
        sel_after_huber <- intersect(sel_i, intersect(colnames(tr2), colnames(vl2)))
        if (!length(sel_after_huber)) { fold_err <- c(fold_err, 1.0); fold_dropped[[i]] <- trans$dropped; next }
        
        # Impute using TRAIN medians; apply to VALIDATION
        imp <- impute_by_train_median(
          tr2[, sel_after_huber, drop = FALSE],
          vl2[, sel_after_huber, drop = FALSE],
          cols = sel_after_huber
        )
        tr2_imp <- tr2; vl2_imp <- vl2
        tr2_imp[, sel_after_huber] <- imp$train
        vl2_imp[, sel_after_huber] <- imp$test
        
        # Balance TRAIN by (class × device) AFTER imputation
        tr2b <- upsample_by_interaction(
          cbind(tr2_imp[, sel_after_huber, drop = FALSE],
                hyperactivity_baseline_cat = tr2$hyperactivity_baseline_cat,
                device_type = tr2$device_type),
          y_col = "hyperactivity_baseline_cat",
          device_col = "device_type"
        )
        
        # Align features again
        keep_feats <- intersect(sel_after_huber, intersect(colnames(tr2b), colnames(vl2_imp)))
        if (!length(keep_feats)) { fold_err <- c(fold_err, 1.0); fold_dropped[[i]] <- trans$dropped; next }
        
        # Build matrices
        X_tr <- as.matrix(as.data.frame(tr2b[, keep_feats, drop = FALSE]))
        X_vl <- as.matrix(as.data.frame(vl2_imp[, keep_feats, drop = FALSE]))
        y_tr <- droplevels(tr2b$hyperactivity_baseline_cat)
        y_vl <- droplevels(vl2_imp$hyperactivity_baseline_cat)
        
        # Fit (λ tuned internally)
        cv_fit <- cv.glmnet(
          X_tr, y_tr,
          family = "multinomial",
          alpha = a,
          type.measure = "class",
          maxit = 1e6,
          nlambda = 100,
          lambda.min.ratio = 0.01
        )
        
        pred <- predict(cv_fit, newx = X_vl, s = "lambda.min", type = "class")
        acc  <- mean(pred == y_vl)
        fold_err <- c(fold_err, 1 - acc)
        
        fold_dropped[[i]] <- trans$dropped
      }
      
      key <- paste0("a", a, "_top", topN)
      grid_scores <- rbind(grid_scores, data.frame(alpha = a, topN = topN, err = mean(fold_err)))
      grid_dropped[[key]] <- fold_dropped
    }
  }
  
  # Best setting (lowest mean error)
  best_idx   <- which.min(grid_scores$err)
  best_alpha <- grid_scores$alpha[best_idx]
  best_topN  <- grid_scores$topN[best_idx]
  
  # -----------------------------
  # Refit on full TRAIN of outer fold with best alpha/topN
  # -----------------------------
  sel_feats_outer <- head(ordered_numeric, best_topN)
  
  trans_outer <- fit_huber_by_device(
    d_train0[, c(sel_feats_outer, "hyperactivity_baseline_cat", "device_type"), drop = FALSE],
    d_test0 [, c(sel_feats_outer, "hyperactivity_baseline_cat", "device_type"), drop = FALSE],
    device_col = "device_type",
    numeric_cols = sel_feats_outer
  )
  d_train2 <- trans_outer$train
  d_test2  <- trans_outer$test
  
  sel_outer_after_huber <- intersect(sel_feats_outer, intersect(colnames(d_train2), colnames(d_test2)))
  if (!length(sel_outer_after_huber)) {
    acc_outer <- NA_real_; nz_total <- 0; selected_features <- character(0)
  } else {
    # Impute using TRAIN medians; apply to TEST
    imp_outer <- impute_by_train_median(
      d_train2[, sel_outer_after_huber, drop = FALSE],
      d_test2 [, sel_outer_after_huber, drop = FALSE],
      cols = sel_outer_after_huber
    )
    d_train2_imp <- d_train2; d_test2_imp <- d_test2
    d_train2_imp[, sel_outer_after_huber] <- imp_outer$train
    d_test2_imp [, sel_outer_after_huber] <- imp_outer$test
    
    # Balance TRAIN by interaction AFTER imputation
    d_train2b <- upsample_by_interaction(
      cbind(d_train2_imp[, sel_outer_after_huber, drop = FALSE],
            hyperactivity_baseline_cat = d_train2$hyperactivity_baseline_cat,
            device_type = d_train2$device_type),
      y_col = "hyperactivity_baseline_cat",
      device_col = "device_type"
    )
    
    keep_feats_outer <- intersect(sel_outer_after_huber,
                                  intersect(colnames(d_train2b), colnames(d_test2_imp)))
    if (!length(keep_feats_outer)) {
      acc_outer <- NA_real_; nz_total <- 0; selected_features <- character(0)
    } else {
      X_train <- as.matrix(as.data.frame(d_train2b[, keep_feats_outer, drop = FALSE]))
      X_test  <- as.matrix(as.data.frame(d_test2_imp[, keep_feats_outer, drop = FALSE]))
      y_train <- droplevels(d_train2b$hyperactivity_baseline_cat)
      y_test  <- droplevels(d_test2_imp$hyperactivity_baseline_cat)
      
      cv_fit_outer <- cv.glmnet(
        X_train, y_train,
        family = "multinomial",
        alpha = best_alpha,
        type.measure = "class",
        maxit = 1e6,
        nlambda = 100,
        lambda.min.ratio = 0.01
      )
      
      preds <- predict(cv_fit_outer, newx = X_test, s = "lambda.min", type = "class")
      acc_outer <- mean(preds == y_test)
      
      final_fit <- glmnet(
        X_train, y_train,
        family = "multinomial",
        alpha = best_alpha,
        lambda = cv_fit_outer$lambda.min,
        maxit = 1e6
      )
      coefs <- coef(final_fit)
      selected_features <- unique(unlist(lapply(coefs, function(mat) rownames(mat)[mat[,1] != 0])))
      selected_features <- setdiff(selected_features, "(Intercept)")
      nz_total <- length(selected_features)
    }
  }
  
  # Save per-fold summary + dropped features
  results[[o]] <- list(
    fold = o,
    best_alpha = best_alpha,
    best_topN  = best_topN,
    acc        = acc_outer,
    nonzero_total = nz_total,
    selected      = selected_features,
    counts_test_combo  = table(d_test0$hyperactivity_baseline_cat, d_test0$device_type),
    counts_train_combo = table(d_train2b$hyperactivity_baseline_cat, d_train2b$device_type)
  )
  excluded_huber[[o]] <- list(
    inner = grid_dropped,
    outer = trans_outer$dropped
  )
}

# -----------------------------
# Diagnostics / outputs
# -----------------------------
accs       <- sapply(results, `[[`, "acc")
best_alpha <- sapply(results, `[[`, "best_alpha")
best_topN  <- sapply(results, `[[`, "best_topN")
nz_total   <- sapply(results, `[[`, "nonzero_total")

selected_summary <- tibble(
  fold     = seq_along(results),
  acc      = sapply(results, `[[`, "acc"),
  alpha    = sapply(results, `[[`, "best_alpha"),
  topN     = sapply(results, `[[`, "best_topN"),
  nonzero  = sapply(results, function(r) length(r$selected)),
  features = I(lapply(results, `[[`, "selected"))
)
print(selected_summary)

# save dropped-feature map for inspection
saveRDS(excluded_huber, file = "excluded_features_huber_nestedcv.rds")

lapply(results, `[[`, "counts_train_combo")
lapply(results, `[[`, "counts_test_combo")

# Gather per-fold selected features
per_fold_feats <- lapply(results, `[[`, "selected")

# 1) Union and intersection across all folds
feat_union       <- Reduce(union,   per_fold_feats)
feat_intersection<- Reduce(intersect, per_fold_feats)

length(feat_union)        # total unique features selected in any fold
length(feat_intersection) # features selected in every fold
feat_intersection         # list them

# 2) Frequency of selection across folds (stability)
freq <- sort(table(unlist(per_fold_feats)), decreasing = TRUE)
head(freq, 20)            # top 20 most frequently selected
# Get features appearing in >= k folds 
stable_k <- function(k) names(freq[freq >= k])
stable_3plus <- stable_k(3)  # example: in at least 3 folds - can change 
length(stable_3plus); head(stable_3plus)

# 3) Per-fold counts & quick summary table
summary_stability <- data.frame(
  fold    = seq_along(per_fold_feats),
  nonzero = sapply(per_fold_feats, length)
)
summary_stability

# 4) Jaccard similarity between folds (overlap / union)
jaccard <- function(a, b) length(intersect(a, b)) / length(union(a, b))
F <- length(per_fold_feats)
J <- matrix(NA_real_, F, F, dimnames = list(paste0("Fold",1:F), paste0("Fold",1:F)))
for (i in 1:F) for (j in 1:F) J[i,j] <- jaccard(per_fold_feats[[i]], per_fold_feats[[j]])
round(J, 3)  # Jaccard matrix

# 5) Tidy table listing features per fold (long format)
tidy_per_fold <- tibble(
  fold = rep(seq_along(per_fold_feats), times = sapply(per_fold_feats, length)),
  feature = unlist(per_fold_feats)
)

# How many folds each feature appears in
feature_stability <- tidy_per_fold %>%
  count(feature, name = "fold_count") %>%
  arrange(desc(fold_count))

head(feature_stability, 20)

# 6)Write to CSV for reporting
write.csv(feature_stability, "feature_stability_across_folds.csv", row.names = FALSE)
write.csv(tidy_per_fold, "features_by_fold_long.csv", row.names = FALSE)

# 7)Show, for each fold, the exact feature list
split(tidy_per_fold$feature, tidy_per_fold$fold)








################################################################################
####################### Random Forest : Model A & B ############################
################################################################################
# Model A (KW-only): 
# uses your precomputed KW-ranked features; tunes topN, ntree, mtry.

# Model B (KW + RF-pruning):
#starts from KW topN inside the inner folds, 
# then uses RF’s internal feature importance to prune to top-k (also tuned).

set.seed(26)

# Stratification label (class × device)
data$y_dev <- interaction(data$hyperactivity_baseline_cat, data$device_type, drop = TRUE)

#========================
# Grids / CV
#========================

K_OUTER <- 10
K_INNER <- 3

topN_grid  <- c(30, 40, 50, 60, 70, 80, 100, 120)
ntree_grid <- c(500, 1000)
mtry_grid_opt <- NULL   # if not NULL, use this vector instead of adaptive choices

outer_folds <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)
rfA_results <- vector("list", K_OUTER)  # Model A (KW-only)
rfB_results <- vector("list", K_OUTER)  # Model B (KW + elbow pruning)

#========================
# Outer loop
#========================
for (o in seq_len(K_OUTER)) {
  cat(sprintf("=== OUTER FOLD %d/%d ===\n", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  d_train0  <- data[train_idx, , drop = FALSE]
  d_test0   <- data[test_idx,  , drop = FALSE]
  
  # Inner folds on training split (stratified)
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  #========================
  # Model A: KW-only
  #========================
  gridA <- tibble(topN = integer(), ntree = integer(), mtry = integer(), err = numeric())
  
  for (topN in topN_grid) {
    sel_feats <- head(ordered_numeric, topN)
    mtry_vals <- if (is.null(mtry_grid_opt)) unique(c(max(1, floor(sqrt(topN))),
                                                      max(1, floor(topN / 3))))
    else mtry_grid_opt
    
    for (ntree in ntree_grid) for (mtry in mtry_vals) {
      inner_err <- c()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        keep_feats <- intersect(sel_feats, colnames(tr))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        # Impute (train medians) -> apply to val
        imp <- impute_by_train_median(tr[, keep_feats, drop = FALSE],
                                      vl[, keep_feats, drop = FALSE],
                                      keep_feats)
        trX <- imp$train; vlX <- imp$test
        
        # Upsample train by class × device
        tr_up <- upsample_by_interaction(cbind(trX,
                                               hyperactivity_baseline_cat = tr$hyperactivity_baseline_cat,
                                               device_type = tr$device_type))
        
        y_tr <- droplevels(tr_up$hyperactivity_baseline_cat)
        y_vl <- droplevels(vl$hyperactivity_baseline_cat)
        X_tr <- tr_up[, keep_feats, drop = FALSE]
        X_vl <- vlX [, keep_feats, drop = FALSE]
        
        rf_fit <- randomForest(x = X_tr, y = y_tr,
                               ntree = ntree, mtry = mtry,
                               importance = TRUE)
        pred <- predict(rf_fit, newdata = X_vl)
        inner_err <- c(inner_err, 1 - mean(pred == y_vl))
      }
      
      gridA <- add_row(gridA, topN = topN, ntree = ntree, mtry = mtry,
                       err = mean(inner_err))
    }
  }
  
  # Best settings for Model A
  bestA_idx   <- which.min(gridA$err)
  bestA_topN  <- gridA$topN [bestA_idx]
  bestA_ntree <- gridA$ntree[bestA_idx]
  bestA_mtry  <- gridA$mtry [bestA_idx]
  
  # Refit A on full outer train → eval on outer test
  A_feats <- intersect(head(ordered_numeric, bestA_topN), colnames(d_train0))
  impA <- impute_by_train_median(d_train0[, A_feats, drop = FALSE],
                                 d_test0 [, A_feats, drop = FALSE],
                                 A_feats)
  A_trX <- impA$train; A_teX <- impA$test
  A_tr_up <- upsample_by_interaction(cbind(A_trX,
                                           hyperactivity_baseline_cat = d_train0$hyperactivity_baseline_cat,
                                           device_type = d_train0$device_type))
  yA_tr <- droplevels(A_tr_up$hyperactivity_baseline_cat)
  yA_te <- droplevels(d_test0$hyperactivity_baseline_cat)
  XA_tr <- A_tr_up[, A_feats, drop = FALSE]
  XA_te <- A_teX [, A_feats, drop = FALSE]
  
  rfA_final <- randomForest(x = XA_tr, y = yA_tr,
                            ntree = bestA_ntree, mtry = bestA_mtry,
                            importance = TRUE)
  predA <- predict(rfA_final, newdata = XA_te)
  accA  <- mean(predA == yA_te)
  cmA   <- make_cm(predA, yA_te)
  
  rfA_results[[o]] <- list(
    fold = o, acc = accA,
    topN = bestA_topN, ntree = bestA_ntree, mtry = bestA_mtry,
    features = A_feats,
    importance = importance(rfA_final),
    confusion_matrix = cmA
  )
  
  #========================
  # Model B: KW → RF importance → elbow pruning
  #========================
  gridB <- tibble(topN = integer(), ntree = integer(), mtry = integer(),
                  err = numeric(), mean_k_elbow = integer())
  
  candidate_imp_store <- list()  # key -> list(inner_fold_importance)
  
  for (topN in topN_grid) {
    sel_feats <- head(ordered_numeric, topN)
    mtry_vals <- if (is.null(mtry_grid_opt)) unique(c(max(1, floor(sqrt(topN))),
                                                      max(1, floor(topN / 3))))
    else mtry_grid_opt
    
    for (ntree in ntree_grid) for (mtry in mtry_vals) {
      inner_err  <- c()
      inner_imps <- list()
      inner_ks   <- c()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        keep_feats <- intersect(sel_feats, colnames(tr))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        # Impute (train medians) -> apply to val
        imp <- impute_by_train_median(tr[, keep_feats, drop = FALSE],
                                      vl[, keep_feats, drop = FALSE],
                                      keep_feats)
        trX <- imp$train; vlX <- imp$test
        
        # Upsample train by class × device
        tr_up <- upsample_by_interaction(cbind(trX,
                                               hyperactivity_baseline_cat = tr$hyperactivity_baseline_cat,
                                               device_type = tr$device_type))
        
        y_tr <- droplevels(tr_up$hyperactivity_baseline_cat)
        y_vl <- droplevels(vl$hyperactivity_baseline_cat)
        X_tr <- tr_up[, keep_feats, drop = FALSE]
        X_vl <- vlX [, keep_feats, drop = FALSE]
        
        # Stage 1: RF on KW pool → importance
        rf_stage1 <- randomForest(x = X_tr, y = y_tr,
                                  ntree = ntree, mtry = mtry,
                                  importance = TRUE)
        imp_vec <- get_imp_vec(rf_stage1)
        inner_imps[[i]] <- imp_vec
        
        # Elbow k on cumulative importance (≥ 90%), enforce ≥ mtry and ≥ 10
        k_elbow <- compute_k_elbow(imp_vec, threshold = 0.90, min_k = 10, mtry = mtry)
        inner_ks <- c(inner_ks, k_elbow)
        
        # Prune to top k_elbow and refit
        imp_ranked  <- sort(imp_vec, decreasing = TRUE)
        prune_feats <- intersect(names(head(imp_ranked, k_elbow)), colnames(X_tr))
        if (!length(prune_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        rf_stage2 <- randomForest(x = X_tr[, prune_feats, drop = FALSE],
                                  y = y_tr,
                                  ntree = ntree,
                                  mtry  = min(mtry, length(prune_feats)),
                                  importance = FALSE)
        pred <- predict(rf_stage2, newdata = X_vl[, prune_feats, drop = FALSE])
        inner_err <- c(inner_err, 1 - mean(pred == y_vl))
      } # inner folds
      
      key <- paste0("topN", topN, "_nt", ntree, "_mt", mtry)
      candidate_imp_store[[key]] <- inner_imps
      
      gridB <- add_row(gridB,
                       topN = topN, ntree = ntree, mtry = mtry,
                       err = mean(inner_err),
                       mean_k_elbow = round(mean(inner_ks, na.rm = TRUE)))
    }
  }
  
  # Best (topN, ntree, mtry) by mean inner error
  bestB_idx   <- which.min(gridB$err)
  bestB_topN  <- gridB$topN [bestB_idx]
  bestB_ntree <- gridB$ntree[bestB_idx]
  bestB_mtry  <- gridB$mtry [bestB_idx]
  best_key    <- paste0("topN", bestB_topN, "_nt", bestB_ntree, "_mt", bestB_mtry)
  
  # Average inner importances for the best candidate → GLOBAL ranking
  best_imp_list <- candidate_imp_store[[best_key]]
  best_imp_list <- best_imp_list[vapply(best_imp_list, function(x) length(x) > 0, logical(1))]
  global_imp    <- average_importance(best_imp_list)
  global_rank   <- sort(global_imp, decreasing = TRUE)
  
  # GLOBAL elbow k on averaged curve (≥ 90%), enforce ≥ mtry and ≥ 10
  k_global <- compute_k_elbow(global_rank, threshold = 0.90, min_k = 10, mtry = bestB_mtry)
  
  # Final feature set for outer refit: restrict to KW pool, then take top-k_global by global RF rank
  B_pool      <- head(ordered_numeric, bestB_topN)
  B_ranked    <- global_rank[names(global_rank) %in% B_pool]
  B_finalfeat <- names(head(B_ranked, k_global))
  
  # Refit on full outer train → eval on outer test
  if (length(B_finalfeat) == 0) {
    accB <- NA_real_
    cmB  <- NA
  } else {
    impB <- impute_by_train_median(d_train0[, B_finalfeat, drop = FALSE],
                                   d_test0 [, B_finalfeat, drop = FALSE],
                                   B_finalfeat)
    B_trX <- impB$train; B_teX <- impB$test
    B_tr_up <- upsample_by_interaction(cbind(B_trX,
                                             hyperactivity_baseline_cat = d_train0$hyperactivity_baseline_cat,
                                             device_type = d_train0$device_type))
    yB_tr <- droplevels(B_tr_up$hyperactivity_baseline_cat)
    yB_te <- droplevels(d_test0$hyperactivity_baseline_cat)
    XB_tr <- B_tr_up[, B_finalfeat, drop = FALSE]
    XB_te <- B_teX [, B_finalfeat, drop = FALSE]
    
    rfB_final <- randomForest(x = XB_tr, y = yB_tr,
                              ntree = bestB_ntree,
                              mtry  = min(bestB_mtry, length(B_finalfeat)),
                              importance = TRUE)
    predB <- predict(rfB_final, newdata = XB_te)
    accB  <- mean(predB == yB_te)
    cmB   <- make_cm(predB, yB_te)
  }
  
  rfB_results[[o]] <- list(
    fold = o, acc = accB,
    topN_pool = bestB_topN, k_global = k_global,
    ntree = bestB_ntree, mtry = bestB_mtry,
    global_importance = global_rank,
    final_features = B_finalfeat,
    confusion_matrix = cmB
  )
}

#========================
# Summaries
#========================

rfA_summary <- tibble(
  fold   = seq_len(K_OUTER),
  acc    = sapply(rfA_results, `[[`, "acc"),
  topN   = sapply(rfA_results, `[[`, "topN"),
  ntree  = sapply(rfA_results, `[[`, "ntree"),
  mtry   = sapply(rfA_results, `[[`, "mtry"),
  used_p = sapply(rfA_results, function(x) length(x$features))
)

rfB_summary <- tibble(
  fold   = seq_len(K_OUTER),
  acc    = sapply(rfB_results, `[[`, "acc"),
  topN   = sapply(rfB_results, `[[`, "topN_pool"),
  k_glob = sapply(rfB_results, `[[`, "k_global"),
  ntree  = sapply(rfB_results, `[[`, "ntree"),
  mtry   = sapply(rfB_results, `[[`, "mtry"),
  used_p = sapply(rfB_results, function(x) length(x$final_features))
)

print(rfA_summary)
print(rfB_summary)

# Access per-fold confusion matrices like:
rfA_results[[1]]$confusion_matrix
rfB_results[[1]]$confusion_matrix



importance = importance(rfA_final)

# Collect all feature importance vectors from Model A
impA_all <- lapply(rfA_results, function(x) {
  im <- x$importance[, "MeanDecreaseGini"]
  setNames(im, rownames(x$importance))
})

# Union of all features across folds
all_featsA <- unique(unlist(lapply(impA_all, names)))
# Build a matrix: rows = features, cols = folds (missing → 0)
M_A <- do.call(cbind, lapply(impA_all, function(v) {
  out <- setNames(numeric(length(all_featsA)), all_featsA)
  out[names(v)] <- v
  out
}))
# Average importance across folds
avg_impA <- sort(rowMeans(M_A, na.rm = TRUE), decreasing = TRUE)
# Top 20 features
head(avg_impA, 20)




# Collect global_importance from Model B
impB_all <- lapply(rfB_results, function(x) x$global_importance)

# Union of features across folds
all_featsB <- unique(unlist(lapply(impB_all, names)))

# Build a matrix
M_B <- do.call(cbind, lapply(impB_all, function(v) {
  out <- setNames(numeric(length(all_featsB)), all_featsB)
  out[names(v)] <- v
  out
}))
# Average importance
avg_impB <- sort(rowMeans(M_B, na.rm = TRUE), decreasing = TRUE)
# Top 20 features
head(avg_impB, 20)









################################################################################
############################### XGBoost: Model A (KW Only) #####################
################################################################################
set.seed(26)

K_OUTER <- 10
K_INNER <- 3

topN_grid   <- c(30, 40, 60, 80, 100, 120)
nrounds_grid <- c(200, 500)
eta_grid     <- c(0.05, 0.1)
depth_grid   <- c(3, 5, 7)

outer_folds <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)
xgbA_results <- vector("list", K_OUTER)

for (o in seq_len(K_OUTER)) {
  cat(sprintf("=== OUTER FOLD %d/%d ===\n", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  
  d_train0 <- data[train_idx, , drop = FALSE]
  d_test0  <- data[test_idx,  , drop = FALSE]
  
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  gridA <- tibble(topN=integer(), nrounds=integer(), eta=numeric(),
                  depth=integer(), err=numeric())
  
  for (topN in topN_grid) {
    sel_feats <- head(ordered_numeric, topN)
    for (nrounds in nrounds_grid) {
      for (eta in eta_grid) {
        for (depth in depth_grid) {
          inner_err <- c()
          
          for (i in seq_len(K_INNER)) {
            val_idx <- inner_folds[[i]]
            tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
            tr <- d_train0[tr_idx, , drop = FALSE]
            vl <- d_train0[val_idx, , drop = FALSE]
            
            keep_feats <- intersect(sel_feats, colnames(tr))
            if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
            
            # Impute
            imp <- impute_by_train_median(tr[, keep_feats, drop=FALSE],
                                          vl[, keep_feats, drop=FALSE],
                                          keep_feats)
            trX <- as.matrix(imp$train)
            vlX <- as.matrix(imp$test)
            y_tr <- as.numeric(tr$hyperactivity_baseline_cat) - 1
            y_vl <- as.numeric(vl$hyperactivity_baseline_cat) - 1
            
            dtrain <- xgb.DMatrix(trX, label=y_tr)
            dval   <- xgb.DMatrix(vlX, label=y_vl)
            
            fit <- xgb.train(list(
              objective="multi:softmax", 
              num_class=length(levels(data$hyperactivity_baseline_cat)),
              eval_metric="merror",
              eta=eta, max_depth=depth
            ), dtrain, nrounds=nrounds, verbose=0)
            
            pred <- predict(fit, dval)
            inner_err <- c(inner_err, mean(pred != y_vl))
          }
          
          gridA <- add_row(gridA, topN=topN, nrounds=nrounds,
                           eta=eta, depth=depth, err=mean(inner_err))
        }
      }
    }
  }
  
  # Best params
  best_idx <- which.min(gridA$err)
  best     <- gridA[best_idx, ]
  sel_feats <- head(ordered_numeric, best$topN)
  
  keep_feats <- intersect(sel_feats, colnames(d_train0))
  imp_outer <- impute_by_train_median(d_train0[, keep_feats, drop=FALSE],
                                      d_test0[, keep_feats, drop=FALSE],
                                      keep_feats)
  trX <- as.matrix(imp_outer$train)
  teX <- as.matrix(imp_outer$test)
  y_tr <- as.numeric(d_train0$hyperactivity_baseline_cat) - 1
  y_te <- as.numeric(d_test0$hyperactivity_baseline_cat) - 1
  
  dtrain <- xgb.DMatrix(trX, label=y_tr)
  dtest  <- xgb.DMatrix(teX, label=y_te)
  
  xgb_final <- xgb.train(list(
    objective="multi:softmax", 
    num_class=length(levels(data$hyperactivity_baseline_cat)),
    eval_metric="merror",
    eta=best$eta, max_depth=best$depth
  ), dtrain, nrounds=best$nrounds, verbose=0)
  
  pred <- predict(xgb_final, dtest)
  acc  <- mean(pred == y_te)
  
  imp  <- xgb.importance(model=xgb_final, feature_names=keep_feats)
  
  xgbA_results[[o]] <- list(
    fold=o, acc=acc, best=best,
    importance=imp
  )
}

# Summary
xgbA_summary <- tibble(
  fold=seq_len(K_OUTER),
  acc =sapply(xgbA_results, `[[`, "acc"),
  topN=sapply(xgbA_results, function(x) x$best$topN)
)
print(xgbA_summary)



# Collect importance by Gain
imp_list <- lapply(xgbA_results, function(x) {
  if (is.null(x$importance)) return(NULL)
  v <- setNames(x$importance$Gain, x$importance$Feature)
  v
})

# Average importance across folds
all_feats <- unique(unlist(lapply(imp_list, names)))
M <- do.call(cbind, lapply(imp_list, function(v) {
  out <- setNames(numeric(length(all_feats)), all_feats)
  out[names(v)] <- v
  out
}))
avg_imp <- sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE)

head(avg_imp, 20)



################################################################################
############ XGBoost: Model B (KW + XGB importance + elbow pruning) ############
################################################################################
set.seed(26)

#---------------- Grids / CV -------------------

K_OUTER <- 10
K_INNER <- 3

topN_grid     <- c(30, 40, 50, 60, 80, 100, 120)
nrounds_grid  <- c(200, 500)
eta_grid      <- c(0.05, 0.1)
depth_grid    <- c(3, 5, 7)

outer_folds <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)
xgbB_results <- vector("list", K_OUTER)

#---------------- Outer loop -------------------
for (o in seq_len(K_OUTER)) {
  cat(sprintf("=== OUTER FOLD %d/%d ===\n", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  d_train0  <- data[train_idx, , drop = FALSE]
  d_test0   <- data[test_idx,  , drop = FALSE]
  
  # Label encodings for xgboost
  class_levels <- levels(d_train0$hyperactivity_baseline_cat)
  y_tr_num <- as.numeric(d_train0$hyperactivity_baseline_cat) - 1L
  y_te_num <- as.numeric(d_test0$hyperactivity_baseline_cat) - 1L
  num_class <- length(class_levels)
  
  # Inner folds (stratified on y_dev)
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  gridB <- tibble(topN=integer(), nrounds=integer(), eta=numeric(), depth=integer(),
                  err=numeric(), mean_k_elbow=integer())
  candidate_imp_store <- list()  # key -> list(inner importance Gain vectors)
  
  #----------------- Inner tuning: (topN, nrounds, eta, depth) -----------------
  for (topN in topN_grid) {
    sel_pool <- head(ordered_numeric, topN)
    for (nrounds in nrounds_grid) for (eta in eta_grid) for (depth in depth_grid) {
      inner_err  <- c()
      inner_imps <- list()
      inner_ks   <- c()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        keep_feats <- intersect(sel_pool, intersect(colnames(tr), colnames(vl)))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        # Impute using TR medians
        imp <- impute_by_train_median(tr[, keep_feats, drop = FALSE],
                                      vl[, keep_feats, drop = FALSE],
                                      keep_feats)
        trX <- imp$train; vlX <- imp$test
        
        # Upsample TRAIN by interaction (on data frame, then matrix)
        tr_up <- upsample_by_interaction(cbind(trX,
                                               hyperactivity_baseline_cat = tr$hyperactivity_baseline_cat,
                                               device_type = tr$device_type))
        
        # Build matrices for xgboost
        X_tr <- as.matrix(tr_up[, keep_feats, drop = FALSE])
        X_vl <- as.matrix(vlX   [, keep_feats, drop = FALSE])
        y_tr <- as.numeric(tr_up$hyperactivity_baseline_cat) - 1L
        y_vl <- as.numeric(vl$hyperactivity_baseline_cat) - 1L
        
        dtrain <- xgb.DMatrix(X_tr, label = y_tr)
        dval   <- xgb.DMatrix(X_vl, label = y_vl)
        
        # Stage 1: train on KW pool → importance (Gain)
        fit_stage1 <- xgb.train(
          params = list(
            objective = "multi:softprob",
            num_class = num_class,
            eval_metric = "mlogloss",
            eta = eta, max_depth = depth,
            subsample = 0.8, colsample_bytree = 0.8
          ),
          data = dtrain, nrounds = nrounds, verbose = 0
        )
        imp_vec <- xgb_gain_vec(fit_stage1, feature_names = keep_feats)
        inner_imps[[i]] <- imp_vec
        
        # Elbow k on cumulative Gain (≥ 90%)
        k_elbow <- compute_k_elbow(imp_vec, threshold = 0.90, min_k = 10)
        inner_ks <- c(inner_ks, k_elbow)
        
        # Prune to top-k by Gain, refit stage 2
        imp_ranked  <- sort(imp_vec, decreasing = TRUE)
        prune_feats <- intersect(names(head(imp_ranked, k_elbow)), colnames(X_tr))
        if (!length(prune_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        dtrain2 <- xgb.DMatrix(as.matrix(tr_up[, prune_feats, drop = FALSE]), label = y_tr)
        dval2   <- xgb.DMatrix(as.matrix(vlX   [, prune_feats, drop = FALSE]), label = y_vl)
        
        fit_stage2 <- xgb.train(
          params = list(
            objective = "multi:softprob",
            num_class = num_class,
            eval_metric = "mlogloss",
            eta = eta, max_depth = depth,
            subsample = 0.8, colsample_bytree = 0.8
          ),
          data = dtrain2, nrounds = nrounds, verbose = 0
        )
        pred <- matrix(predict(fit_stage2, dval2), ncol = num_class, byrow = TRUE)
        pred_class <- max.col(pred) - 1L
        inner_err <- c(inner_err, mean(pred_class != y_vl))
      } # inner folds
      
      key <- paste0("topN", topN, "_nr", nrounds, "_eta", eta, "_md", depth)
      candidate_imp_store[[key]] <- inner_imps
      
      gridB <- add_row(gridB,
                       topN = topN, nrounds = nrounds, eta = eta, depth = depth,
                       err = mean(inner_err),
                       mean_k_elbow = round(mean(inner_ks, na.rm = TRUE))
      )
    }
  }
  
  #----------------- Pick best combo by inner error -----------------
  best_idx <- which.min(gridB$err)
  best     <- gridB[best_idx, ]
  best_key <- paste0("topN", best$topN, "_nr", best$nrounds, "_eta", best$eta, "_md", best$depth)
  
  # Average inner importances (best combo) → global ranking
  best_imp_list <- candidate_imp_store[[best_key]]
  best_imp_list <- best_imp_list[vapply(best_imp_list, function(x) length(x) > 0, logical(1))]
  global_imp    <- average_importance(best_imp_list)
  global_rank   <- sort(global_imp, decreasing = TRUE)
  
  # Global elbow on averaged curve (≥ 90%)
  k_global <- compute_k_elbow(global_rank, threshold = 0.90, min_k = 10)
  
  # Final feature set: restrict to best KW pool, then take top-k_global by global Gain
  B_pool      <- head(ordered_numeric, best$topN)
  B_ranked    <- global_rank[names(global_rank) %in% B_pool]
  B_finalfeat <- names(head(B_ranked, k_global))
  
  #----------------- Refit on full outer TRAIN and evaluate on TEST -----------------
  if (!length(B_finalfeat)) {
    accB <- NA_real_; cmB <- NA
  } else {
    imp_outer <- impute_by_train_median(d_train0[, B_finalfeat, drop = FALSE],
                                        d_test0 [, B_finalfeat, drop = FALSE],
                                        B_finalfeat)
    B_trX <- imp_outer$train; B_teX <- imp_outer$test
    B_tr_up <- upsample_by_interaction(cbind(B_trX,
                                             hyperactivity_baseline_cat = d_train0$hyperactivity_baseline_cat,
                                             device_type = d_train0$device_type))
    
    X_train <- as.matrix(B_tr_up[, B_finalfeat, drop = FALSE])
    X_test  <- as.matrix(B_teX   [, B_finalfeat, drop = FALSE])
    y_train <- as.numeric(B_tr_up$hyperactivity_baseline_cat) - 1L
    y_test  <- as.numeric(d_test0$hyperactivity_baseline_cat) - 1L
    
    dtrain_final <- xgb.DMatrix(X_train, label = y_train)
    dtest_final  <- xgb.DMatrix(X_test,  label = y_test)
    
    xgb_final <- xgb.train(
      params = list(
        objective = "multi:softprob",
        num_class = num_class,
        eval_metric = "mlogloss",
        eta = best$eta, max_depth = best$depth,
        subsample = 0.8, colsample_bytree = 0.8
      ),
      data = dtrain_final, nrounds = best$nrounds, verbose = 0
    )
    
    prob  <- matrix(predict(xgb_final, dtest_final), ncol = num_class, byrow = TRUE)
    pred_class <- factor(class_levels[max.col(prob)], levels = class_levels)
    truth      <- droplevels(d_test0$hyperactivity_baseline_cat)
    
    accB <- mean(pred_class == truth)
    cmB  <- make_cm(pred_class, truth)
  }
  
  xgbB_results[[o]] <- list(
    fold = o,
    acc  = accB,
    topN_pool = best$topN,
    nrounds   = best$nrounds,
    eta       = best$eta,
    depth     = best$depth,
    k_global  = k_global,
    global_importance = global_rank,
    final_features    = B_finalfeat,
    confusion_matrix  = cmB
  )
}

#---------------- Summary ----------------
xgbB_summary <- tibble(
  fold   = seq_len(K_OUTER),
  acc    = sapply(xgbB_results, `[[`, "acc"),
  topN   = sapply(xgbB_results, `[[`, "topN_pool"),
  k_glob = sapply(xgbB_results, `[[`, "k_global"),
  nrounds= sapply(xgbB_results, `[[`, "nrounds"),
  eta    = sapply(xgbB_results, `[[`, "eta"),
  depth  = sapply(xgbB_results, `[[`, "depth"),
  used_p = sapply(xgbB_results, function(x) length(x$final_features))
)
print(xgbB_summary)

# Access per-fold confusion matrices:
# xgbB_results[[1]]$confusion_matrix

#---------------- Averaged feature importance across folds (Gain) --------------
impB_all <- lapply(xgbB_results, `[[`, "global_importance")
all_feats <- unique(unlist(lapply(impB_all, names)))
M <- do.call(cbind, lapply(impB_all, function(v) {
  out <- setNames(numeric(length(all_feats)), all_feats)
  out[names(v)] <- v; out
}))
avg_gain <- sort(rowMeans(M, na.rm = TRUE), decreasing = TRUE)
head(avg_gain, 20)

# Optionally save:
# saveRDS(list(B = xgbB_results, summary = xgbB_summary), "xgb_modelB_elbow_results.rds")




# Model A
accA <- sapply(xgbA_results, `[[`, "acc")
mean_accA <- mean(accA, na.rm=TRUE)
sd_accA   <- sd(accA,   na.rm=TRUE)
ciA <- mean_accA + qt(c(0.025, 0.975), df=length(accA)-1) * sd_accA/sqrt(length(accA))

# Model B
accB <- sapply(xgbB_results, `[[`, "acc")
mean_accB <- mean(accB, na.rm=TRUE)
sd_accB   <- sd(accB,   na.rm=TRUE)
ciB <- mean_accB + qt(c(0.025, 0.975), df=length(accB)-1) * sd_accB/sqrt(length(accB))




# Extract per-fold metrics from a caret confusionMatrix
fold_metrics_from_cm <- function(cm) {
  by <- as.data.frame(cm$byClass)
  prec <- by$`Pos Pred Value`
  rec  <- by$Sensitivity
  f1   <- ifelse(prec + rec == 0, NA_real_, 2*prec*rec/(prec+rec))
  bal  <- (by$Sensitivity + by$Specificity)/2
  
  tibble(
    accuracy          = as.numeric(cm$overall["Accuracy"]),
    kappa             = as.numeric(cm$overall["Kappa"]),
    macro_precision   = mean(prec, na.rm=TRUE),
    macro_recall      = mean(rec,  na.rm=TRUE),
    macro_F1          = mean(f1,   na.rm=TRUE),
    balanced_accuracy = mean(bal,  na.rm=TRUE)
  )
}

# MODEL B: per-fold metrics table
metricsB <- bind_rows(lapply(xgbB_results, function(r) fold_metrics_from_cm(r$confusion_matrix)))
summaryB <- summarise_all(metricsB, list(mean = ~mean(., na.rm=TRUE), sd = ~sd(., na.rm=TRUE)))
summaryB

# Pooled confusion matrix across outer folds (micro averaging)
pool_cm <- function(results_list) {
  tabs <- lapply(results_list, function(r) r$confusion_matrix$table)
  Reduce(`+`, tabs)
}
pooled_tabB <- pool_cm(xgbB_results)
pooled_cmB  <- caret::confusionMatrix(pooled_tabB)  # global confusion matrix
pooled_cmB

# Pooled accuracy 95% CI (binomial on all test predictions combined)
tot_correct <- sum(diag(pooled_tabB))
tot_n       <- sum(pooled_tabB)
ci95_poolB  <- binom.test(tot_correct, tot_n)$conf.int
ci95_poolB







#################################################################################
################################ POST-HOC ANALYSIS ##############################
#################################################################################

# ---------------------- Post-hoc analysis pipeline ----------------------
suppressPackageStartupMessages({
library(ggplot2)
library(dunn.test) # FSA::dunnTest
})

set.seed(26)

# 0) Build top-20 feature sets from each models stored results

top20_from_rfA <- (function() {
  impA_all <- lapply(rfA_results, function(x) {
    im <- x$importance[, "MeanDecreaseGini"]; setNames(im, rownames(x$importance))
  })
  feats <- unique(unlist(lapply(impA_all, names)))
  M <- do.call(cbind, lapply(impA_all, function(v) { out <- setNames(numeric(length(feats)), feats); out[names(v)] <- v; out }))
  sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE) |> head(20) |> names()
})()

top20_from_rfB <- (function() {
  impB_all <- lapply(rfB_results, `[[`, "global_importance")
  feats <- unique(unlist(lapply(impB_all, names)))
  M <- do.call(cbind, lapply(impB_all, function(v){ out <- setNames(numeric(length(feats)), feats); out[names(v)] <- v; out }))
  sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE) |> head(20) |> names()
})()

top20_from_xgbA <- (function() {
  impA <- lapply(xgbA_results, function(x) if (is.null(x$importance)) NULL else setNames(x$importance$Gain, x$importance$Feature))
  impA <- impA[lengths(impA) > 0]
  feats <- unique(unlist(lapply(impA, names)))
  M <- do.call(cbind, lapply(impA, function(v){ out <- setNames(numeric(length(feats)), feats); out[names(v)] <- v; out }))
  sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE) |> head(20) |> names()
})()

top20_from_xgbB <- (function() {
  impB <- lapply(xgbB_results, `[[`, "global_importance")
  feats <- unique(unlist(lapply(impB, names)))
  M <- do.call(cbind, lapply(impB, function(v){ out <- setNames(numeric(length(feats)), feats); out[names(v)] <- v; out }))
  sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE) |> head(20) |> names()
})()

top20_from_mnl <- (function() {
  # Use selection frequency across folds (you computed `feature_stability`)
  if (exists("feature_stability")) {
    head(feature_stability$feature[order(-feature_stability$fold_count)], 20)
  } else {
    # Fallback: union of features selected per fold (results list)
    per_fold <- lapply(results, `[[`, "selected")
    freq <- sort(table(unlist(per_fold)), decreasing = TRUE)
    names(head(freq, 20))
  }
})()

top20_by_model <- list(
  Multinomial = top20_from_mnl,
  RF_A = top20_from_rfA,
  RF_B = top20_from_rfB,
  XGB_A = top20_from_xgbA,
  XGB_B = top20_from_xgbB
)
# select the unique union of top 20 features pooled from each model
top20_union <- sort(unique(unlist(top20_by_model)))

# 1) Use pooled outer-test data (each subject once). Across 10 folds, this is all rows.
#    Still, treat p-values as exploratory (features were selected post-hoc).

df <- data %>%
  select(all_of(c("hyperactivity_baseline_cat", top20_union))) %>%
  filter(!is.na(hyperactivity_baseline_cat)) %>%
  mutate(hyperactivity_baseline_cat = droplevels(hyperactivity_baseline_cat))

# 2) Kruskal–Wallis + epsilon^2 + Dunn (FDR)
kw_eps2 <- function(x, g) {
  kt <- kruskal.test(x ~ g)
  n <- length(x); k <- nlevels(g)
  H <- unname(kt$statistic)
  eps2 <- max(0, (H - (k - 1)) / (n - 1))  # epsilon^2 effect size
  list(p = kt$p.value, eps2 = eps2)
}
# ---- Dunn's post-hoc without FSA; lightweight + safe fallback ----
pairwise_dunn <- function(df, feat) {
  x <- df[[feat]]
  g <- droplevels(df$hyperactivity_baseline_cat)
  
  # Need at least 2 groups and some non-NA data
  if (!is.factor(g) || nlevels(g) < 2 || all(!is.finite(x))) {
    return(tibble::tibble(feature = character(0), contrast = character(0),
                          Z = numeric(0), p_raw = numeric(0),
                          p_adj = numeric(0), method = character(0)))
  }
  
  if (requireNamespace("dunn.test", quietly = TRUE)) {
    dt <- dunn.test::dunn.test(x, g, method = "bh", kw = FALSE)
    return(tibble::tibble(
      feature   = feat,
      contrast  = dt$comparisons,
      Z         = as.numeric(dt$Z),
      p_raw     = as.numeric(dt$P),
      p_adj     = as.numeric(dt$P.adjusted),
      method    = dt$method
    ))
  } else {
    # Fallback: pairwise Wilcoxon with BH FDR (not Dunn, but acceptable)
    pw <- pairwise.wilcox.test(x, g, p.adjust.method = "BH", exact = FALSE)
    mat <- as.matrix(pw$p.value)
    # Tidy upper triangle
    out <- do.call(rbind, lapply(seq_len(nrow(mat)), function(i) {
      data.frame(
        feature  = feat,
        contrast = paste(rownames(mat)[i], colnames(mat), sep = " - "),
        p_adj    = as.numeric(mat[i, ]),
        stringsAsFactors = FALSE
      )
    }))
    out <- dplyr::filter(out, !is.na(p_adj))
    out$Z <- NA_real_; out$p_raw <- NA_real_; out$method <- "pairwise.wilcox (BH)"
    return(tibble::as_tibble(out[, c("feature","contrast","Z","p_raw","p_adj","method")]))
  }
}

# 3) Bootstrap 95% CIs for per-class medians (robust visual summaries)
boot_ci_median <- function(x, B = 2000, conf = 0.95) {
  x <- x[is.finite(x)]
  if (length(x) < 3) return(c(lwr = NA, med = stats::median(x, na.rm = TRUE), upr = NA))
  meds <- replicate(B, stats::median(sample(x, replace = TRUE), na.rm = TRUE))
  qs <- stats::quantile(meds, probs = c((1 - conf) / 2, 0.5, 1 - (1 - conf) / 2), na.rm = TRUE)
  setNames(as.numeric(qs), c("lwr", "med", "upr"))
}

summarize_feature <- function(df, feat) {
  tmp <- df %>%
    dplyr::select(group = hyperactivity_baseline_cat, val = dplyr::all_of(feat)) %>%
    tidyr::drop_na()
  if (!nrow(tmp)) {
    return(tibble::tibble(feature = feat, group = factor(), n = integer(),
                          lwr = numeric(), med = numeric(), upr = numeric()))
  }
  stats <- tmp %>%
    dplyr::group_by(group) %>%
    dplyr::summarize(n = dplyr::n(),
                     ci = list(boot_ci_median(val)), .groups = "drop") %>%
    tidyr::unnest_wider(ci)
  stats$feature <- feat
  stats
}

# 4) Run stats for all features
kw_table <- lapply(top20_union, function(f) {
  if (!f %in% names(df)) return(NULL)
  res <- kw_eps2(df[[f]], df$hyperactivity_baseline_cat)   # assumes kw_eps2 defined earlier
  tibble::tibble(feature = f, kw_p = res$p, eps2 = res$eps2)
}) %>%
  dplyr::bind_rows() %>%
  dplyr::mutate(q_FDR = p.adjust(kw_p, method = "BH")) %>%
  dplyr::arrange(q_FDR)

dunn_table <- dplyr::bind_rows(
  lapply(top20_union, function(f) if (f %in% names(df)) pairwise_dunn(df, f))
)

median_table <- dplyr::bind_rows(
  lapply(top20_union, function(f) if (f %in% names(df)) summarize_feature(df, f))
)

# 5) Quick overlap/stability snapshot (optional for text/tables)
overlap <- stack(lapply(top20_by_model, length)) %>%
  dplyr::rename(n = values, model = ind)
jaccard_rf_xgb <- length(intersect(top20_by_model$RF_B, top20_by_model$XGB_B)) /
  length(union   (top20_by_model$RF_B, top20_by_model$XGB_B))

# 6) Plots: per-feature violin with median + 95% CI (bootstrap)
plot_feature <- function(feat) {
  if (!feat %in% names(df)) return(NULL)
  stats <- median_table %>% dplyr::filter(feature == feat)
  if (!nrow(stats)) return(NULL)
  
  # align factor levels for clean x-axis placement
  stats$group <- factor(stats$group, levels = levels(df$hyperactivity_baseline_cat))
  
  ggplot2::ggplot(
    df,
    ggplot2::aes(x = hyperactivity_baseline_cat,
                 y = .data[[feat]],
                 fill = hyperactivity_baseline_cat)
  ) +
    ggplot2::geom_violin(trim = FALSE, alpha = 0.35, linewidth = 0.2) +
    ggplot2::geom_point(position = ggplot2::position_jitter(width = 0.1),
                        alpha = 0.25, size = 0.6) +
    ggplot2::geom_pointrange(
      data = stats,
      ggplot2::aes(x = group, y = med, ymin = lwr, ymax = upr),
      inherit.aes = FALSE,            # <- key: don't inherit 'fill = ...'
      color = "black", fatten = 1.2, size = 0.4
    ) +
    ggplot2::labs(x = NULL, y = feat, title = feat) +
    ggplot2::theme_minimal(base_size = 10) +
    ggplot2::theme(legend.position = "none")
}


dir.create("posthoc_plots", showWarnings = FALSE)
invisible(lapply(top20_union, function(f) {
  gp <- plot_feature(f)
  if (!is.null(gp)) ggplot2::ggsave(
    filename = file.path("posthoc_plots", paste0(gsub("[^A-Za-z0-9_]+","_", f), ".pdf")),
    plot = gp, width = 6, height = 4, device = "pdf"
  )
}))


alpha <- 0.05

sig_kw <- kw_table %>%
  dplyr::arrange(q_FDR) %>%
  dplyr::filter(q_FDR <= alpha)

sig_kw %>% dplyr::select(feature, eps2, kw_p, q_FDR) %>% print(n = Inf)








