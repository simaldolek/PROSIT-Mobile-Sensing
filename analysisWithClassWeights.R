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


# select features and rank
selected <- kw_df$feature[kw_df$q < 0.05]
# no significant features... 

selected <- kw_df$feature[kw_df$eps2 >= 0.14]
# no features with large enough effect size..

# rank by effect size (descending)
kw_df_desc <- kw_df[order(-kw_df$eps2), ]




################################################################################
############################# HELPER FUNCTIONS #################################
################################################################################

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


# Returns a *named* vector of class weights (inverse-frequency; mean=1)
class_weight_vec <- function(y, power = 1) {
  y <- droplevels(y)
  tab <- table(y)
  w  <- (as.numeric(tab))^(-power)
  w  <- w / mean(w)                 # normalize so mean weight ~ 1
  names(w) <- names(tab)
  w
}

# Returns a *per-sample* weight vector aligned to y (factor)
sample_weights <- function(y, power = 1) {
  cw <- class_weight_vec(y, power = power)
  unname(cw[as.character(y)])
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


# Safe XGB prediction: prefer best_ntreelimit (when ES is used), silence warnings
xgb_pred <- function(model, dmat, best_ntree = NULL, best_it = NULL) {
  if (!is.null(best_ntree)) return(suppressWarnings(predict(model, dmat, ntreelimit = as.integer(best_ntree))))
  if (!is.null(best_it))    return(suppressWarnings(predict(model, dmat, ntreelimit = as.integer(best_it))))
  suppressWarnings(predict(model, dmat))
}


# Extract a named Gain vector from an xgb model
xgb_gain_vec <- function(model, feature_names) {
  imp <- try(xgb.importance(model = model, feature_names = feature_names), silent = TRUE)
  if (inherits(imp, "try-error") || is.null(imp) || !nrow(imp)) {
    return(setNames(numeric(0), character(0)))
  }
  setNames(imp$Gain, imp$Feature)
}

################################################################################
####################### Multinomial Logistic Regression ########################
################################################################################
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
# Nested CV  (weighted, no upsampling)
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
        
        # === CHANGED: use class weights instead of upsampling ==================
        keep_feats <- intersect(sel_after_huber, intersect(colnames(tr2_imp), colnames(vl2_imp)))
        if (!length(keep_feats)) { fold_err <- c(fold_err, 1.0); fold_dropped[[i]] <- trans$dropped; next }
        
        X_tr <- as.matrix(as.data.frame(tr2_imp[, keep_feats, drop = FALSE]))
        X_vl <- as.matrix(as.data.frame(vl2_imp[, keep_feats, drop = FALSE]))
        y_tr <- droplevels(tr2$hyperactivity_baseline_cat)           # training labels
        y_vl <- droplevels(vl2$hyperactivity_baseline_cat)
        w_tr <- sample_weights(y_tr, power = 1)                      # per-sample weights
        # ======================================================================
        
        # Fit (λ tuned internally) with weights
        cv_fit <- cv.glmnet(
          X_tr, y_tr,
          family = "multinomial",
          alpha = a,
          type.measure = "class",
          maxit = 1e6,
          nlambda = 100,
          lambda.min.ratio = 0.01,
          weights = w_tr
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
    
    # === CHANGED: no upsampling; fit with class weights =======================
    keep_feats_outer <- intersect(sel_outer_after_huber,
                                  intersect(colnames(d_train2_imp), colnames(d_test2_imp)))
    if (!length(keep_feats_outer)) {
      acc_outer <- NA_real_; nz_total <- 0; selected_features <- character(0)
    } else {
      X_train <- as.matrix(as.data.frame(d_train2_imp[, keep_feats_outer, drop = FALSE]))
      X_test  <- as.matrix(as.data.frame(d_test2_imp[, keep_feats_outer, drop = FALSE]))
      y_train <- droplevels(d_train2$hyperactivity_baseline_cat)
      y_test  <- droplevels(d_test2_imp$hyperactivity_baseline_cat)
      w_train <- sample_weights(y_train, power = 1)
      
      cv_fit_outer <- cv.glmnet(
        X_train, y_train,
        family = "multinomial",
        alpha = best_alpha,
        type.measure = "class",
        maxit = 1e6,
        nlambda = 100,
        lambda.min.ratio = 0.01,
        weights = w_train
      )
      
      preds <- predict(cv_fit_outer, newx = X_test, s = "lambda.min", type = "class")
      acc_outer <- mean(preds == y_test)
      
      # Confusion matrix for this outer fold
      cm_outer <- caret::confusionMatrix(
        factor(preds, levels = levels(y_test)),
        y_test
      )
      
      final_fit <- glmnet(
        X_train, y_train,
        family = "multinomial",
        alpha = best_alpha,
        lambda = cv_fit_outer$lambda.min,
        maxit = 1e6,
        weights = w_train
      )
      coefs <- coef(final_fit)
      selected_features <- unique(unlist(lapply(coefs, function(mat) rownames(mat)[mat[,1] != 0])))
      selected_features <- setdiff(selected_features, "(Intercept)")
      nz_total <- length(selected_features)
    }
    # ==========================================================================
  }
  
  # Save per-fold summary + dropped features
  results[[o]] <- list(
    fold = o,
    best_alpha = best_alpha,
    best_topN  = best_topN,
    acc        = acc_outer,
    nonzero_total = nz_total,
    selected      = selected_features,
    counts_test_combo  = table(d_test2$hyperactivity_baseline_cat, d_test2$device_type),
    counts_train_combo = table(d_train2$hyperactivity_baseline_cat, d_train2$device_type),
    confusion_matrix   = cm_outer    
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

# Pooled (micro-averaged) confusion matrix across outer folds
pooled_tab_mnl <- Reduce(`+`, lapply(results, function(r) r$confusion_matrix$table))
pooled_cm_mnl  <- caret::confusionMatrix(pooled_tab_mnl)

# Quick look
pooled_cm_mnl

# save dropped-feature map for inspection
# saveRDS(excluded_huber, file = "excluded_features_huber_nestedcv.rds")

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
#write.csv(feature_stability, "feature_stability_across_folds.csv", row.names = FALSE)
#write.csv(tidy_per_fold, "features_by_fold_long.csv", row.names = FALSE)

# 7)Show, for each fold, the exact feature list
split(tidy_per_fold$feature, tidy_per_fold$fold)








################################################################################
####################### Random Forest : Model A & B (class weights) ###########
################################################################################
# Model A (KW-only): tunes topN, ntree, mtry using KW-ranked numeric features
# Model B (KW + RF-pruning): KW pool → RF importance → elbow-pruned top-k

set.seed(26)

# Stratification label (class × device) for CV splits
data$y_dev <- interaction(data$hyperactivity_baseline_cat, data$device_type, drop = TRUE)

#========================
# Grids / CV
#========================
K_OUTER <- 10
K_INNER <- 3

topN_grid   <- c(30, 40, 50, 60, 70, 80, 100, 120)
ntree_grid  <- c(500, 1000)
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
  # Model A: KW-only (class weights)
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
        X_tr <- imp$train
        X_vl <- imp$test
        y_tr <- droplevels(tr$hyperactivity_baseline_cat)
        y_vl <- droplevels(vl$hyperactivity_baseline_cat)
        
        # Per-fold class weights (inverse-frequency; mean ≈ 1)
        cw <- class_weight_vec(y_tr)
        
        rf_fit <- randomForest(x = X_tr, y = y_tr,
                               ntree = ntree, mtry = mtry,
                               importance = TRUE,
                               classwt = cw)
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
  
  # Refit A on full outer train → eval on outer test (class weights)
  A_feats <- intersect(head(ordered_numeric, bestA_topN), colnames(d_train0))
  impA <- impute_by_train_median(d_train0[, A_feats, drop = FALSE],
                                 d_test0 [, A_feats, drop = FALSE],
                                 A_feats)
  XA_tr <- impA$train
  XA_te <- impA$test
  yA_tr <- droplevels(d_train0$hyperactivity_baseline_cat)
  yA_te <- droplevels(d_test0$hyperactivity_baseline_cat)
  cwA   <- class_weight_vec(yA_tr)
  
  rfA_final <- randomForest(x = XA_tr, y = yA_tr,
                            ntree = bestA_ntree, mtry = bestA_mtry,
                            importance = TRUE,
                            classwt = cwA)
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
  # Model B: KW → RF importance → elbow pruning (class weights)
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
        
        keep_feats <- intersect(sel_feats, intersect(colnames(tr), colnames(vl)))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        # Impute (train medians) -> apply to val
        imp <- impute_by_train_median(tr[, keep_feats, drop = FALSE],
                                      vl[, keep_feats, drop = FALSE],
                                      keep_feats)
        X_tr <- imp$train
        X_vl <- imp$test
        y_tr <- droplevels(tr$hyperactivity_baseline_cat)
        y_vl <- droplevels(vl$hyperactivity_baseline_cat)
        cw   <- class_weight_vec(y_tr)
        
        # Stage 1: RF on KW pool → importance (class weights)
        rf_stage1 <- randomForest(x = X_tr, y = y_tr,
                                  ntree = ntree, mtry = mtry,
                                  importance = TRUE,
                                  classwt = cw)
        imp_vec <- get_imp_vec(rf_stage1)
        inner_imps[[i]] <- imp_vec
        
        # Elbow k on cumulative importance (≥ 90%), enforce ≥ mtry and ≥ 10
        k_elbow <- compute_k_elbow(imp_vec, threshold = 0.90, min_k = 10, mtry = mtry)
        inner_ks <- c(inner_ks, k_elbow)
        
        # Prune to top-k and refit (class weights)
        imp_ranked  <- sort(imp_vec, decreasing = TRUE)
        prune_feats <- intersect(names(head(imp_ranked, k_elbow)), colnames(X_tr))
        if (!length(prune_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        rf_stage2 <- randomForest(x = X_tr[, prune_feats, drop = FALSE],
                                  y = y_tr,
                                  ntree = ntree,
                                  mtry  = min(mtry, length(prune_feats)),
                                  importance = FALSE,
                                  classwt = cw)
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
  
  # Average inner importances (best) → GLOBAL ranking
  best_imp_list <- candidate_imp_store[[best_key]]
  best_imp_list <- best_imp_list[vapply(best_imp_list, function(x) length(x) > 0, logical(1))]
  global_imp    <- average_importance(best_imp_list)
  global_rank   <- sort(global_imp, decreasing = TRUE)
  
  # GLOBAL elbow k on averaged curve (≥ 90%), enforce ≥ mtry and ≥ 10
  k_global <- compute_k_elbow(global_rank, threshold = 0.90, min_k = 10, mtry = bestB_mtry)
  
  # Final feature set for outer refit: restrict to KW pool, then take top-k by global rank
  B_pool      <- head(ordered_numeric, bestB_topN)
  B_ranked    <- global_rank[names(global_rank) %in% B_pool]
  B_finalfeat <- names(head(B_ranked, k_global))
  
  # Refit on full outer train → eval on outer test (class weights)
  if (length(B_finalfeat) == 0) {
    accB <- NA_real_; cmB <- NA
  } else {
    impB <- impute_by_train_median(d_train0[, B_finalfeat, drop = FALSE],
                                   d_test0 [, B_finalfeat, drop = FALSE],
                                   B_finalfeat)
    XB_tr <- impB$train
    XB_te <- impB$test
    yB_tr <- droplevels(d_train0$hyperactivity_baseline_cat)
    yB_te <- droplevels(d_test0$hyperactivity_baseline_cat)
    cwB   <- class_weight_vec(yB_tr)
    
    rfB_final <- randomForest(x = XB_tr, y = yB_tr,
                              ntree = bestB_ntree,
                              mtry  = min(bestB_mtry, length(B_finalfeat)),
                              importance = TRUE,
                              classwt = cwB)
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
# rfA_results[[1]]$confusion_matrix
# rfB_results[[1]]$confusion_matrix

# Average importance snapshots
importance <- importance(rfA_final)

impA_all <- lapply(rfA_results, function(x) {
  im <- x$importance[, "MeanDecreaseGini"]
  setNames(im, rownames(x$importance))
})
all_featsA <- unique(unlist(lapply(impA_all, names)))
M_A <- do.call(cbind, lapply(impA_all, function(v) {
  out <- setNames(numeric(length(all_featsA)), all_featsA)
  out[names(v)] <- v; out
}))
avg_impA <- sort(rowMeans(M_A, na.rm = TRUE), decreasing = TRUE)
head(avg_impA, 20)

impB_all <- lapply(rfB_results, function(x) x$global_importance)
all_featsB <- unique(unlist(lapply(impB_all, names)))
M_B <- do.call(cbind, lapply(impB_all, function(v) {
  out <- setNames(numeric(length(all_featsB)), all_featsB)
  out[names(v)] <- v; out
}))
avg_impB <- sort(rowMeans(M_B, na.rm = TRUE), decreasing = TRUE)
head(avg_impB, 20)

# Model A
pooled_tab_A <- Reduce(`+`, lapply(rfA_results, function(r) r$confusion_matrix$table))
pooled_cm_A  <- caret::confusionMatrix(pooled_tab_A)
pooled_cm_A

# Model B
pooled_tab_B <- Reduce(`+`, lapply(rfB_results, function(r) r$confusion_matrix$table))
pooled_cm_B  <- caret::confusionMatrix(pooled_tab_B)
pooled_cm_B





################################################################################
############################### XGBoost: Model A (KW Only, class weights) #####
################################################################################
set.seed(26)

K_OUTER <- 10
K_INNER <- 3
ES_ROUNDS <- 50

topN_grid    <- c(30, 40, 60, 80, 100, 120)
nrounds_grid <- c(200, 500)
eta_grid     <- c(0.05, 0.1)
depth_grid   <- c(3, 5, 7)

outer_folds  <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)
xgbA_results <- vector("list", K_OUTER)

num_class_all <- length(levels(data$hyperactivity_baseline_cat))

for (o in seq_len(K_OUTER)) {
  cat(sprintf("=== OUTER FOLD %d/%d ===\n", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  d_train0  <- data[train_idx, , drop = FALSE]
  d_test0   <- data[test_idx,  , drop = FALSE]
  
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  gridA <- tibble(topN=integer(), nrounds=integer(), eta=numeric(),
                  depth=integer(), err=numeric())
  
  for (topN in topN_grid) {
    sel_feats <- head(ordered_numeric, topN)
    for (nrounds in nrounds_grid) for (eta in eta_grid) for (depth in depth_grid) {
      inner_err <- c()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        keep_feats <- intersect(sel_feats, colnames(tr))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        imp <- impute_by_train_median(tr[, keep_feats, drop=FALSE],
                                      vl[, keep_feats, drop=FALSE],
                                      keep_feats)
        trX <- as.matrix(imp$train)
        vlX <- as.matrix(imp$test)
        y_tr_f <- droplevels(tr$hyperactivity_baseline_cat)
        y_vl_f <- droplevels(vl$hyperactivity_baseline_cat)
        y_tr <- as.numeric(y_tr_f) - 1L
        y_vl <- as.numeric(y_vl_f) - 1L
        
        w_tr <- sample_weights(y_tr_f)
        
        dtrain <- xgb.DMatrix(trX, label = y_tr, weight = w_tr)
        dval   <- xgb.DMatrix(vlX, label = y_vl)
        
        fit <- xgb.train(
          params = list(
            objective  = "multi:softprob",
            num_class  = num_class_all,
            eval_metric= "mlogloss",
            eta        = eta,
            max_depth  = depth,
            verbosity  = 0,
            nthread    = max(1, parallel::detectCores() - 1)
          ),
          data = dtrain,
          nrounds = nrounds,
          watchlist = list(train = dtrain, eval = dval),
          early_stopping_rounds = ES_ROUNDS,
          verbose = 0
        )
        best_ntree <- if (!is.null(fit$best_ntreelimit)) fit$best_ntreelimit else fit$best_iteration
        prob <- matrix(xgb_pred(fit, dval, best_ntree = best_ntree),
                       ncol = num_class_all, byrow = TRUE)
        pred <- max.col(prob) - 1L
        inner_err <- c(inner_err, mean(pred != y_vl))
      }
      
      gridA <- add_row(gridA, topN=topN, nrounds=nrounds, eta=eta, depth=depth,
                       err=mean(inner_err))
    }
  }
  
  # Best params
  best_idx <- which.min(gridA$err)
  best     <- gridA[best_idx, ]
  sel_feats <- head(ordered_numeric, best$topN)
  
  keep_feats <- intersect(sel_feats, colnames(d_train0))
  imp_outer  <- impute_by_train_median(d_train0[, keep_feats, drop=FALSE],
                                       d_test0 [, keep_feats, drop=FALSE],
                                       keep_feats)
  trX  <- as.matrix(imp_outer$train)
  teX  <- as.matrix(imp_outer$test)
  y_tr_f <- droplevels(d_train0$hyperactivity_baseline_cat)
  y_te_f <- droplevels(d_test0$hyperactivity_baseline_cat)
  y_tr   <- as.numeric(y_tr_f) - 1L
  y_te   <- as.numeric(y_te_f) - 1L
  w_tr   <- sample_weights(y_tr_f)
  
  dtrain <- xgb.DMatrix(trX, label=y_tr, weight=w_tr)
  dtest  <- xgb.DMatrix(teX, label=y_te)
  
  # final refit (no early stopping vs. test)
  xgb_final <- xgb.train(
    params  = list(
      objective = "multi:softmax",
      num_class = num_class_all,
      eval_metric = "merror",
      eta = best$eta, max_depth = best$depth,
      verbosity = 0,
      nthread = max(1, parallel::detectCores() - 1)
    ),
    data    = dtrain,
    nrounds = best$nrounds,
    verbose = 0
  )
  
  pred <- xgb_pred(xgb_final, dtest)
  acc  <- mean(pred == y_te)
  pred_class <- factor(levels(y_te_f)[pred + 1L], levels = levels(y_te_f))
  truth      <- droplevels(y_te_f)
  cmA        <- make_cm(pred_class, truth)
  
  imp  <- xgb.importance(model=xgb_final, feature_names=keep_feats)
  
  xgbA_results[[o]] <- list(
    fold = o,
    acc  = acc,
    best = best,
    importance = imp,
    confusion_matrix = cmA
  )
}

# Summary
xgbA_summary <- tibble(
  fold    = seq_len(K_OUTER),
  acc     = sapply(xgbA_results, `[[`, "acc"),
  topN    = sapply(xgbA_results, function(x) x$best$topN),
  nrounds = sapply(xgbA_results, function(x) x$best$nrounds),
  eta     = sapply(xgbA_results, function(x) x$best$eta),
  depth   = sapply(xgbA_results, function(x) x$best$depth)
)
print(xgbA_summary)


# Average importance across folds (Gain)
imp_list <- lapply(xgbA_results, function(x) {
  if (is.null(x$importance)) return(NULL)
  setNames(x$importance$Gain, x$importance$Feature)
})
all_feats <- unique(unlist(lapply(imp_list, names)))
M <- do.call(cbind, lapply(imp_list, function(v) { out <- setNames(numeric(length(all_feats)), all_feats); out[names(v)] <- v; out }))
avg_imp <- sort(rowMeans(M, na.rm=TRUE), decreasing=TRUE)
head(avg_imp, 20)


# ---- Per-fold metrics from confusion matrix ----
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

# ---- Model A: per-fold metrics ----
metricsA <- bind_rows(lapply(xgbA_results, function(r) fold_metrics_from_cm(r$confusion_matrix)))
summaryA <- summarise_all(metricsA, list(mean = ~mean(., na.rm=TRUE),
                                         sd   = ~sd(.,   na.rm=TRUE)))
print(summaryA)

# ---- Pooled confusion matrix (all folds combined) ----
pool_cm <- function(results_list) {
  tabs <- lapply(results_list, function(r) r$confusion_matrix$table)
  Reduce(`+`, tabs)
}
pooled_tabA <- pool_cm(xgbA_results)
pooled_cmA  <- caret::confusionMatrix(pooled_tabA)
print(pooled_cmA)

# ---- Pooled accuracy CI ----
tot_correct <- sum(diag(pooled_tabA))
tot_n       <- sum(pooled_tabA)
ci95_poolA  <- binom.test(tot_correct, tot_n)$conf.int
print(ci95_poolA)



################################################################################
############ XGBoost: Model B (KW + XGB importance + elbow pruning, weights) ###
################################################################################
set.seed(26)

# (reuse xgb_pred defined above)

K_OUTER <- 10
K_INNER <- 3
ES_ROUNDS <- 50

topN_grid    <- c(30, 40, 50, 60, 80, 100, 120)
nrounds_grid <- c(200, 500)
eta_grid     <- c(0.05, 0.1)
depth_grid   <- c(3, 5, 7)

outer_folds  <- createFolds(data$y_dev, k = K_OUTER, list = TRUE, returnTrain = FALSE)
xgbB_results <- vector("list", K_OUTER)

for (o in seq_len(K_OUTER)) {
  cat(sprintf("=== OUTER FOLD %d/%d ===\n", o, K_OUTER))
  test_idx  <- outer_folds[[o]]
  train_idx <- setdiff(seq_len(nrow(data)), test_idx)
  d_train0  <- data[train_idx, , drop = FALSE]
  d_test0   <- data[test_idx,  , drop = FALSE]
  
  class_levels <- levels(d_train0$hyperactivity_baseline_cat)
  num_class    <- length(class_levels)
  
  inner_folds <- createFolds(d_train0$y_dev, k = K_INNER, list = TRUE, returnTrain = FALSE)
  
  gridB <- tibble(topN=integer(), nrounds=integer(), eta=numeric(), depth=integer(),
                  err=numeric(), mean_k_elbow=integer())
  candidate_imp_store <- list()
  
  for (topN in topN_grid) {
    sel_pool <- head(ordered_numeric, topN)
    for (nrounds in nrounds_grid) for (eta in eta_grid) for (depth in depth_grid) {
      inner_err <- c(); inner_imps <- list(); inner_ks <- c()
      
      for (i in seq_len(K_INNER)) {
        val_idx <- inner_folds[[i]]
        tr_idx  <- setdiff(seq_len(nrow(d_train0)), val_idx)
        tr <- d_train0[tr_idx, , drop = FALSE]
        vl <- d_train0[val_idx, , drop = FALSE]
        
        keep_feats <- intersect(sel_pool, intersect(colnames(tr), colnames(vl)))
        if (!length(keep_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        imp <- impute_by_train_median(tr[, keep_feats, drop = FALSE],
                                      vl[, keep_feats, drop = FALSE],
                                      keep_feats)
        X_tr <- as.matrix(imp$train)
        X_vl <- as.matrix(imp$test)
        y_tr_f <- droplevels(tr$hyperactivity_baseline_cat)
        y_vl_f <- droplevels(vl$hyperactivity_baseline_cat)
        y_tr   <- as.numeric(y_tr_f) - 1L
        y_vl   <- as.numeric(y_vl_f) - 1L
        w_tr   <- sample_weights(y_tr_f)
        
        dtrain <- xgb.DMatrix(X_tr, label = y_tr, weight = w_tr)
        dval   <- xgb.DMatrix(X_vl, label = y_vl)
        
        # Stage 1: KW pool → importance (Gain) with early stopping
        fit_stage1 <- xgb.train(
          params  = list(
            objective="multi:softprob",
            num_class=num_class,
            eval_metric="mlogloss",
            eta=eta, max_depth=depth,
            subsample=0.8, colsample_bytree=0.8,
            verbosity = 0,
            nthread = max(1, parallel::detectCores() - 1)
          ),
          data    = dtrain,
          nrounds = nrounds,
          watchlist = list(train = dtrain, eval = dval),
          early_stopping_rounds = ES_ROUNDS,
          verbose = 0
        )
        imp_vec  <- xgb_gain_vec(fit_stage1, feature_names = colnames(X_tr))
        inner_imps[[i]] <- imp_vec
        
        # Elbow on cumulative Gain (≥ 90%)
        k_elbow <- compute_k_elbow(imp_vec, threshold = 0.90, min_k = 10)
        inner_ks <- c(inner_ks, k_elbow)
        
        # Stage 2: prune to top-k, refit with early stopping (reuse weights)
        prune_feats <- names(head(sort(imp_vec, decreasing = TRUE), k_elbow))
        if (!length(prune_feats)) { inner_err <- c(inner_err, 1.0); next }
        
        dtrain2 <- xgb.DMatrix(as.matrix(X_tr[, prune_feats, drop = FALSE]),
                               label = y_tr, weight = w_tr)
        dval2   <- xgb.DMatrix(as.matrix(X_vl[, prune_feats, drop = FALSE]),
                               label = y_vl)
        
        fit_stage2 <- xgb.train(
          params  = list(
            objective="multi:softprob",
            num_class=num_class,
            eval_metric="mlogloss",
            eta=eta, max_depth=depth,
            subsample=0.8, colsample_bytree=0.8,
            verbosity = 0,
            nthread = max(1, parallel::detectCores() - 1)
          ),
          data    = dtrain2,
          nrounds = nrounds,
          watchlist = list(train = dtrain2, eval = dval2),
          early_stopping_rounds = ES_ROUNDS,
          verbose = 0
        )
        best_ntree2 <- if (!is.null(fit_stage2$best_ntreelimit)) fit_stage2$best_ntreelimit else fit_stage2$best_iteration
        prob <- matrix(xgb_pred(fit_stage2, dval2, best_ntree = best_ntree2),
                       ncol = num_class, byrow = TRUE)
        pred <- max.col(prob) - 1L
        inner_err <- c(inner_err, mean(pred != y_vl))
      }
      
      key <- paste0("topN", topN, "_nr", nrounds, "_eta", eta, "_md", depth)
      candidate_imp_store[[key]] <- inner_imps
      
      gridB <- add_row(gridB,
                       topN=topN, nrounds=nrounds, eta=eta, depth=depth,
                       err=mean(inner_err),
                       mean_k_elbow = round(mean(inner_ks, na.rm=TRUE)))
    }
  }
  
  # Best combo
  best_idx <- which.min(gridB$err)
  best     <- gridB[best_idx, ]
  best_key <- paste0("topN", best$topN, "_nr", best$nrounds, "_eta", best$eta, "_md", best$depth)
  
  # Average inner importances (best) → global ranking
  best_imp_list <- candidate_imp_store[[best_key]]
  best_imp_list <- best_imp_list[vapply(best_imp_list, function(x) length(x) > 0, logical(1))]
  global_imp    <- average_importance(best_imp_list)
  global_rank   <- sort(global_imp, decreasing = TRUE)
  
  # Global elbow (≥ 90%)
  k_global <- compute_k_elbow(global_rank, threshold = 0.90, min_k = 10)
  
  # Final features: KW pool ∩ top-k by global rank
  B_pool      <- head(ordered_numeric, best$topN)
  B_ranked    <- global_rank[names(global_rank) %in% B_pool]
  B_finalfeat <- names(head(B_ranked, k_global))
  
  # Refit on full outer TRAIN → evaluate on TEST (class weights); no ES vs test
  if (!length(B_finalfeat)) {
    accB <- NA_real_; cmB <- NA
  } else {
    imp_outer <- impute_by_train_median(d_train0[, B_finalfeat, drop = FALSE],
                                        d_test0 [, B_finalfeat, drop = FALSE],
                                        B_finalfeat)
    X_tr  <- as.matrix(imp_outer$train)
    X_te  <- as.matrix(imp_outer$test)
    y_tr_f <- droplevels(d_train0$hyperactivity_baseline_cat)
    y_te_f <- droplevels(d_test0$hyperactivity_baseline_cat)
    y_tr   <- as.numeric(y_tr_f) - 1L
    num_class <- length(levels(y_tr_f))
    w_tr   <- sample_weights(y_tr_f)
    
    dtrain_final <- xgb.DMatrix(X_tr, label = y_tr, weight = w_tr)
    dtest_final  <- xgb.DMatrix(X_te)
    
    xgb_final <- xgb.train(
      params  = list(
        objective="multi:softprob",
        num_class=num_class,
        eval_metric="mlogloss",
        eta=best$eta, max_depth=best$depth,
        subsample=0.8, colsample_bytree=0.8,
        verbosity = 0,
        nthread = max(1, parallel::detectCores() - 1)
      ),
      data    = dtrain_final,
      nrounds = best$nrounds,
      verbose = 0
    )
    
    prob        <- matrix(xgb_pred(xgb_final, dtest_final), ncol = num_class, byrow = TRUE)
    pred_class  <- factor(levels(y_tr_f)[max.col(prob)], levels = levels(y_tr_f))
    truth       <- droplevels(y_te_f)
    
    accB <- mean(pred_class == truth)
    cmB  <- make_cm(pred_class, truth)
  }
  
  xgbB_results[[o]] <- list(
    fold = o, acc = accB,
    topN_pool = best$topN, nrounds = best$nrounds, eta = best$eta, depth = best$depth,
    k_global  = k_global,
    global_importance = global_rank,
    final_features    = B_finalfeat,
    confusion_matrix  = cmB
  )
}

# Summary
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

# Fold-level confusion matrices:
# xgbB_results[[1]]$confusion_matrix

# Averaged feature importance (Gain) across folds
impB_all  <- lapply(xgbB_results, `[[`, "global_importance")
all_feats <- unique(unlist(lapply(impB_all, names)))
M <- do.call(cbind, lapply(impB_all, function(v) { out <- setNames(numeric(length(all_feats)), all_feats); out[names(v)] <- v; out }))
avg_gain <- sort(rowMeans(M, na.rm = TRUE), decreasing = TRUE)
head(avg_gain, 20)

# Optional pooled confusion matrix for Model B
pooled_tabB <- Reduce(`+`, lapply(xgbB_results, function(r) r$confusion_matrix$table))
caret::confusionMatrix(pooled_tabB)





# ---- Model A: per-fold metrics ----
metricsB <- bind_rows(lapply(xgbB_results, function(r) fold_metrics_from_cm(r$confusion_matrix)))
summaryB <- summarise_all(metricsB, list(mean = ~mean(., na.rm=TRUE),
                                         sd   = ~sd(.,   na.rm=TRUE)))
print(summaryB)

# ---- Pooled confusion matrix (all folds combined) ----
pool_cm <- function(results_list) {
  tabs <- lapply(results_list, function(r) r$confusion_matrix$table)
  Reduce(`+`, tabs)
}
pooled_tabB <- pool_cm(xgbB_results)
pooled_cmB  <- caret::confusionMatrix(pooled_tabB)
print(pooled_cmB)

# ---- Pooled accuracy CI ----
tot_correct <- sum(diag(pooled_tabB))
tot_n       <- sum(pooled_tabB)
ci95_poolB  <- binom.test(tot_correct, tot_n)$conf.int
print(ci95_poolB)

