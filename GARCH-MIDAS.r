# ------------------------------------------------------------------------------
# Module 0: Environment Setup and Package Management
# ------------------------------------------------------------------------------
# This function sets up the R environment by installing and loading necessary
# packages, and configuring global options for the session.
setup_environment <- function() {
  # A list of all required packages for this analysis.
  # Grouped by functionality for clarity.
  packages <- c(
    # Core packages for time series and modeling
    "xts", "zoo", "maxLik", "rumidas", "lubridate", "rugarch",
    # Visualization packages for high-quality, academic-style plots
    "ggplot2", "tidyr", "gridExtra", "scales", "ggthemes", "viridis",
    "patchwork", "ggpubr", "latex2exp",
    # Data handling and reporting packages
    "readxl", "knitr", "kableExtra", "xtable", "formattable",
    # Statistical analysis and testing packages
    "tseries", "forecast", "moments", "lmtest", "nortest", "car",
    # Parallel computing to speed up intensive tasks
    "doParallel", "foreach", "doRNG", "future", "future.apply",
    # Graphics output and font handling
    "svglite", "Cairo", "extrafont"
  )
  
  # Check for missing packages and install them from CRAN.
  new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
  if(length(new_packages) > 0) {
    cat("Installing missing packages:", paste(new_packages, collapse=", "), "\n")
    install.packages(new_packages, quiet = TRUE, repos = "https://cran.r-project.org")
  }
  
  # The 'rumidas' package is not on CRAN and must be installed from GitHub.
  if (!"rumidas" %in% installed.packages()[,"Package"]) {
    if (!"devtools" %in% installed.packages()[,"Package"]) {
      install.packages("devtools")
    }
    tryCatch(
      devtools::install_github("jstriaukas/midasr_extensions"),
      error = function(e) {
        stop("Could not install 'rumidas' from GitHub. Check network connection.")
      }
    )
  }
  
  # Load all packages, suppressing startup messages for a cleaner console.
  suppressPackageStartupMessages(lapply(packages, library, character.only = TRUE))
  
  # Set global options for the R session to ensure consistent output.
  options(
    scipen = 999,             # Disable scientific notation for numbers.
    digits = 6,               # Set numeric precision for display.
    stringsAsFactors = FALSE, # Prevent strings from being converted to factors.
    mc.cores = parallel::detectCores() - 1 # Use all but one core for parallel tasks.
  )
  
  # Configure the parallel computing backend.
  n_cores <- max(1, detectCores() - 1)
  
  # Use the 'future' package, a modern and flexible parallel framework.
  future::plan(future::multisession, workers = n_cores)
  
  # Also set up 'doParallel' for compatibility with older 'foreach' loops.
  cl <- makeCluster(n_cores)
  registerDoParallel(cl)
  # Set a reproducible seed for parallel random number generation.
  registerDoRNG(123) 
  
  # Load system fonts for high-quality plot exports (e.g., in PDF/SVG).
  # This can fail if fonts are not configured, so it's wrapped in a tryCatch.
  tryCatch({
    if(.Platform$OS.type == "windows") {
      loadfonts(device = "win", quiet = TRUE)
    } else {
      loadfonts(quiet = TRUE)
    }
  }, error = function(e) {
    cat("Font loading failed. Default fonts will be used.\n")
  })
  
  cat("========================================\n")
  cat("Environment Setup Complete:\n")
  cat("- All packages successfully loaded.\n")
  cat("- Parallel cores available:", n_cores, "\n")
  cat("- Parallel backend: future + doParallel\n")
  cat("========================================\n\n")
  
  return(cl)
}

# Execute the environment setup.
cl <- setup_environment()
# Ensure that parallel resources are cleaned up properly when the script exits.
on.exit({
  if(!is.null(cl)) try(stopCluster(cl), silent=TRUE)
  future::plan(future::sequential) # Reset future to non-parallel.
}, add = TRUE)


# ==============================================================================
# User Configuration Area
# ==============================================================================
# --- Data File Configuration ---
EXCEL_FILE_PATH     <- "midas_sample_data.xlsx"  
DAILY_SHEET_NAME    <- "Daily"                  
MONTHLY_SHEET_NAME  <- "Monthly"                

# --- Variable Name Configuration ---
# These names must match the column headers in the Excel file.
EPU_NAME            <- "US.EPU.Index" # Economic Policy Uncertainty Index
SMP_NAME            <- "SMP.Index"    # Stock Market Policy Index
VIX_NAME            <- "VIX.Index"    # Volatility Index (VIX)

# --- Sectors/Indices to Analyze ---
# The script will loop through this list.
SECTOR_NAMES        <- c(
  "SPX.Index",      # S&P 500 Index
  "S5INFT.Index",   # Information Technology Sector
  "S5HLTH.Index",   # Health Care Sector
  "S5ENRS.Index",   # Energy Sector
  "S5FINL.Index",   # Financials Sector
  "S5COND.Index"    # Consumer Discretionary Sector
)

# --- Model Parameter Configuration ---
K_EPU               <- 6              # MIDAS lag order for monthly variables.
DISTRIBUTION        <- "std"          # Error distribution: "norm" (Normal) or "std" (Student-t).
SKEW                <- "YES"          # Include GJR-GARCH asymmetry ("leverage effect"): "YES" or "NO".
ALLOW_BELL_SHAPE    <- FALSE          # Beta weight constraint (not used in this version).

# --- Output Configuration ---
OUTPUT_DIR          <- "./MIDAS_Results_Enhanced_v41"  # Directory to save all results.
DPI                 <- 300            # Image resolution (dots per inch) for saved plots.
FIGURE_FORMAT       <- c("svg", "png") # Formats for saving plots.

# --- Time Periods for Visualization ---
# Defines shaded regions on plots for specific time spans (e.g., political administrations).
ADMIN_DATES         <- data.frame(
  start_date = as.Date(c("2017-01-20", "2021-01-20")),
  end_date = as.Date(c("2021-01-20", "2025-01-20")),
  administration = factor(c("Trump", "Biden"), levels = c("Trump", "Biden"))
)

# --- Advanced Analysis Toggles ---
# These can be disabled to run the script faster.
RUN_ROBUSTNESS_ANALYSIS <- TRUE  # Run model with different parameters (time-consuming).
RUN_ROLLING_ANALYSIS    <- TRUE  # Run rolling window estimation (very time-consuming).

# --- Robustness Analysis Configuration ---
# Defines the parameter space for robustness checks.
K_VALUES_SENSITIVITY <- c(3, 6, 9, 12) # Range of K (MIDAS lag) values to test.

# --- Rolling Window Configuration ---
ROLLING_WINDOW_SIZE <- 500  # Size of the moving window in trading days.
ROLLING_STEP_SIZE   <- 20   # How many days to move the window forward at each step.


# ==============================================================================
# Custom ggplot Theme (Academic Journal Style)
# ==============================================================================
# Defines a custom ggplot theme for consistent, publication-quality plots.
setup_plot_theme <- function() {
  # A color palette suitable for academic publications.
  academic_colors <- c(
    "#1f77b4",  # Blue
    "#ff7f0e",  # Orange
    "#2ca02c",  # Green
    "#d62728",  # Red
    "#9467bd",  # Purple
    "#8c564b",  # Brown
    "#e377c2",  # Pink
    "#7f7f7f",  # Gray
    "#bcbd22",  # Yellow-Green
    "#17becf"   # Cyan
  )
  
  theme_academic <- theme_bw(base_size = 12) +
    theme(
      # Title and caption settings
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5, 
                                margin = margin(b = 10)),
      plot.subtitle = element_text(size = 14, hjust = 0.5, 
                                   margin = margin(b = 10), color = "gray30"),
      plot.caption = element_text(size = 10, hjust = 1, color = "gray50",
                                  margin = margin(t = 10)),
      
      # Axis titles and text
      axis.title = element_text(size = 13, face = "bold"),
      axis.title.x = element_text(margin = margin(t = 10)),
      axis.title.y = element_text(margin = margin(r = 10)),
      axis.text = element_text(size = 11, color = "black"),
      axis.ticks = element_line(color = "black", linewidth = 0.5),
      
      # Legend settings (placed at the bottom)
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.title = element_text(size = 12, face = "bold"),
      legend.text = element_text(size = 11),
      legend.background = element_rect(fill = "white", color = NA),
      legend.key = element_rect(fill = "white", color = NA),
      legend.spacing.x = unit(0.5, "cm"),
      legend.box.margin = margin(t = 10),
      
      # Panel and grid settings for a clean look
      panel.grid.major = element_line(color = "gray90", linewidth = 0.3),
      panel.grid.minor = element_blank(),
      panel.border = element_rect(color = "black", linewidth = 1, fill = NA),
      panel.background = element_rect(fill = "white"),
      
      # Facet (small multiple) settings
      strip.background = element_rect(fill = "gray95", color = "black"),
      strip.text = element_text(size = 12, face = "bold", margin = margin(5, 5, 5, 5)),
      
      # Overall plot margin
      plot.margin = margin(20, 20, 20, 20)
    )
  
  # Set the color palette as the default for discrete color scales in ggplot.
  options(ggplot2.discrete.colour = academic_colors)
  
  return(theme_academic)
}

# Apply the custom theme as the default for all subsequent plots.
theme_set(setup_plot_theme())


# ==============================================================================
# Module 1: Data Import and Preprocessing
# ==============================================================================
# Loads daily and monthly data from an Excel file, preprocesses it, and
# converts it into xts time series objects.
load_and_prepare_data <- function(file_path, daily_sheet, monthly_sheet) {
  cat("Loading data...\n")
  
  # A robust function to read an Excel sheet. It previews the file to
  # determine column types, which prevents common import errors.
  safe_read_excel <- function(path, sheet) {
    tryCatch({
      # Preview the first row to get the number of columns.
      preview <- readxl::read_excel(path, sheet = sheet, n_max = 0)
      n_cols <- ncol(preview)
      
      # Assume the first column is a date and the rest are numeric.
      col_types <- c("date", rep("numeric", n_cols - 1))
      
      # Read the full sheet with specified column types.
      data <- readxl::read_excel(path, sheet = sheet, col_types = col_types)
      
      return(data)
    }, error = function(e) {
      stop(paste("Failed to read sheet '", sheet, "': ", e$message))
    })
  }
  
  # Read the daily and monthly data sheets in parallel to save time.
  data_list <- future.apply::future_lapply(
    list(daily = daily_sheet, monthly = monthly_sheet),
    function(sheet) safe_read_excel(file_path, sheet),
    future.seed = TRUE
  )
  
  daily_raw <- data_list$daily
  monthly_raw <- data_list$monthly
  
  # Converts a raw data frame to a cleaned xts object and prints a summary.
  process_data_to_xts <- function(raw_data, data_type) {
    dates <- as.Date(raw_data[[1]])
    
    # Check for missing dates, which would break the time series.
    if (any(is.na(dates))) {
      stop(paste(data_type, "data contains NA values in the date column."))
    }
    
    # Convert data to a numeric matrix with sanitized column names.
    values <- as.matrix(raw_data[, -1, drop = FALSE])
    colnames(values) <- make.names(colnames(raw_data)[-1], unique = TRUE)
    
    # Create the xts (eXtensible Time Series) object.
    data_xts <- xts::xts(values, order.by = dates)
    
    # Print a data quality summary.
    cat("\n", data_type, "Data Summary:\n")
    cat("  - Time range:", format(start(data_xts), "%Y-%m-%d"), "to", 
        format(end(data_xts), "%Y-%m-%d"), "\n")
    cat("  - Variables:", ncol(data_xts), "\n")
    cat("  - Observations:", nrow(data_xts), "\n")
    
    # Report count and percentage of missing values for each variable.
    na_count <- colSums(is.na(data_xts))
    if (any(na_count > 0)) {
      cat("  - Missing Values Report:\n")
      na_summary <- data.frame(
        Variable = names(na_count[na_count > 0]),
        Missing = na_count[na_count > 0],
        Percentage = round(100 * na_count[na_count > 0] / nrow(data_xts), 2)
      )
      print(na_summary, row.names = FALSE)
    }
    
    return(data_xts)
  }
  
  # Process both the daily and monthly raw data.
  daily_prices <- process_data_to_xts(daily_raw, "Daily")
  monthly_data <- process_data_to_xts(monthly_raw, "Monthly")
  
  # Return a list containing the two processed xts objects.
  return(list(
    daily_prices = daily_prices,
    monthly_data = monthly_data
  ))
}


# ==============================================================================
# Module 2: Core Data Preparation for MIDAS
# ==============================================================================
# This function aligns daily return data with lagged monthly explanatory
# variables, creating the matrix structure required for MIDAS regression.
prepare_midas_data <- function(daily_ret, monthly_epu, monthly_smp, K_epu) {
  cat("    Preparing data for MIDAS model...\n")
  
  # Merge and clean monthly data. Lag by one period as is standard.
  monthly_master <- na.omit(merge(monthly_epu, monthly_smp, join = "inner"))
  index(monthly_master) <- as.Date(format(index(monthly_master), "%Y-%m-01"))
  monthly_master <- na.omit(zoo::na.locf(monthly_master, fromLast = FALSE))
  monthly_master_lagged <- na.omit(lag(monthly_master, k = 1))
  
  if (nrow(monthly_master_lagged) < K_epu) {
    stop(paste("Not enough monthly observations after lagging. Need at least", K_epu))
  }
  
  # Standardize monthly variables to have a mean of 0 and standard deviation of 1.
  colnames(monthly_master_lagged) <- c("EPU", "SMP")
  monthly_master_lagged$EPU <- scale(zoo::coredata(monthly_master_lagged$EPU))
  monthly_master_lagged$SMP <- scale(zoo::coredata(monthly_master_lagged$SMP))
  
  # Align daily and monthly series. Daily data must start after enough
  # monthly lags are available.
  first_available_month <- start(monthly_master_lagged)
  min_daily_date <- first_available_month %m+% months(K_epu)
  daily_ret_filtered <- daily_ret[index(daily_ret) >= min_daily_date]
  
  if (nrow(daily_ret_filtered) == 0) {
    stop("No daily returns remaining after date alignment.")
  }
  
  # Build the MIDAS matrix (mv_m_epu) where each column corresponds to a day
  # and rows contain the K lagged monthly EPU values for that day.
  cat("    Constructing MIDAS matrix...\n")
  
  monthly_df <- data.frame(
    Date = index(monthly_master_lagged),
    EPU = coredata(monthly_master_lagged$EPU),
    SMP = coredata(monthly_master_lagged$SMP),
    YYYYMM = format(index(monthly_master_lagged), "%Y-%m"),
    stringsAsFactors = FALSE
  )
  
  # Pre-allocate matrix for efficiency.
  daily_dates <- index(daily_ret_filtered)
  n_days <- length(daily_dates)
  mv_m_epu <- matrix(NA, nrow = K_epu, ncol = n_days)
  
  # Populate the MIDAS matrix by matching daily dates to lagged monthly dates.
  for (i in seq_len(n_days)) {
    target_months <- format(seq(as.Date(format(daily_dates[i], "%Y-%m-01")), 
                                by = "-1 month", 
                                length.out = K_epu), "%Y-%m")
    matched_rows <- match(target_months, monthly_df$YYYYMM)
    mv_m_epu[, i] <- rev(monthly_df$EPU[matched_rows])
  }
  
  # Align contemporaneous SMP data to the daily frequency using last observation carried forward.
  smp_daily_aligned <- merge(daily_ret_filtered, monthly_master_lagged$SMP, join = "left")
  smp_daily_filled <- zoo::na.locf(smp_daily_aligned, fromLast = FALSE)[,2]
  
  # Final merge of all data components and removal of any remaining NA rows.
  temp_mv_xts <- xts(t(mv_m_epu), order.by = index(daily_ret_filtered))
  colnames(temp_mv_xts) <- paste0("EPU_L", 1:K_epu)
  
  final_merged_data <- na.omit(merge(
    daily_ret_filtered, 
    smp_daily_filled, 
    temp_mv_xts
  ))
  
  # Re-extract the clean, aligned data components.
  final_daily_ret <- final_merged_data[,1]
  final_smp_daily <- final_merged_data[,2]
  final_mv_m_epu  <- t(coredata(final_merged_data[, -c(1, 2)]))
  
  # Final sanity check on dimensions.
  if (length(final_daily_ret) > 0 && ncol(final_mv_m_epu) != nrow(final_daily_ret)) {
    stop("Final data alignment failed. Mismatched dimensions.")
  }
  
  min_obs <- max(250, K_epu * 30) # A reasonable minimum number of observations.
  if (length(final_daily_ret) < min_obs) {
    stop(paste("Final effective sample size is too small:", length(final_daily_ret)))
  }
  
  cat("    Data preparation complete. Effective observations:", length(final_daily_ret), "\n")
  
  return(list(
    daily_ret = final_daily_ret, 
    mv_m_epu = final_mv_m_epu, 
    smp_contemp = final_smp_daily, 
    n_obs = length(final_daily_ret)
  ))
}

# ==============================================================================
# Module 3: Likelihood Function and Initial Values
# ==============================================================================
# Provides sensible starting values for the optimization routine.
# Good starting values are crucial for model convergence.
get_start_values <- function(model_data, distribution, skew) {
  # Fit a simple GARCH(1,1) model using 'rugarch' to get initial estimates
  # for the short-run volatility parameters.
  spec <- ugarchspec(
    variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
    mean.model = list(armaOrder = c(0, 0), include.mean = FALSE),
    distribution.model = "norm"
  )
  
  fit <- tryCatch({
    suppressMessages(ugarchfit(spec = spec, data = model_data$daily_ret, solver = 'hybrid'))
  }, error = function(e) NULL)
  
  # Use parameters from the GARCH fit if successful.
  if (!is.null(fit) && !inherits(fit, "try-error")) {
    coefs <- coef(fit)
    omega_init <- max(coefs["omega"], 1e-7)
    alpha_init <- max(coefs["alpha1"], 1e-6)
    beta_init  <- max(coefs["beta1"], 1e-6)
  } else {
    # Provide reasonable fallback values if the GARCH fit fails.
    var_ret <- var(as.numeric(model_data$daily_ret))
    omega_init <- var_ret * 0.01
    alpha_init <- 0.05
    beta_init  <- 0.90
  }
  
  # Ensure the initial persistence (alpha + beta) is less than 1 for stationarity.
  if (skew == "YES") {
    gamma_init <- 0.05
    persistence <- alpha_init + beta_init + 0.5 * abs(gamma_init)
  } else {
    gamma_init <- 0
    persistence <- alpha_init + beta_init
  }
  
  if (persistence >= 0.995) {
    scale_factor <- 0.98 / persistence
    alpha_init <- alpha_init * scale_factor
    beta_init  <- beta_init * scale_factor
  }
  
  # Assemble the full starting parameter vector for the GARCH-MIDAS model.
  start_params <- c(
    omega  = omega_init,
    alpha  = alpha_init,
    beta   = beta_init,
    theta0 = log(var(as.numeric(model_data$daily_ret))), # LR intercept
    theta1 = 0.0, # EPU effect on LR vol
    theta2 = 0.0, # SMP effect on LR vol
    theta3 = 0.0, # Interaction effect
    w1     = 1.0, # Beta weight parameter
    w2     = 2.0  # Beta weight parameter
  )
  
  if (skew == "YES") {
    start_params["gamma"] <- gamma_init
  }
  
  if (distribution == "std") {
    start_params["nu"] <- 8.0 # Degrees of freedom for Student-t
  }
  
  return(start_params)
}

# The log-likelihood function for the GARCH-MIDAS model.
# This function is the core of the estimation and will be maximized by the optimizer.
loglik_garch_midas <- function(params, daily_ret, mv_m_epu, K_epu, smp_contemp, 
                               distribution, skew, allow_bell_shape, par_names) {
  names(params) <- par_names
  
  # Extract parameters by name for clarity.
  omega  <- params["omega"]
  alpha  <- params["alpha"]
  beta   <- params["beta"]
  gamma  <- if(skew == "YES" && "gamma" %in% names(params)) params["gamma"] else 0
  theta0 <- params["theta0"]
  theta1 <- params["theta1"]
  theta2 <- params["theta2"]
  theta3 <- params["theta3"]
  w1     <- params["w1"]
  w2     <- params["w2"]
  nu     <- if(distribution == "std" && "nu" %in% names(params)) params["nu"] else Inf
  
  # Check parameter constraints. If violated, return -Inf to guide the optimizer away.
  if (any(is.na(params)) || omega <= 1e-8 || alpha < 0 || beta < 0) return(-Inf)
  if (skew == "YES" && (alpha + gamma < 0)) return(-Inf) # Positivity of conditional variance
  persistence <- alpha + beta + 0.5 * abs(gamma)
  if (persistence >= 1) return(-Inf) # Stationarity constraint
  if (!is.infinite(nu) && nu <= 2) return(-Inf) # Student-t requires df > 2
  if (w1 <= 0 || w2 <= 0 || w1 > 50 || w2 > 50) return(-Inf) # Beta weight constraints
  
  # Prepare data vectors.
  daily_ret_vec <- as.numeric(daily_ret)
  TT <- length(daily_ret_vec)
  
  # Calculate Beta weights for the MIDAS component.
  weights_epu <- tryCatch(
    rumidas::beta_function(1:K_epu, K_epu, w1, w2),
    error = function(e) return(NULL)
  )
  if (is.null(weights_epu) || any(is.na(weights_epu))) return(-Inf)
  
  # Calculate the weighted MIDAS term using efficient matrix multiplication.
  mv_m_epu_core <- zoo::coredata(mv_m_epu)
  midas_epu <- as.numeric(crossprod(weights_epu, mv_m_epu_core))
  smp_vec <- as.numeric(smp_contemp)
  
  # Calculate the long-run volatility component (tau) for each day.
  log_tau_m <- theta0 + theta1 * midas_epu + theta2 * smp_vec + 
    theta3 * (smp_vec * midas_epu)
  if (any(!is.finite(log_tau_m))) return(-Inf)
  tau_m <- exp(log_tau_m)
  
  # Calculate the short-run GARCH component (h_t) via recursion.
  sq_innovations <- (daily_ret_vec^2) / tau_m
  h_t <- numeric(TT)
  h_t[1] <- omega / (1 - persistence) # Unconditional variance as starting value
  
  for (t in 2:TT) {
    I_neg <- as.numeric(daily_ret_vec[t-1] < 0) # Indicator for negative returns (leverage effect)
    h_t[t] <- omega + (alpha + gamma * I_neg) * sq_innovations[t-1] + beta * h_t[t-1]
    h_t[t] <- max(h_t[t], 1e-10) # Enforce positivity
  }
  
  # Total conditional variance is the product of the long-run and short-run components.
  sigma2_t <- tau_m * h_t
  if (any(sigma2_t <= 0)) return(-Inf)
  
  # Calculate the log-likelihood for each observation based on the chosen distribution.
  if (distribution == "norm") {
    ll_vec <- dnorm(daily_ret_vec, mean = 0, sd = sqrt(sigma2_t), log = TRUE)
  } else if (distribution == "std") {
    adj_sigma_t <- sqrt(sigma2_t * (nu - 2) / nu) # Adjust variance for Student-t
    ll_vec <- dt(daily_ret_vec / adj_sigma_t, df = nu, log = TRUE) - log(adj_sigma_t)
  }
  
  # The total log-likelihood is the sum over all observations.
  ll <- sum(ll_vec[is.finite(ll_vec)])
  
  return(ifelse(is.finite(ll), ll, -Inf))
}


# ==============================================================================
# Module 4: Model Fitting and S3 Methods
# ==============================================================================
# Fits the GARCH-MIDAS model using maximum likelihood estimation.
fit_garch_midas <- function(model_data, K_epu, distribution, skew, 
                            allow_bell_shape, verbose = TRUE) {
  if (verbose) cat("    Estimating GARCH-MIDAS model...\n")
  
  start_params <- get_start_values(model_data, distribution, skew)
  par_names <- names(start_params)
  
  # Optimization strategy: Try multiple optimizers to find the best fit,
  # as some may perform better or avoid local optima.
  methods <- c("BFGS", "NM", "SANN")
  
  # Fit models using different optimization methods in parallel.
  if (getOption("mc.cores", 1) > 1) {
    # Parallel version
    fit_results <- future.apply::future_lapply(methods, function(method) {
      if (verbose) cat("      Trying optimization method:", method, "...\n")
      
      tryCatch({
        maxLik::maxLik(
          logLik = loglik_garch_midas, 
          start = start_params, 
          method = method,
          control = list(iterlim = 3000, reltol = 1e-8),
          daily_ret = model_data$daily_ret, 
          mv_m_epu = model_data$mv_m_epu, 
          K_epu = K_epu, 
          smp_contemp = model_data$smp_contemp, 
          distribution = distribution, 
          skew = skew, 
          allow_bell_shape = allow_bell_shape, 
          par_names = par_names
        )
      }, error = function(e) {
        if (verbose) cat("        Method", method, "failed:", e$message, "\n")
        return(NULL)
      })
    }, future.seed = TRUE)
    
    # Select the best result (highest log-likelihood) from the successful fits.
    valid_fits <- Filter(function(fit) {
      !is.null(fit) && is.finite(fit$maximum) && !is.na(fit$maximum)
    }, fit_results)
    
    if (length(valid_fits) > 0) {
      model_fit <- valid_fits[[which.max(sapply(valid_fits, function(f) f$maximum))]]
    } else {
      model_fit <- NULL
    }
    
  } else {
    # Serial version (if running on a single core)
    model_fit <- NULL
    for (method in methods) {
      if (!is.null(model_fit) && model_fit$code < 3) break # Stop if already converged
      
      if (verbose) cat("      Trying optimization method:", method, "...\n")
      
      current_fit <- tryCatch({
        maxLik::maxLik(
          logLik = loglik_garch_midas, 
          start = start_params, 
          method = method,
          control = list(iterlim = 3000, reltol = 1e-8),
          daily_ret = model_data$daily_ret, 
          mv_m_epu = model_data$mv_m_epu, 
          K_epu = K_epu, 
          smp_contemp = model_data$smp_contemp, 
          distribution = distribution, 
          skew = skew, 
          allow_bell_shape = allow_bell_shape, 
          par_names = par_names
        )
      }, error = function(e) {
        if (verbose) cat("        Method", method, "failed:", e$message, "\n")
        return(NULL)
      })
      
      if (!is.null(current_fit) && is.finite(current_fit$maximum)) {
        if (is.null(model_fit) || current_fit$maximum > model_fit$maximum) {
          model_fit <- current_fit
          # Use results of a good fit as starting values for the next optimizer
          if(!is.null(coef(current_fit))) {
            start_params <- coef(current_fit)
          }
        }
      }
    }
  }
  
  if (is.null(model_fit)) {
    warning("All optimization methods failed to produce a valid result.")
    return(NULL)
  }
  
  if (model_fit$code >= 3) {
    warning(paste("Optimization may not have fully converged. Code:", model_fit$code))
  }
  
  # Calculate and store additional convergence diagnostics.
  model_fit$convergence_info <- list(
    iterations = model_fit$iterations,
    convergence_code = model_fit$code,
    gradient_norm = tryCatch(sqrt(sum(model_fit$gradient^2)), error = function(e) NA),
    condition_number = tryCatch(kappa(model_fit$hessian), error = function(e) NA)
  )
  
  # Construct the S3 object to return, which bundles the fit, data, and parameters.
  result <- structure(
    list(
      model_fit = model_fit,
      data = model_data,
      K_epu = K_epu,
      skew = skew,
      distribution = distribution,
      allow_bell_shape = allow_bell_shape,
      estimation_date = Sys.Date()
    ),
    class = "midas_result"
  )
  
  return(result)
}

# S3 'predict' method for 'midas_result' objects.
# Calculates fitted values and, optionally, forecasts.
predict.midas_result <- function(object, n.ahead = 0, ...) {
  if (!inherits(object, "midas_result")) {
    stop("Input must be a 'midas_result' object.")
  }
  
  params <- coef(object$model_fit)
  data <- object$data
  
  # Extract parameters
  omega  <- params["omega"]
  alpha  <- params["alpha"]
  beta   <- params["beta"]
  gamma  <- if(object$skew == "YES" && "gamma" %in% names(params)) params["gamma"] else 0
  theta0 <- params["theta0"]
  theta1 <- params["theta1"]
  theta2 <- params["theta2"]
  theta3 <- params["theta3"]
  w1     <- params["w1"]
  w2     <- params["w2"]
  
  # Prepare data
  daily_ret_vec <- as.numeric(data$daily_ret)
  TT <- length(daily_ret_vec)
  
  # Recalculate weights and volatility components using the estimated parameters.
  weights_epu <- rumidas::beta_function(1:object$K_epu, object$K_epu, w1, w2)
  mv_m_epu_core <- zoo::coredata(data$mv_m_epu)
  midas_epu <- as.numeric(crossprod(weights_epu, mv_m_epu_core))
  smp_vec <- as.numeric(data$smp_contemp)
  
  log_tau_m <- theta0 + theta1 * midas_epu + theta2 * smp_vec + 
    theta3 * (smp_vec * midas_epu)
  tau_m <- exp(log_tau_m)
  
  sq_innovations <- (daily_ret_vec^2) / tau_m
  h_t <- numeric(TT)
  persistence <- alpha + beta + 0.5 * abs(gamma)
  h_t[1] <- omega / (1 - persistence)
  
  for (t in 2:TT) {
    I_neg <- as.numeric(daily_ret_vec[t-1] < 0)
    h_t[t] <- omega + (alpha + gamma * I_neg) * sq_innovations[t-1] + beta * h_t[t-1]
    h_t[t] <- max(h_t[t], 1e-8)
  }
  
  # Calculate total volatility and standardized residuals.
  sigma2_t <- tau_m * h_t
  std_residuals <- daily_ret_vec / sqrt(sigma2_t)
  
  # Generate out-of-sample forecasts if n.ahead > 0.
  forecast_results <- NULL
  if (n.ahead > 0) {
    h_forecast <- numeric(n.ahead)
    last_innovation <- tail(sq_innovations, 1)
    last_h <- tail(h_t, 1)
    last_ret <- tail(daily_ret_vec, 1)
    
    # First-step forecast uses the last available data.
    I_neg_last <- as.numeric(last_ret < 0)
    h_forecast[1] <- omega + (alpha + gamma * I_neg_last) * last_innovation + beta * last_h
    
    # Subsequent forecasts iterate forward using the expected value of innovations.
    if(n.ahead > 1){
      for (i in 2:n.ahead) {
        h_forecast[i] <- omega + (alpha + 0.5 * gamma) * h_forecast[i-1] + beta * h_forecast[i-1]
      }
    }
    
    sigma2_forecast <- tail(tau_m, 1) * h_forecast
    forecast_dates <- seq(index(data$daily_ret)[TT] + 1, by = "days", length.out = n.ahead)
    
    # Calculate 95% forecast confidence intervals.
    if (object$distribution == "norm") {
      z_lower <- qnorm(0.025)
      z_upper <- qnorm(0.975)
    } else {
      nu <- params["nu"]
      z_lower <- qt(0.025, df = nu) * sqrt((nu - 2) / nu)
      z_upper <- qt(0.975, df = nu) * sqrt((nu - 2) / nu)
    }
    
    forecast_results <- list(
      forecast = xts(sqrt(sigma2_forecast), forecast_dates),
      lower = xts(sqrt(sigma2_forecast) * z_lower, forecast_dates),
      upper = xts(sqrt(sigma2_forecast) * z_upper, forecast_dates)
    )
  }
  
  # Return a list of all calculated series.
  return(list(
    total_vol = xts(sqrt(sigma2_t), index(data$daily_ret)),
    long_run_vol = xts(sqrt(tau_m), index(data$daily_ret)),
    short_run_comp = xts(sqrt(h_t), index(data$daily_ret)),
    residuals = xts(std_residuals, index(data$daily_ret)),
    midas_epu_ts = xts(midas_epu, index(data$daily_ret)),
    forecast = forecast_results
  ))
}

# S3 'summary' method for 'midas_result' objects.
# Produces a detailed, publication-style summary of the model fit.
summary.midas_result <- function(object, ...) {
  if (!inherits(object, "midas_result")) {
    stop("Input must be a 'midas_result' object.")
  }
  
  cat("\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("                 GARCH-MIDAS MODEL ESTIMATION RESULTS                 \n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  
  # --- Model Specification ---
  cat("\n【 Model Specification 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  cat(sprintf("  ▪ Model Type: GARCH(1,1)-MIDAS with K = %d lags\n", object$K_epu))
  cat(sprintf("  ▪ Distribution: %s\n", 
              ifelse(object$distribution == "norm", "Normal", "Student-t")))
  cat(sprintf("  ▪ Asymmetric Effect: %s\n", 
              ifelse(object$skew == "YES", "Yes (GJR-GARCH)", "No (Standard GARCH)")))
  cat(sprintf("  ▪ Estimation Date: %s\n", format(object$estimation_date, "%Y-%m-%d")))
  cat(sprintf("  ▪ Sample Size: %d observations\n", object$data$n_obs))
  
  # --- Convergence Diagnostics ---
  cat("\n【 Convergence Diagnostics 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  conv_info <- object$model_fit$convergence_info
  cat(sprintf("  ▪ Optimizer: %s\n", object$model_fit$type))
  cat(sprintf("  ▪ Iterations: %d\n", conv_info$iterations))
  cat(sprintf("  ▪ Status: %s (code = %d)\n", 
              ifelse(conv_info$convergence_code < 3, "Converged", "Not Converged"),
              conv_info$convergence_code))
  cat(sprintf("  ▪ Gradient Norm: %.6f\n", conv_info$gradient_norm))
  
  if (!is.na(conv_info$condition_number)) {
    cat(sprintf("  ▪ Hessian Condition Number: %.2e\n", conv_info$condition_number))
  }
  
  # --- Parameter Estimates ---
  cat("\n【 Parameter Estimates 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  
  est_coef <- coef(object$model_fit)
  # Standard errors are the square root of the diagonal of the variance-covariance matrix.
  std_err <- tryCatch({
    sqrt(diag(vcov(object$model_fit)))
  }, error = function(e) {
    rep(NA, length(est_coef))
  })
  
  # Build a detailed table of estimates, standard errors, t-values, and p-values.
  t_val <- est_coef / std_err
  p_val <- 2 * pnorm(-abs(t_val))
  estimate_table <- data.frame(
    Parameter = names(est_coef),
    Estimate = est_coef,
    `Std.Error` = std_err,
    `t value` = t_val,
    `Pr(>|t|)` = p_val,
    `CI.lower` = est_coef - 1.96 * std_err,
    `CI.upper` = est_coef + 1.96 * std_err,
    check.names = FALSE
  )
  
  # Add conventional significance stars.
  estimate_table$Signif <- sapply(estimate_table$`Pr(>|t|)`, function(p) {
    if (is.na(p)) return("   ")
    if (p < 0.001) return("***")
    if (p < 0.01) return("** ")
    if (p < 0.05) return("* ")
    if (p < 0.1) return(".  ")
    return("   ")
  })
  
  # Plain-English labels for parameters.
  param_labels <- c(
    omega = "ω (Intercept)",
    alpha = "α (ARCH)",
    beta = "β (GARCH)",
    gamma = "γ (Asymmetry)",
    theta0 = "θ₀ (LR Intercept)",
    theta1 = "θ₁ (EPU Effect)",
    theta2 = "θ₂ (SMP Effect)",
    theta3 = "θ₃ (Interaction)",
    w1 = "w₁ (Beta Shape 1)",
    w2 = "w₂ (Beta Shape 2)",
    nu = "ν (DoF)"
  )
  
  # Print the formatted parameter table.
  for (i in 1:nrow(estimate_table)) {
    param <- estimate_table$Parameter[i]
    label <- ifelse(param %in% names(param_labels), param_labels[param], param)
    
    cat(sprintf("%-20s %10.6f %s (%8.6f)  [%8.4f, %8.4f]\n",
                label,
                estimate_table$Estimate[i],
                estimate_table$Signif[i],
                estimate_table$`Std.Error`[i],
                estimate_table$CI.lower[i],
                estimate_table$CI.upper[i]))
  }
  
  cat("────────────────────────────────────────────────────────────────\n")
  cat("Signif. codes: 0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1\n")
  
  # --- Model Fit Statistics ---
  n_obs <- object$data$n_obs
  n_params <- length(est_coef)
  log_lik <- object$model_fit$maximum
  aic <- -2 * log_lik + 2 * n_params
  aicc <- aic + 2 * n_params * (n_params + 1) / (n_obs - n_params - 1)
  bic <- -2 * log_lik + log(n_obs) * n_params
  
  cat("\n【 Model Fit Statistics 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  cat(sprintf("  ▪ Log-Likelihood      : %12.4f\n", log_lik))
  cat(sprintf("  ▪ AIC                 : %12.4f\n", aic))
  cat(sprintf("  ▪ AICc (Corrected AIC) : %12.4f\n", aicc))
  cat(sprintf("  ▪ BIC                 : %12.4f\n", bic))
  cat(sprintf("  ▪ Number of Parameters: %12d\n", n_params))
  cat(sprintf("  ▪ Degrees of Freedom  : %12d\n", n_obs - n_params))
  
  # --- Model Diagnostics ---
  persistence <- est_coef["alpha"] + est_coef["beta"] + 
    if(object$skew == "YES" && "gamma" %in% names(est_coef)) {
      0.5 * abs(est_coef["gamma"])
    } else 0
  
  cat("\n【 Model Diagnostics 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  cat(sprintf("  ▪ Persistence Parameter : %.6f\n", persistence))
  
  if (persistence >= 0.999) {
    cat("    ⚠ Warning: Model is close to non-stationary (near unit-root).\n")
  } else if (persistence >= 0.95) {
    cat("    - Note: High persistence; volatility shocks will decay slowly.\n")
  } else {
    cat("    ✓ Model stationarity is well-behaved.\n")
  }
  
  # The half-life of a volatility shock.
  if (persistence < 1) {
    half_life <- log(0.5) / log(persistence)
    cat(sprintf("  ▪ Volatility Half-Life: %.1f days\n", half_life))
  }
  
  # --- Residual Analysis ---
  vol_preds <- tryCatch(predict(object), error = function(e) NULL)
  if (!is.null(vol_preds)) {
    std_resid <- as.numeric(vol_preds$residuals)
    
    cat("\n【 Residual Analysis 】\n")
    cat("────────────────────────────────────────────────────────────────\n")
    
    # Descriptive statistics of standardized residuals.
    resid_stats <- c(
      Mean = mean(std_resid, na.rm = TRUE),
      SD = sd(std_resid, na.rm = TRUE),
      Skewness = moments::skewness(std_resid, na.rm = TRUE),
      Kurtosis = moments::kurtosis(std_resid, na.rm = TRUE),
      Min = min(std_resid, na.rm = TRUE),
      Max = max(std_resid, na.rm = TRUE)
    )
    
    cat("  Standardized Residual Statistics:\n")
    for (stat_name in names(resid_stats)) {
      cat(sprintf("    %-10s: %8.4f", stat_name, resid_stats[stat_name]))
      
      # Quick evaluation: A well-specified model should have residuals with
      # mean ~0, SD ~1, and low skewness.
      if (stat_name == "Mean") {
        cat(ifelse(abs(resid_stats[stat_name]) < 0.1, " ✓", " ⚠"))
      } else if (stat_name == "SD") {
        cat(ifelse(abs(resid_stats[stat_name] - 1) < 0.1, " ✓", " ⚠"))
      } else if (stat_name == "Skewness") {
        cat(ifelse(abs(resid_stats[stat_name]) < 0.5, " ✓", " ⚠"))
      } else if (stat_name == "Kurtosis") {
        excess_kurt <- resid_stats[stat_name] - 3
        cat(sprintf(" (Excess Kurtosis: %.2f)", excess_kurt))
      }
      cat("\n")
    }
    
    # Statistical tests on residuals.
    cat("\n  Hypothesis Tests on Residuals:\n")
    
    # Ljung-Box test for remaining ARCH effects in squared residuals.
    # A high p-value is desired, indicating no remaining autocorrelation.
    lb_test <- Box.test(std_resid^2, lag = 20, type = "Ljung-Box")
    cat(sprintf("    ▪ Ljung-Box (Squared Resids, lag=20):\n"))
    cat(sprintf("      Statistic = %.4f, p-value = %.4f %s\n", 
                lb_test$statistic, 
                lb_test$p.value,
                ifelse(lb_test$p.value > 0.05, "✓ (No ARCH effects)", "✗ (ARCH effects remain)")))
    
    # Jarque-Bera test for normality.
    # A high p-value suggests residuals are normally distributed.
    jb_test <- tseries::jarque.bera.test(std_resid)
    cat(sprintf("    ▪ Jarque-Bera Normality Test:\n"))
    cat(sprintf("      Statistic = %.4f, p-value = %.4f %s\n", 
                jb_test$statistic, 
                jb_test$p.value,
                ifelse(jb_test$p.value > 0.05, "✓ (Normal)", "✗ (Not Normal)")))
    
    # Shapiro-Wilk test (more powerful for smaller samples).
    if (length(std_resid) <= 5000) {
      sw_test <- shapiro.test(std_resid)
      cat(sprintf("    ▪ Shapiro-Wilk Normality Test:\n"))
      cat(sprintf("      Statistic = %.4f, p-value = %.4f %s\n", 
                  sw_test$statistic, 
                  sw_test$p.value,
                  ifelse(sw_test$p.value > 0.05, "✓", "✗")))
    }
  }
  
  # --- Economic Interpretation ---
  cat("\n【 Economic Interpretation 】\n")
  cat("────────────────────────────────────────────────────────────────\n")
  
  if ("theta1" %in% names(est_coef) && !is.na(estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "theta1"])) {
    if (estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "theta1"] < 0.05) {
      direction <- ifelse(est_coef["theta1"] > 0, "positive", "negative")
      cat(sprintf("  ▪ EPU has a significant %s effect on long-run volatility (θ₁ = %.4f, p < 0.05).\n", 
                  direction, est_coef["theta1"]))
    } else {
      cat("  ▪ The effect of EPU on long-run volatility is not statistically significant.\n")
    }
  }
  
  if ("theta2" %in% names(est_coef) && !is.na(estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "theta2"])) {
    if (estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "theta2"] < 0.05) {
      direction <- ifelse(est_coef["theta2"] > 0, "positive", "negative")
      cat(sprintf("  ▪ SMP has a significant %s effect on long-run volatility (θ₂ = %.4f, p < 0.05).\n", 
                  direction, est_coef["theta2"]))
    } else {
      cat("  ▪ The effect of SMP on long-run volatility is not statistically significant.\n")
    }
  }
  
  if ("gamma" %in% names(est_coef) && object$skew == "YES") {
    if (!is.na(estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "gamma"]) &&
        estimate_table$`Pr(>|t|)`[estimate_table$Parameter == "gamma"] < 0.05) {
      cat(sprintf("  ▪ A significant leverage effect exists (γ = %.4f, p < 0.05).\n", est_coef["gamma"]))
      cat("    Negative shocks have a greater impact on volatility than positive shocks.\n")
    } else {
      cat("  ▪ The leverage effect is not statistically significant.\n")
    }
  }
  
  cat("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  
  # Return key results invisibly so they can be stored in a variable if needed.
  invisible(list(
    coefficients = estimate_table,
    loglik = log_lik,
    aic = aic,
    aicc = aicc,
    bic = bic,
    n_obs = n_obs,
    persistence = persistence,
    residual_stats = if(exists("resid_stats")) resid_stats else NULL
  ))
}


# ==============================================================================
# Module 5: S3 Plot Method for `midas_result`
# ==============================================================================
# Creates a suite of diagnostic and analytical plots for a fitted model.
plot.midas_result <- function(x, which = 1:8, main_title = NULL, 
                              save_plots = TRUE, format = c("svg", "png"), 
                              dpi = 300, width = 12, height = NULL, ...) {
  if (!inherits(x, "midas_result")) {
    stop("Input must be a 'midas_result' object.")
  }
  
  # Get fitted values and forecasts from the model.
  vol_preds <- tryCatch(predict(x), error = function(e) NULL)
  if(is.null(vol_preds)) {
    cat("Could not generate predictions, skipping plots.\n")
    return(invisible())
  }
  
  if (is.null(main_title)) {
    main_title <- "GARCH-MIDAS Model Analysis"
  }
  
  # Define plot color palette.
  academic_palette <- c(
    "#1f77b4",  # Deep Blue
    "#ff7f0e",  # Orange
    "#2ca02c",  # Green
    "#d62728",  # Red
    "#9467bd"   # Purple
  )
  
  plot_list <- list()
  params <- coef(x$model_fit)
  
  # --- Plot 1: Volatility Decomposition ---
  # Shows realized volatility vs. fitted short-run and long-run components.
  if(1 %in% which) {
    # Combine data into a single xts object. Use make.names-style names for compatibility.
    vol_data <- merge(
      xts::xts(abs(x$data$daily_ret) * 100, order.by=index(x$data$daily_ret)),
      xts::xts(vol_preds$total_vol * 100, order.by=index(vol_preds$total_vol)),
      xts::xts(vol_preds$long_run_vol * 100, order.by=index(vol_preds$long_run_vol))
    )
    colnames(vol_data) <- c("Realized.Volatility", "Conditional.Volatility", "Long.run.Component")
    
    df_vol <- data.frame(
      Date = index(vol_data),
      zoo::coredata(vol_data),
      check.names = FALSE
    )
    
    # Pivot to long format for ggplot.
    df_vol_long <- tidyr::pivot_longer(
      df_vol, -Date, 
      names_to = "Component", 
      values_to = "Volatility"
    )
    
    # Prettify component names for the legend.
    df_vol_long$Component <- gsub("\\.", " ", df_vol_long$Component)
    
    # Set factor order for the legend.
    df_vol_long$Component <- factor(
      df_vol_long$Component,
      levels = c("Realized Volatility", "Conditional Volatility", "Long run Component")
    )
    
    p1 <- ggplot(df_vol_long, aes(x = Date, y = Volatility)) +
      geom_line(aes(color = Component, linetype = Component), linewidth = 0.8) +
      scale_color_manual(
        values = c("Realized Volatility" = "gray60",
                   "Conditional Volatility" = academic_palette[1],
                   "Long run Component" = academic_palette[2]),
        name = NULL
      ) +
      scale_linetype_manual(
        values = c("Realized Volatility" = "solid",
                   "Conditional Volatility" = "solid",
                   "Long run Component" = "dashed"),
        name = NULL
      ) +
      labs(
        title = "GARCH-MIDAS Volatility Decomposition",
        subtitle = "Daily Return Volatility (%)",
        x = NULL,
        y = "Volatility (%)"
      ) +
      scale_y_continuous(labels = scales::number_format(accuracy = 0.1)) +
      scale_x_date(date_labels = "%Y", date_breaks = "2 years") +
      theme(legend.position = "bottom")
    
    # Add shaded regions for specified time periods (e.g., administrations).
    if (!is.null(ADMIN_DATES)) {
      p1 <- p1 + 
        annotate("rect", 
                 xmin = ADMIN_DATES$start_date, 
                 xmax = ADMIN_DATES$end_date,
                 ymin = -Inf, ymax = Inf,
                 fill = rep(c("blue", "red"), length.out = nrow(ADMIN_DATES)),
                 alpha = 0.1)
    }
    
    plot_list[[1]] <- p1
  }
  
  # --- Plot 2: MIDAS Beta Weighting Function ---
  # Visualizes how much weight is given to each monthly lag.
  if(2 %in% which && all(c("w1", "w2") %in% names(params))) {
    weights <- rumidas::beta_function(1:x$K_epu, x$K_epu, params["w1"], params["w2"])
    weight_df <- data.frame(
      Lag = factor(1:x$K_epu),
      Weight = weights
    )
    
    p2 <- ggplot(weight_df, aes(x = Lag, y = Weight)) +
      geom_col(fill = academic_palette[1], alpha = 0.8, width = 0.7) +
      geom_text(aes(label = sprintf("%.3f", Weight)), 
                vjust = -0.5, size = 4, fontface = "bold") +
      labs(
        title = "MIDAS Beta Weighting Function",
        subtitle = latex2exp::TeX(sprintf("$w_1 = %.3f, w_2 = %.3f$", 
                                          params["w1"], params["w2"])),
        x = "Lag (months)",
        y = "Weight"
      ) +
      scale_y_continuous(limits = c(0, max(weights) * 1.15),
                         labels = scales::number_format(accuracy = 0.01)) +
      theme(panel.grid.major.x = element_blank())
    
    plot_list[[2]] <- p2
  }
  
  # --- Plot 3: Long-run Volatility Drivers ---
  # Decomposes the long-run component (tau) into its drivers (EPU, SMP, etc.).
  if(3 %in% which && all(c("theta0", "theta1", "theta2", "theta3") %in% names(params))) {
    df_drivers_aligned <- merge(vol_preds$midas_epu_ts, x$data$smp_contemp, join = "inner")
    colnames(df_drivers_aligned) <- c("midas_epu", "smp")
    
    # Calculate the contribution of each component to log(tau).
    df_drivers <- data.frame(
      Date = index(df_drivers_aligned),
      Intercept = params["theta0"],
      `EPU Effect` = params["theta1"] * as.numeric(df_drivers_aligned$midas_epu),
      `SMP Effect` = params["theta2"] * as.numeric(df_drivers_aligned$smp),
      `Interaction` = params["theta3"] * as.numeric(df_drivers_aligned$midas_epu) * as.numeric(df_drivers_aligned$smp),
      check.names = FALSE
    )
    
    # Calculate the total effect (sum of components).
    df_drivers$Total <- rowSums(df_drivers[, -1])
    
    df_drivers_long <- tidyr::pivot_longer(
      df_drivers[, -ncol(df_drivers)], -Date,
      names_to = "Driver",
      values_to = "Contribution"
    )
    
    # Set factor order for stacking in the area plot.
    df_drivers_long$Driver <- factor(
      df_drivers_long$Driver,
      levels = c("Interaction", "SMP Effect", "EPU Effect", "Intercept")
    )
    
    p3 <- ggplot(df_drivers_long, aes(x = Date, y = Contribution, fill = Driver)) +
      geom_area(alpha = 0.8, position = "stack") +
      geom_line(data = df_drivers, aes(x = Date, y = Total), 
                color = "black", linewidth = 1, inherit.aes = FALSE) +
      scale_fill_manual(
        name = "Component",
        values = c("Intercept" = academic_palette[3],
                   "EPU Effect" = academic_palette[1],
                   "SMP Effect" = academic_palette[2],
                   "Interaction" = academic_palette[4])
      ) +
      labs(
        title = "Long-run Volatility Decomposition",
        subtitle = latex2exp::TeX("Contributions to $\\log(\\tau_t)$"),
        x = NULL,
        y = "Log Volatility Contribution"
      ) +
      scale_x_date(date_labels = "%Y", date_breaks = "2 years") +
      theme(legend.position = "bottom")
    
    plot_list[[3]] <- p3
  }
  
  # --- Plot 4: Standardized Residual Diagnostics (4-in-1 Panel) ---
  if(4 %in% which) {
    std_residuals <- as.numeric(vol_preds$residuals)
    resid_df <- data.frame(
      Date = index(vol_preds$residuals),
      Residuals = std_residuals
    )
    
    # 4a: Time series plot of residuals.
    p4a <- ggplot(resid_df, aes(x = Date, y = Residuals)) +
      geom_point(alpha = 0.5, size = 0.8, color = academic_palette[1]) +
      geom_hline(yintercept = c(-3, -2, 2, 3), 
                 linetype = "dashed", 
                 color = c("red", "orange", "orange", "red"),
                 alpha = 0.7) +
      geom_hline(yintercept = 0, color = "black") +
      labs(title = "Standardized Residuals", x = NULL, y = "Residuals") +
      scale_x_date(date_labels = "%Y", date_breaks = "2 years")
    
    # 4b: Q-Q plot to check for normality.
    p4b <- ggplot(resid_df, aes(sample = Residuals)) +
      stat_qq(color = academic_palette[1], alpha = 0.7) +
      stat_qq_line(color = "red", linewidth = 1) +
      labs(title = "Normal Q-Q Plot", 
           x = "Theoretical Quantiles", 
           y = "Sample Quantiles")
    
    # 4c: Density plot compared to a standard normal distribution.
    p4c <- ggplot(resid_df, aes(x = Residuals)) +
      geom_histogram(aes(y = after_stat(density)), bins = 50, 
                     fill = academic_palette[1], alpha = 0.5) +
      geom_density(color = academic_palette[2], linewidth = 1) +
      stat_function(fun = dnorm, args = list(mean = 0, sd = 1),
                    color = "red", linewidth = 1, linetype = "dashed") +
      labs(title = "Density Plot", x = "Residuals", y = "Density") +
      xlim(-5, 5)
    
    # 4d: ACF plot of squared residuals to check for remaining ARCH effects.
    acf_data <- acf(std_residuals^2, plot = FALSE, lag.max = 30)
    acf_df <- data.frame(
      Lag = acf_data$lag[-1],
      ACF = acf_data$acf[-1]
    )
    ci <- qnorm(0.975) / sqrt(length(std_residuals))
    
    p4d <- ggplot(acf_df, aes(x = Lag, y = ACF)) +
      geom_col(fill = academic_palette[1], alpha = 0.8, width = 0.8) +
      geom_hline(yintercept = c(-ci, ci), linetype = "dashed", 
                 color = "red", alpha = 0.8) +
      geom_hline(yintercept = 0, color = "black") +
      labs(title = "ACF of Squared Residuals", x = "Lag", y = "ACF") +
      scale_x_continuous(breaks = seq(0, 30, by = 5))
    
    # Combine the four plots into a single panel using 'patchwork'.
    p4_combined <- (p4a + p4b) / (p4c + p4d) +
      plot_annotation(
        title = "Residual Diagnostics",
        theme = theme(plot.title = element_text(size = 16, face = "bold", hjust = 0.5))
      )
    
    plot_list[[4]] <- p4_combined
  }
  
  # --- Plot 5: Model Fit Evaluation ---
  if(5 %in% which) {
    # Calculate cumulative squared prediction error over time.
    actual_vol <- abs(as.numeric(x$data$daily_ret)) * 100
    fitted_vol <- as.numeric(vol_preds$total_vol) * 100
    
    cum_error_df <- data.frame(
      Date = index(x$data$daily_ret),
      `Cumulative Squared Error` = cumsum((actual_vol - fitted_vol)^2),
      check.names = FALSE
    )
    
    p5a <- ggplot(cum_error_df, aes(x = Date, y = `Cumulative Squared Error`)) +
      geom_line(color = academic_palette[1], linewidth = 1) +
      labs(title = "Cumulative Squared Prediction Error",
           x = NULL, y = "CSE") +
      scale_x_date(date_labels = "%Y", date_breaks = "2 years")
    
    # Scatter plot of actual vs. fitted volatility.
    scatter_df <- data.frame(
      Actual = actual_vol,
      Fitted = fitted_vol
    )
    
    # Calculate R-squared as a measure of fit.
    r_squared <- cor(actual_vol, fitted_vol, use = "complete.obs")^2
    
    p5b <- ggplot(scatter_df, aes(x = Actual, y = Fitted)) +
      geom_point(alpha = 0.3, color = academic_palette[1]) +
      geom_abline(intercept = 0, slope = 1, color = "red", linewidth = 1) +
      geom_smooth(method = "lm", se = TRUE, color = academic_palette[2]) +
      labs(title = "Actual vs Fitted Volatility",
           subtitle = latex2exp::TeX(sprintf("$R^2 = %.3f$", r_squared)),
           x = "Actual Volatility (%)",
           y = "Fitted Volatility (%)") +
      # Force a 1:1 aspect ratio for accurate visual comparison.
      coord_equal()
    
    p5_combined <- p5a / p5b
    plot_list[[5]] <- p5_combined
  }
  
  # --- Plot 6: Long-run Volatility Surface ---
  # Visualizes the joint effect of EPU and SMP on long-run volatility.
  if(6 %in% which && "theta1" %in% names(params)) {
    # Create a grid of EPU and SMP values to calculate the surface.
    epu_range <- seq(-2, 2, length.out = 50)
    smp_range <- seq(-2, 2, length.out = 50)
    
    grid_data <- expand.grid(EPU = epu_range, SMP = smp_range)
    grid_data$LogTau <- params["theta0"] + 
      params["theta1"] * grid_data$EPU +
      params["theta2"] * grid_data$SMP +
      params["theta3"] * grid_data$EPU * grid_data$SMP
    
    p6 <- ggplot(grid_data, aes(x = EPU, y = SMP, z = LogTau, fill = LogTau)) +
      geom_tile() +
      geom_contour(color = "white", alpha = 0.5) +
      scale_fill_viridis_c(name = latex2exp::TeX("$\\log(\\tau)$")) +
      labs(
        title = "Long-run Volatility Surface",
        subtitle = "Joint Effects of EPU and SMP",
        x = "EPU (standardized)",
        y = "SMP (standardized)"
      ) +
      coord_equal()
    
    plot_list[[6]] <- p6
  }
  
  # --- Plot 7: Volatility Forecast ---
  if(7 %in% which && !is.null(vol_preds$forecast)) {
    # Combine the last 60 days of historical data with the forecast.
    hist_vol <- tail(vol_preds$total_vol, 60)
    
    forecast_df <- data.frame(
      Date = c(index(hist_vol), index(vol_preds$forecast$forecast)),
      Volatility = c(as.numeric(hist_vol) * 100, 
                     as.numeric(vol_preds$forecast$forecast) * 100),
      Type = c(rep("Historical", length(hist_vol)),
               rep("Forecast", length(vol_preds$forecast$forecast)))
    )
    
    # Add forecast confidence intervals.
    if(length(vol_preds$forecast$forecast) > 0) {
      forecast_df$Lower <- c(rep(NA, length(hist_vol)),
                             as.numeric(vol_preds$forecast$lower) * 100)
      forecast_df$Upper <- c(rep(NA, length(hist_vol)),
                             as.numeric(vol_preds$forecast$upper) * 100)
    }
    
    p7 <- ggplot(forecast_df, aes(x = Date, y = Volatility)) +
      geom_line(aes(color = Type), linewidth = 1) +
      geom_ribbon(aes(ymin = Lower, ymax = Upper), 
                  fill = academic_palette[1], alpha = 0.2) +
      scale_color_manual(values = c("Historical" = academic_palette[1],
                                    "Forecast" = academic_palette[2]),
                         name = NULL) +
      labs(
        title = "Volatility Forecast",
        subtitle = "Out-of-sample predictions with 95% confidence interval",
        x = NULL,
        y = "Volatility (%)"
      ) +
      theme(legend.position = "bottom")
    
    plot_list[[7]] <- p7
  }
  
  # --- Plot 8: Model Performance Radar Chart ---
  # Provides a multi-dimensional summary of model quality.
  if(8 %in% which) {
    # Calculate various model evaluation metrics.
    vol_preds_calc <- predict(x)
    resid <- as.numeric(vol_preds_calc$residuals)
    
    # Standardize metrics to a 0-1 scale, where higher is better.
    metrics <- data.frame(
      Metric = c("Convergence", "Persistence", "Normality", 
                 "No ARCH", "Stability", "Parsimony"),
      Value = c(
        ifelse(x$model_fit$code < 3, 1, 0),                           # Convergence
        1 - min(abs(params["alpha"] + params["beta"] - 1), 1),        # Persistence (closer to 1 is worse)
        max(0, 1 - abs(moments::skewness(resid, na.rm=TRUE))/2),      # Normality (based on skewness)
        min(1, 2 * Box.test(resid^2, lag=20, type="Ljung-Box")$p.value), # No ARCH effect
        max(0, 1 - abs(sd(resid, na.rm=TRUE) - 1)),                   # Stability (residual SD close to 1)
        1 - length(params)/15                                        # Parsimony (fewer params is better)
      )
    )
    
    # Constrain values to the [0, 1] range.
    metrics$Value <- pmax(0, pmin(1, metrics$Value))
    
    # Create the polar coordinate (radar) plot.
    p8 <- ggplot(metrics, aes(x = Metric, y = Value, group = 1)) +
      geom_polygon(fill = academic_palette[1], alpha = 0.3) +
      geom_line(color = academic_palette[1], linewidth = 1) +
      geom_point(color = academic_palette[1], size = 3) +
      coord_polar() +
      scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, 0.25)) +
      labs(
        title = "Model Performance Radar Chart",
        subtitle = "Multi-dimensional model evaluation",
        x = NULL, y = NULL
      ) +
      theme(
        axis.text.x = element_text(size = 12, face = "bold"),
        panel.grid.major = element_line(color = "gray80"),
        axis.text.y = element_blank(),
        axis.ticks.y = element_blank()
      )
    
    plot_list[[8]] <- p8
  }
  
  # Filter out any plots that were not generated.
  valid_plots <- Filter(Negate(is.null), plot_list)
  
  # Save all generated plots to the output directory.
  if (save_plots && length(valid_plots) > 0 && !is.null(OUTPUT_DIR)) {
    fig_dir <- file.path(OUTPUT_DIR, "figures", make.names(main_title))
    if (!dir.exists(fig_dir)) dir.create(fig_dir, recursive = TRUE)
    
    plot_names <- c("volatility_decomposition", "beta_weights", "drivers", 
                    "residual_diagnostics", "model_fit", "volatility_surface",
                    "forecast", "performance_radar")
    
    # Save each individual plot in the specified formats.
    for (i in seq_along(valid_plots)) {
      current_plot_index <- which[i]
      plot_name <- plot_names[current_plot_index]
      
      for (fmt in format) {
        filename <- file.path(fig_dir, paste0(plot_name, ".", fmt))
        
        # Dynamically adjust plot dimensions for better layout.
        current_height <- if(is.null(height)) 7 else height
        current_width <- if(is.null(width)) 10 else width
        
        if(grepl("combined|diagnostics", plot_name)) {
          current_height <- 10
          current_width <- 12
        }
        
        ggsave(filename, plot = valid_plots[[i]], 
               device = fmt, width = current_width, height = current_height, dpi = dpi)
        cat("  Saved:", filename, "\n")
      }
    }
    
    # Create a combined plot of all generated figures for a single overview.
    if (length(valid_plots) > 1) {
      combined_plot <- wrap_plots(valid_plots, ncol = 2, guides = 'collect') +
        plot_annotation(
          title = main_title,
          theme = theme(
            plot.title = element_text(size = 18, face = "bold", hjust = 0.5)
          )
        )
      
      for (fmt in format) {
        filename <- file.path(fig_dir, paste0("combined_all.", fmt))
        ggsave(filename, plot = combined_plot, 
               device = fmt, width = 16, height = length(valid_plots) * 3, dpi = dpi, limitsize = FALSE)
        cat("  Saved combined plot:", filename, "\n")
      }
    }
  }
  
  invisible(plot_list)
}


# ==============================================================================
# Module 6: Robustness Analysis (Parallelized)
# ==============================================================================
# Performs robustness checks by re-estimating the model under different
# specifications (e.g., varying K, distribution, asymmetry).
robustness_analysis <- function(model_data, K_values, distributions, 
                                skew_options, verbose = TRUE) {
  
  cat("\nStarting robustness analysis (parallelized)...\n")
  cat("==============================================\n")
  
  # Create a grid of all parameter combinations to test.
  param_grid <- expand.grid(
    K = K_values,
    Distribution = distributions,
    Asymmetry = skew_options,
    stringsAsFactors = FALSE
  )
  
  total_models <- nrow(param_grid)
  cat("Total models to estimate:", total_models, "\n")
  cat("Using", getOption("mc.cores", 1), "CPU cores\n\n")
  
  # Estimate all model specifications in parallel.
  results_list <- future.apply::future_lapply(1:total_models, function(i) {
    params <- param_grid[i, ]
    
    if (verbose) {
      cat(sprintf("[%d/%d] K=%d, Dist=%s, Asymm=%s\n", 
                  i, total_models, params$K, params$Distribution, params$Asymmetry))
    }
    
    tryCatch({
      # Prepare data for the specific K value.
      prep_data <- prepare_midas_data(
        model_data$daily_ret_original,
        model_data$monthly_epu_original,
        model_data$monthly_smp_original,
        params$K
      )
      
      # Fit the model with the current specification.
      model_result <- fit_garch_midas(
        model_data = prep_data,
        K_epu = params$K,
        distribution = params$Distribution,
        skew = params$Asymmetry,
        allow_bell_shape = FALSE,
        verbose = FALSE
      )
      
      if (!is.null(model_result)) {
        # Extract key results: log-likelihood, information criteria, etc.
        coefs <- coef(model_result$model_fit)
        n_obs <- model_result$data$n_obs
        loglik <- model_result$model_fit$maximum
        n_params <- length(coefs)
        aic <- -2 * loglik + 2 * n_params
        bic <- -2 * loglik + log(n_obs) * n_params
        
        persistence <- coefs["alpha"] + coefs["beta"] + 
          if(params$Asymmetry == "YES" && "gamma" %in% names(coefs)) {
            0.5 * abs(coefs["gamma"])
          } else 0
        
        return(data.frame(
          K = params$K,
          Distribution = params$Distribution,
          Asymmetry = params$Asymmetry,
          LogLik = loglik,
          AIC = aic,
          BIC = bic,
          Persistence = persistence,
          Converged = model_result$model_fit$code < 3,
          stringsAsFactors = FALSE
        ))
      }
      
      return(NULL)
      
    }, error = function(e) {
      if (verbose) cat("  Error:", e$message, "\n")
      return(NULL)
    })
  }, future.seed = TRUE)
  
  # Combine results from all parallel jobs into a single table.
  results_table <- do.call(rbind, Filter(Negate(is.null), results_list))
  
  if (nrow(results_table) > 0) {
    # Sort results by AIC to easily identify the best-fitting models.
    results_table <- results_table[order(results_table$AIC), ]
    
    # Identify the best models according to AIC and BIC.
    best_aic_idx <- which.min(results_table$AIC)
    best_bic_idx <- which.min(results_table$BIC)
    
    cat("\n==============================================\n")
    cat("Robustness Analysis Summary:\n")
    cat("==============================================\n")
    cat("Successfully estimated models:", nrow(results_table), "/", total_models, "\n\n")
    
    # Print the summary table.
    print(knitr::kable(
      results_table,
      format = "simple",
      digits = c(0, 0, 0, 2, 2, 2, 4, 0),
      caption = "Model Comparison Results"
    ))
    
    cat("\nBest Model (by AIC):\n")
    print(results_table[best_aic_idx, ])
    
    cat("\nBest Model (by BIC):\n")
    print(results_table[best_bic_idx, ])
    
    # Create a visualization of the results.
    if (!is.null(OUTPUT_DIR)) {
      # Plot AIC and BIC values for visual comparison.
      results_long <- tidyr::pivot_longer(
        results_table,
        cols = c("AIC", "BIC"),
        names_to = "Criterion",
        values_to = "Value"
      )
      
      results_long$Model <- paste0("K=", results_long$K, 
                                   ", ", substr(results_long$Distribution, 1, 1),
                                   ", ", substr(results_long$Asymmetry, 1, 1))
      
      p_robust <- ggplot(results_long, aes(x = reorder(Model, Value), y = Value)) +
        geom_point(aes(color = Criterion), size = 3) +
        geom_line(aes(group = Criterion, color = Criterion)) +
        facet_wrap(~ Criterion, scales = "free_y") +
        labs(
          title = "Model Selection Criteria Comparison",
          subtitle = "Results from Robustness Analysis",
          x = "Model Specification",
          y = "Information Criterion Value"
        ) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
        scale_color_manual(values = c("AIC" = "#1f77b4", "BIC" = "#ff7f0e"))
      
      # Save the plot.
      robust_fig_path <- file.path(OUTPUT_DIR, "figures", "robustness_analysis.png")
      ggsave(
        robust_fig_path,
        plot = p_robust,
        width = 12, height = 6, dpi = 300
      )
      cat("\nSaved robustness analysis plot to:", robust_fig_path, "\n")
    }
  } else {
    cat("Warning: All robustness model estimations failed.\n")
  }
  
  return(results_table)
}


# ==============================================================================
# Module 7: Rolling Window Analysis (Parallelized)
# ==============================================================================
# Examines parameter stability by re-estimating the model over a series of
# moving time windows.
rolling_window_analysis <- function(daily_prices, monthly_epu, monthly_smp, 
                                    sector_name, window_size, step_size, K_epu,
                                    distribution, skew) {
  
  cat("\nStarting rolling window analysis (parallelized) for:", sector_name, "\n")
  
  # Calculate returns for the full sample.
  daily_ret_full <- na.omit(diff(log(daily_prices)) * 100)
  n_total <- length(daily_ret_full)
  
  # Define the start points for each rolling window.
  window_starts <- seq(1, n_total - window_size + 1, by = step_size)
  n_windows <- length(window_starts)
  
  cat("Total windows to estimate:", n_windows, "\n")
  cat("Using", getOption("mc.cores", 1), "CPU cores\n")
  
  # Create a progress bar for monitoring.
  pb <- txtProgressBar(min = 0, max = n_windows, style = 3)
  
  # Process all windows in parallel.
  results_list <- future.apply::future_lapply(1:n_windows, function(i) {
    # Optional progress update (may not be smooth in parallel but gives an idea).
    # try(setTxtProgressBar(pb, i)) 
    
    window_end <- window_starts[i] + window_size - 1
    window_ret <- daily_ret_full[window_starts[i]:window_end]
    
    tryCatch({
      # Prepare data for the current window.
      model_data <- prepare_midas_data(window_ret, monthly_epu, monthly_smp, K_epu)
      
      # Fit the model.
      model_result <- fit_garch_midas(
        model_data = model_data,
        K_epu = K_epu,
        distribution = distribution,
        skew = skew,
        allow_bell_shape = FALSE,
        verbose = FALSE
      )
      
      if (!is.null(model_result)) {
        params <- coef(model_result$model_fit)
        
        # Calculate additional statistics for the window.
        vol_pred <- predict(model_result)
        avg_vol <- mean(as.numeric(vol_pred$total_vol), na.rm = TRUE)
        
        # The window end date is needed for plotting time-varying parameters.
        window_end_date <- index(window_ret)[length(index(window_ret))]
        
        return(list(
          model = model_result,
          params = data.frame(
            Window = i,
            EndDate = window_end_date,
            AvgVol = avg_vol,
            t(params)
          )
        ))
      }
      
      return(NULL)
      
    }, error = function(e) {
      return(NULL)
    })
  }, future.seed = TRUE)
  
  close(pb)
  
  # Extract and combine results from all windows.
  valid_results <- Filter(Negate(is.null), results_list)
  rolling_models <- lapply(valid_results, function(x) x$model)
  param_paths <- do.call(rbind, lapply(valid_results, function(x) x$params))
  
  cat("\nSuccessfully estimated windows:", length(valid_results), "/", n_windows, "\n")
  
  # Calculate summary statistics for parameter stability.
  if (nrow(param_paths) > 0) {
    param_stability <- data.frame(
      Parameter = names(param_paths)[-(1:3)],
      Mean = apply(param_paths[, -(1:3)], 2, mean, na.rm = TRUE),
      SD = apply(param_paths[, -(1:3)], 2, sd, na.rm = TRUE),
      CV = apply(param_paths[, -(1:3)], 2, function(x) sd(x, na.rm=TRUE)/mean(x, na.rm=TRUE))
    )
    
    cat("\nParameter Stability Statistics:\n")
    print(param_stability)
  }
  
  return(list(
    results = rolling_models,
    param_paths = param_paths,
    window_info = data.frame(
      window_size = window_size,
      step_size = step_size,
      n_windows = n_windows,
      success_rate = length(valid_results) / n_windows
    ),
    stability_stats = if(exists("param_stability")) param_stability else NULL
  ))
}


# ==============================================================================
# Module 8: LaTeX Table Generation
# ==============================================================================
# Generates a publication-quality LaTeX table from a list of fitted models.
generate_latex_table <- function(model_results, caption = NULL, label = NULL) {
  if (length(model_results) == 0) {
    cat("No model results available to generate a table.\n")
    return(invisible())
  }
  
  # Set default caption and label if not provided.
  if (is.null(caption)) {
    caption <- "GARCH-MIDAS Model Estimation Results"
  }
  if (is.null(label)) {
    label <- "tab:garch_midas_results"
  }
  
  # Extract and format results from each model in the list.
  all_results <- lapply(names(model_results), function(sector_name) {
    model <- model_results[[sector_name]]
    if (is.null(model)) return(NULL)
    
    # Get the summary object which contains formatted results.
    summary_obj <- summary(model)
    coeffs <- summary_obj$coefficients
    
    # Function to format coefficients with standard errors and significance stars.
    format_coef <- function(est, se, p_val) {
      stars <- ""
      if (!is.na(p_val)) {
        if (p_val < 0.001) stars <- "$^{***}$"
        else if (p_val < 0.01) stars <- "$^{**}$"
        else if (p_val < 0.05) stars <- "$^{*}$"
        else if (p_val < 0.1) stars <- "$^{\\dagger}$"
      }
      
      if(is.na(se)) {
        return(sprintf("%.4f%s", est, stars))
      } else {
        # Format as: Estimate***\n(Std.Error)
        return(sprintf("%.4f%s\\newline(%.4f)", est, stars, se))
      }
    }
    
    # Apply the formatting function to all coefficients.
    formatted_coeffs <- mapply(
      format_coef,
      coeffs$Estimate,
      coeffs$`Std.Error`,
      coeffs$`Pr(>|t|)`
    )
    
    # Map parameter names to their LaTeX symbols for the table.
    param_labels <- c(
      omega = "$\\omega$",
      alpha = "$\\alpha$",
      beta = "$\\beta$",
      gamma = "$\\gamma$",
      theta0 = "$\\theta_0$",
      theta1 = "$\\theta_1$",
      theta2 = "$\\theta_2$",
      theta3 = "$\\theta_3$",
      w1 = "$w_1$",
      w2 = "$w_2$",
      nu = "$\\nu$"
    )
    
    # Apply the LaTeX labels.
    names(formatted_coeffs) <- rownames(coeffs)
    names(formatted_coeffs) <- sapply(names(formatted_coeffs), function(x) {
      ifelse(x %in% names(param_labels), param_labels[x], x)
    })
    
    # Extract and format model fit statistics.
    fit_stats <- list(
      N = format(summary_obj$n_obs, big.mark = ","),
      `Log-L` = sprintf("%.2f", summary_obj$loglik),
      AIC = sprintf("%.2f", summary_obj$aic),
      BIC = sprintf("%.2f", summary_obj$bic),
      Persistence = sprintf("%.4f", summary_obj$persistence)
    )
    
    # Combine all results for the model.
    return(c(formatted_coeffs, fit_stats))
  })
  
  names(all_results) <- names(model_results)
  
  # Filter out any models that failed to estimate.
  all_results <- Filter(Negate(is.null), all_results)
  if (length(all_results) == 0) {
    cat("All model estimations failed; cannot generate table.\n"); return(invisible())
  }
  
  # Get a unique, ordered list of all parameters across all models.
  all_params <- unique(unlist(lapply(all_results, names)))
  
  # Define a canonical parameter order for the table.
  param_order <- c(
    "$\\omega$", "$\\alpha$", "$\\beta$", "$\\gamma$",
    "$\\theta_0$", "$\\theta_1$", "$\\theta_2$", "$\\theta_3$",
    "$w_1$", "$w_2$", "$\\nu$",
    "N", "Log-L", "AIC", "BIC", "Persistence"
  )
  
  # Reorder parameters according to the canonical order.
  all_params <- c(
    param_order[param_order %in% all_params],
    all_params[!all_params %in% param_order]
  )
  
  # Build the data frame that will become the table.
  table_df <- data.frame(
    Parameter = all_params,
    stringsAsFactors = FALSE
  )
  
  # Add a column for each sector's results.
  for(sector_name in names(all_results)) {
    sector_results <- all_results[[sector_name]]
    if(!is.null(sector_results)) {
      table_df[[sector_name]] <- sapply(all_params, function(p) {
        if(p %in% names(sector_results)) {
          return(sector_results[[p]])
        } else {
          return("—") # Use an em-dash for missing parameters.
        }
      })
    }
  }
  
  # Use the 'kableExtra' package to generate a high-quality LaTeX table.
  kable_latex <- knitr::kable(
    table_df,
    format = "latex",
    booktabs = TRUE,
    caption = caption,
    label = label,
    escape = FALSE, # Allow LaTeX commands in cells.
    align = c("l", rep("c", ncol(table_df)-1))
  ) %>%
    kableExtra::kable_styling(
      latex_options = c("striped", "hold_position", "scale_down"),
      font_size = 9
    ) %>%
    kableExtra::add_header_above(c(" " = 1, "Sector Models" = length(all_results))) %>%
    kableExtra::pack_rows("Short-run (GARCH)", 1, which(all_params == "$\\gamma$")) %>%
    kableExtra::pack_rows("Long-run (MIDAS)", which(all_params == "$\\theta_0$"), which(all_params == "$\\nu$")) %>%
    kableExtra::pack_rows("Model Statistics", which(all_params == "N"), nrow(table_df))
  
  # Save the generated LaTeX code to a .tex file.
  if (!is.null(OUTPUT_DIR)) {
    table_dir <- file.path(OUTPUT_DIR, "tables")
    if (!dir.exists(table_dir)) dir.create(table_dir, recursive = TRUE)
    
    kable_file <- file.path(table_dir, "model_results_kable.tex")
    writeLines(as.character(kable_latex), kable_file)
    cat("KableExtra LaTeX table saved to:", kable_file, "\n")
  }
  
  return(invisible(kable_latex))
}

# ==============================================================================
# Module 9: Descriptive Data Visualization
# ==============================================================================
# Creates and saves an initial plot of the key input time series.
plot_descriptive_data <- function(loaded_data, main_index_name, epu_name, smp_name, output_dir) {
  cat("\n  ▶ Generating descriptive data plots...\n")
  
  # 1. Plot of the main stock index daily returns.
  daily_prices <- loaded_data$daily_prices[, make.names(main_index_name)]
  daily_returns <- na.omit(diff(log(daily_prices)) * 100)
  returns_df <- data.frame(Date = index(daily_returns), Returns = coredata(daily_returns))
  colnames(returns_df) <- c("Date", "Returns")
  
  p_returns <- ggplot(returns_df, aes(x = Date, y = Returns)) +
    geom_line(color = "#1f77b4", alpha = 0.8) +
    geom_rect(data = ADMIN_DATES, aes(xmin = start_date, xmax = end_date, ymin = -Inf, ymax = Inf, fill = administration), inherit.aes = FALSE, alpha = 0.1) +
    scale_fill_manual(values = c("Trump" = "blue", "Biden" = "red"), name = "Administration") +
    labs(title = paste(main_index_name, "Daily Returns"), y = "Returns (%)", x = NULL) +
    scale_x_date(date_labels = "%Y", date_breaks = "2 years")
  
  # 2. Plot of the Economic Policy Uncertainty (EPU) index.
  epu_xts <- loaded_data$monthly_data[, make.names(epu_name)]
  epu_df <- data.frame(Date = index(epu_xts), Value = coredata(epu_xts))
  colnames(epu_df) <- c("Date", "Value")
  
  p_epu <- ggplot(epu_df, aes(x = Date, y = Value)) +
    geom_line(color = "#ff7f0e") +
    geom_rect(data = ADMIN_DATES, aes(xmin = start_date, xmax = end_date, ymin = -Inf, ymax = Inf, fill = administration), inherit.aes = FALSE, alpha = 0.1) +
    labs(title = "Economic Policy Uncertainty (EPU) Index", y = "Index Level", x = NULL) +
    scale_x_date(date_labels = "%Y", date_breaks = "2 years") +
    theme(legend.position = "none")
  
  # 3. Plot of the Stock Market Policy (SMP) index.
  smp_xts <- loaded_data$monthly_data[, make.names(smp_name)]
  smp_df <- data.frame(Date = index(smp_xts), Value = coredata(smp_xts))
  colnames(smp_df) <- c("Date", "Value")
  
  p_smp <- ggplot(smp_df, aes(x = Date, y = Value)) +
    geom_line(color = "#2ca02c") +
    geom_rect(data = ADMIN_DATES, aes(xmin = start_date, xmax = end_date, ymin = -Inf, ymax = Inf, fill = administration), inherit.aes = FALSE, alpha = 0.1) +
    labs(title = "Stock Market Policy (SMP) Index", y = "Index Level", x = "Date") +
    scale_x_date(date_labels = "%Y", date_breaks = "2 years") +
    theme(legend.position = "none")
  
  # Combine the three plots into a single figure.
  combined_plot <- (p_returns / p_epu / p_smp) + 
    plot_layout(guides = "collect") & theme(legend.position = 'bottom')
  
  # Save the combined plot.
  fig_path <- file.path(output_dir, "figures", "descriptive_data_analysis.png")
  ggsave(fig_path, plot = combined_plot, width = 10, height = 12, dpi = 300)
  cat("  Saved descriptive data plot to:", fig_path, "\n")
  
  invisible(combined_plot)
}


# ==============================================================================
# Module 10: Rolling Window Visualization
# ==============================================================================
# Creates and saves a plot of the time-varying parameter estimates from the
# rolling window analysis.
plot_rolling_analysis <- function(rolling_results, sector_name, output_dir) {
  cat("\n  ▶ Generating rolling window analysis plots for", sector_name, "...\n")
  
  param_paths <- rolling_results$param_paths
  if(is.null(param_paths) || nrow(param_paths) < 2) {
    cat("  ⚠ Insufficient data from rolling analysis to create plot.\n")
    return(invisible(NULL))
  }
  
  # Select key parameters to visualize their evolution over time.
  key_params <- c("alpha", "beta", "gamma", "theta1", "theta2", "theta3")
  params_to_plot <- intersect(key_params, names(param_paths))
  
  if(length(params_to_plot) == 0) {
    cat("  ⚠ No key parameters found to plot.\n")
    return(invisible(NULL))
  }
  
  # Prepare the data for plotting with ggplot.
  plot_data <- param_paths %>%
    dplyr::select(EndDate, all_of(params_to_plot)) %>%
    tidyr::pivot_longer(
      cols = -EndDate,
      names_to = "Parameter",
      values_to = "Estimate"
    ) %>%
    dplyr::mutate(Parameter = factor(Parameter, levels = params_to_plot))
  
  # Create a faceted plot showing each parameter's path.
  p_rolling <- ggplot(plot_data, aes(x = EndDate, y = Estimate, color = Parameter)) +
    geom_line(linewidth = 1) +
    facet_wrap(~Parameter, scales = "free_y", ncol = 2) +
    geom_rect(data = ADMIN_DATES, aes(xmin = start_date, xmax = end_date, ymin = -Inf, ymax = Inf, fill = administration), inherit.aes = FALSE, alpha = 0.1) +
    scale_fill_manual(values = c("Trump" = "blue", "Biden" = "red"), name = "Administration") +
    labs(
      title = paste("Rolling Window Parameter Estimates:", sector_name),
      subtitle = paste("Window Size:", rolling_results$window_info$window_size, "days"),
      x = "Window End Date",
      y = "Parameter Estimate"
    ) +
    theme(legend.position = "none") +
    scale_x_date(date_labels = "%Y", date_breaks = "2 years")
  
  # Save the plot.
  fig_path <- file.path(output_dir, "figures", paste0("rolling_analysis_", make.names(sector_name), ".png"))
  ggsave(fig_path, plot = p_rolling, width = 12, height = 8, dpi = 300)
  cat("  Saved rolling window analysis plot to:", fig_path, "\n")
  
  invisible(p_rolling)
}


# ==============================================================================
# Module 11: Main Workflow
# ==============================================================================
# This is the main function that orchestrates the entire analysis from start to finish.
main_workflow_enhanced <- function() {
  start_time <- Sys.time()
  set.seed(20240627)
  
  cat("\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("                    GARCH-MIDAS ANALYSIS WORKFLOW                     \n")
  cat("                          Version 41.0                            \n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("\n")
  cat("Start Time:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
  cat("CPU Cores Detected:", parallel::detectCores(), "\n")
  cat("Cores to be Used:", getOption("mc.cores", 1), "\n")
  cat("\n")
  
  # Create the main output directory and subdirectories if they don't exist.
  output_dirs <- c(OUTPUT_DIR, 
                   file.path(OUTPUT_DIR, c("figures", "tables", "reports", "diagnostics")))
  
  sapply(output_dirs, function(dir) {
    if (!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE)
      cat("Created directory:", dir, "\n")
    }
  })
  
  # --- Step 1: Data Loading and Initial Analysis ---
  cat("\n【 Step 1 】Data Loading and Descriptive Analysis\n")
  cat("────────────────────────────────────────────────────────────────\n")
  
  loaded_data <- tryCatch({
    load_and_prepare_data(EXCEL_FILE_PATH, DAILY_SHEET_NAME, MONTHLY_SHEET_NAME)
  }, error = function(e) {
    stop(paste("Data loading failed:", e$message))
  })
  
  # Plot and save descriptive data graphics.
  plot_descriptive_data(loaded_data, SECTOR_NAMES[1], EPU_NAME, SMP_NAME, OUTPUT_DIR)
  
  # Initialize containers to store all results.
  results_container <- list(
    models = list(),
    rolling = list(),
    robustness = list(),
    diagnostics = list(),
    performance = list()
  )
  
  # Data frame to log performance metrics for each sector.
  performance_log <- data.frame()
  
  # --- Step 2: Main Analysis Loop for Each Sector ---
  cat("\n【 Step 2 】Model Estimation and Analysis per Index\n")
  cat("────────────────────────────────────────────────────────────────\n")
  
  for (idx in seq_along(SECTOR_NAMES)) {
    sector <- SECTOR_NAMES[idx]
    sector_start_time <- Sys.time()
    
    cat(sprintf("\n[%d/%d] Processing: %s\n", idx, length(SECTOR_NAMES), sector))
    cat(paste0(rep("━", 60), collapse=""), "\n")
    
    # Check if data for the current sector is available.
    sector_clean <- make.names(sector)
    if (!(sector_clean %in% colnames(loaded_data$daily_prices))) {
      cat("  ⚠ Warning: Data for this index not found. Skipping.\n")
      next
    }
    
    # Prepare data for the specific sector.
    cat("  ▶ Preparing data...\n")
    model_data_container <- tryCatch({
      # Extract price series and calculate log returns.
      price_series <- loaded_data$daily_prices[, sector_clean]
      price_series[price_series <= 0] <- NA # Handle non-positive prices
      daily_returns <- na.omit(diff(log(price_series)) * 100)
      
      # Check for sufficient data after cleaning.
      if (nrow(daily_returns) < 250) {
        stop("Not enough valid observations (< 250).")
      }
      
      # Extract monthly data.
      monthly_epu <- loaded_data$monthly_data[, make.names(EPU_NAME)]
      monthly_smp <- loaded_data$monthly_data[, make.names(SMP_NAME)]
      
      # Prepare data in the final MIDAS format.
      prep_data <- prepare_midas_data(daily_returns, monthly_epu, monthly_smp, K_EPU)
      
      # Store original (unscaled) data for use in advanced analyses.
      prep_data$daily_ret_original <- daily_returns
      prep_data$monthly_epu_original <- monthly_epu
      prep_data$monthly_smp_original <- monthly_smp
      prep_data$sector_name <- sector
      
      prep_data
      
    }, error = function(e) {
      cat("  ✗ Error during data preparation:", e$message, "\n")
      return(NULL)
    })
    
    if (is.null(model_data_container)) {
      next
    }
    
    # --- Full-Sample Model Estimation ---
    cat("  ▶ Estimating GARCH-MIDAS model...\n")
    model_result <- fit_garch_midas(
      model_data = model_data_container,
      K_epu = K_EPU,
      distribution = DISTRIBUTION,
      skew = SKEW,
      allow_bell_shape = ALLOW_BELL_SHAPE,
      verbose = TRUE
    )
    
    if (!is.null(model_result)) {
      results_container$models[[sector]] <- model_result
      
      # Generate a detailed text report of the model summary.
      cat("  ▶ Generating analysis report...\n")
      report_file <- file.path(OUTPUT_DIR, "reports", 
                               sprintf("report_%s.txt", sector_clean))
      
      sink(report_file) # Redirect console output to the file.
      cat(paste0(rep("━", 80), collapse=""), "\n")
      cat(sprintf("GARCH-MIDAS Analysis Report: %s\n", sector))
      cat(paste0(rep("━", 80), collapse=""), "\n\n")
      
      try(summary(model_result))
      
      sink() # Restore console output.
      
      # Generate and save all associated visualizations.
      cat("  ▶ Generating visualizations...\n")
      plot(model_result,
           main_title = sprintf("GARCH-MIDAS Analysis: %s", sector),
           save_plots = TRUE,
           format = FIGURE_FORMAT,
           dpi = DPI)
      
    } else {
      cat("  ✗ Model estimation failed.\n")
    }
    
    # --- Advanced Analyses (if enabled) ---
    # Robustness checks
    if(RUN_ROBUSTNESS_ANALYSIS && !is.null(model_data_container)) {
      cat("  ▶ Running robustness analysis...\n")
      robust_results <- robustness_analysis(
        model_data = model_data_container,
        K_values = K_VALUES_SENSITIVITY,
        distributions = c("norm", "std"),
        skew_options = c("YES", "NO"),
        verbose = FALSE
      )
      results_container$robustness[[sector]] <- robust_results
    }
    
    # Rolling window analysis
    if(RUN_ROLLING_ANALYSIS && !is.null(model_data_container)) {
      cat("  ▶ Running rolling window analysis...\n")
      rolling_results <- rolling_window_analysis(
        daily_prices = loaded_data$daily_prices[, sector_clean],
        monthly_epu = loaded_data$monthly_data[, make.names(EPU_NAME)],
        monthly_smp = loaded_data$monthly_data[, make.names(SMP_NAME)],
        sector_name = sector,
        window_size = ROLLING_WINDOW_SIZE,
        step_size = ROLLING_STEP_SIZE,
        K_epu = K_EPU,
        distribution = DISTRIBUTION,
        skew = SKEW
      )
      results_container$rolling[[sector]] <- rolling_results
      
      # Plot the rolling analysis results.
      plot_rolling_analysis(rolling_results, sector, OUTPUT_DIR)
    }
    
    # Log performance for this sector.
    sector_time <- difftime(Sys.time(), sector_start_time, units = "secs")
    performance_log <- rbind(performance_log, data.frame(
      Sector = sector,
      Time_seconds = as.numeric(sector_time),
      Success = !is.null(model_result)
    ))
    
    cat(sprintf("  ✓ %s complete (Time taken: %.1f seconds)\n", sector, sector_time))
  }
  
  # --- Step 3: Generate Summary Reports ---
  cat("\n【 Step 3 】Generate Summary Reports and Tables\n")
  cat("────────────────────────────────────────────────────────────────\n")
  
  if (length(results_container$models) > 0) {
    # Generate a LaTeX table comparing results across all sectors.
    cat("  ▶ Generating summary LaTeX table...\n")
    generate_latex_table(
      results_container$models,
      caption = "GARCH-MIDAS Model Estimation Results Across Sectors",
      label = "tab:garch_midas_main_comparison"
    )
    
    # Create a summary CSV file for easy comparison.
    cat("  ▶ Generating model comparison CSV...\n")
    comparison_df <- do.call(rbind, lapply(names(results_container$models), function(sector) {
      model <- results_container$models[[sector]]
      if (is.null(model)) return(NULL)
      
      summary_obj <- summary(model)
      params <- coef(model$model_fit)
      
      data.frame(
        Sector = sector,
        N = summary_obj$n_obs,
        LogLik = summary_obj$loglik,
        AIC = summary_obj$aic,
        BIC = summary_obj$bic,
        Persistence = summary_obj$persistence,
        Alpha = params["alpha"],
        Beta = params["beta"],
        Gamma = ifelse("gamma" %in% names(params), params["gamma"], NA),
        Theta1_EPU = params["theta1"],
        Theta2_SMP = params["theta2"],
        stringsAsFactors = FALSE
      )
    }))
    
    # Save the comparison CSV.
    comp_path <- file.path(OUTPUT_DIR, "tables", "model_comparison_across_sectors.csv")
    write.csv(comparison_df, comp_path, row.names = FALSE)
    cat("  Saved model comparison CSV to:", comp_path, "\n")
    
  }
  
  # --- Final Messages ---
  cat("\n")
  cat("✓ Analysis workflow complete!\n")
  cat(sprintf("✓ Total execution time: %.2f minutes\n", 
              difftime(Sys.time(), start_time, units = "mins")))
  cat(sprintf("✓ All results have been saved to: %s\n", OUTPUT_DIR))
  cat("\n")
  
  # Return the main container with all results.
  invisible(results_container)
}


# ==============================================================================
# Script Execution
# ==============================================================================
# This block provides guidance when the script is loaded interactively.
if (interactive()) {
  cat("\n")
  cat(paste0(rep("━", 70), collapse=""), "\n")
  cat("  GARCH-MIDAS analysis script loaded (v41.0).\n")
  cat("  This version includes extensive visualization and a modular workflow.\n")
  cat(paste0(rep("━", 70), collapse=""), "\n")
  cat("\n")
  cat("  Instructions:\n")
  cat("  1. Review and modify the 'User Configuration Area' at the top.\n")
  cat("  2. Note that `RUN_ROLLING_ANALYSIS` is very time-consuming.\n")
  cat("\n")
  cat("  To run the full analysis, execute:\n")
  cat("    results <- main_workflow_enhanced()\n")
  cat("\n")
}
