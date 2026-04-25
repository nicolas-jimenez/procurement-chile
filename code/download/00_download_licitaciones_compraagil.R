#!/usr/bin/env Rscript

# Download ChileCompra historical raw monthly files for licitaciones and Compra Agil.
#
# Data sources are the bulk-download ZIPs documented by ChileCompra:
#   - Licitaciones: https://transparenciachc.blob.core.windows.net/lic-da/YYYY-M.zip
#   - Compra Agil:  https://transparenciachc.blob.core.windows.net/trnspchc/COT_YYYY-MM.zip
#
# The script writes into PROCUREMENT_CHILE_DB/data/raw/chilecompra by default.
# Full runs should happen on Bouchet. Non-Bouchet runs are capped at 500 source
# ZIP files to avoid accidentally pulling the archive onto a laptop.

suppressPackageStartupMessages({
  library(httr)
})


# -- Paths --------------------------------------------------------------------
read_env_file <- function(env_path) {
  out <- list()
  if (!file.exists(env_path)) {
    return(out)
  }

  lines <- readLines(env_path, warn = FALSE, encoding = "UTF-8")
  for (line in lines) {
    line <- trimws(line)
    if (!nzchar(line) || startsWith(line, "#") || !grepl("=", line, fixed = TRUE)) {
      next
    }
    parts <- strsplit(line, "=", fixed = TRUE)[[1]]
    key <- trimws(parts[1])
    value <- trimws(paste(parts[-1], collapse = "="))
    value <- gsub("^['\"]|['\"]$", "", value)
    out[[key]] <- value
  }
  out
}

find_repo_root <- function(start_path) {
  current <- normalizePath(start_path, winslash = "/", mustWork = FALSE)

  repeat {
    has_git <- file.exists(file.path(current, ".git"))
    has_env_example <- file.exists(file.path(current, ".env.example"))
    has_code_dir <- dir.exists(file.path(current, "code"))

    if ((has_git || has_env_example) && has_code_dir) {
      return(current)
    }

    parent <- dirname(current)
    if (identical(parent, current)) {
      stop("Could not locate repo root from: ", start_path, call. = FALSE)
    }
    current <- parent
  }
}

script_dir <- local({
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg)) {
    dirname(normalizePath(sub("^--file=", "", file_arg[1]), winslash = "/", mustWork = FALSE))
  } else {
    getwd()
  }
})

REPO_ROOT <- find_repo_root(script_dir)
ENV_ENTRIES <- read_env_file(file.path(REPO_ROOT, ".env"))

resolve_data_root <- function() {
  from_env <- Sys.getenv("PROCUREMENT_CHILE_DB", unset = "")
  if (nzchar(from_env)) {
    return(normalizePath(from_env, winslash = "/", mustWork = FALSE))
  }
  if (!is.null(ENV_ENTRIES$PROCUREMENT_CHILE_DB) && nzchar(ENV_ENTRIES$PROCUREMENT_CHILE_DB)) {
    return(normalizePath(ENV_ENTRIES$PROCUREMENT_CHILE_DB, winslash = "/", mustWork = FALSE))
  }
  REPO_ROOT
}

DEFAULT_OUTPUT_ROOT <- file.path(resolve_data_root(), "data", "raw", "chilecompra")
LOCAL_RUN_DOWNLOAD_CAP <- 500L

is_bouchet_run <- function(output_root) {
  project_root <- normalizePath(Sys.getenv("PROJECT_ROOT", unset = ""), winslash = "/", mustWork = FALSE)
  output_root_norm <- normalizePath(output_root, winslash = "/", mustWork = FALSE)
  repo_root_norm <- normalizePath(REPO_ROOT, winslash = "/", mustWork = FALSE)

  any(startsWith(
    c(project_root, output_root_norm, repo_root_norm),
    "/nfs/roberts/project/pi_rp269/nj229/procurement-chile"
  ))
}


# -- CLI ----------------------------------------------------------------------
print_help <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript code/download/00_download_licitaciones_compraagil.R [options]",
      "",
      "Options:",
      "  --datasets <value>                 all | licitaciones | compra_agil. Default: all.",
      "  --licitaciones-start-month <YYYY-MM> Default: 2007-01.",
      "  --licitaciones-end-month <YYYY-MM>   Default: current month.",
      "  --compra-agil-start-month <YYYY-MM>  Default: 2020-04.",
      "  --compra-agil-end-month <YYYY-MM>    Default: current month.",
      "  --output-root <path>                Default: PROCUREMENT_CHILE_DB/data/raw/chilecompra.",
      "  --sleep-seconds <value>             Pause between months. Default: 0.25.",
      "  --timeout-seconds <value>           Request timeout. Default: 600.",
      "  --max-months <value>                Optional cap for testing.",
      "                                      Non-Bouchet runs are also capped at 500 source ZIPs.",
      "  --keep-archives                     Keep source ZIP files under archives/.",
      "  --no-extract                        Download ZIP files but do not unzip.",
      "  --overwrite-archives                Re-download ZIPs even if present.",
      "  --overwrite-extracted               Re-extract months even if CSVs already exist.",
      "  --dry-run                           Print planned months without downloading.",
      "  --help                              Print this message.",
      "",
      "Output layout:",
      "  <output-root>/licitaciones/YYYY_M/lic_YYYY-M.csv",
      "  <output-root>/compra_agil/COT_YYYY-MM/COT*.csv",
      "  <output-root>/archives/<dataset>/*.zip          when --keep-archives is used",
      "  <output-root>/manifests/raw_bulk_downloads.csv",
      sep = "\n"
    )
  )
}

parse_month <- function(value, option_name) {
  if (!grepl("^\\d{4}-\\d{1,2}$", value)) {
    stop(option_name, " must have format YYYY-MM.", call. = FALSE)
  }
  parts <- strsplit(value, "-", fixed = TRUE)[[1]]
  year <- as.integer(parts[[1]])
  month <- as.integer(parts[[2]])
  if (is.na(year) || is.na(month) || month < 1 || month > 12) {
    stop(option_name, " must have a valid month.", call. = FALSE)
  }
  as.Date(sprintf("%04d-%02d-01", year, month))
}

format_month <- function(date_value) {
  format(date_value, "%Y-%m")
}

parse_args <- function(args) {
  current_month <- as.Date(format(Sys.Date(), "%Y-%m-01"))
  opts <- list(
    datasets = "all",
    licitaciones_start_month = as.Date("2007-01-01"),
    licitaciones_end_month = current_month,
    compra_agil_start_month = as.Date("2020-04-01"),
    compra_agil_end_month = current_month,
    output_root = DEFAULT_OUTPUT_ROOT,
    sleep_seconds = 0.25,
    timeout_seconds = 600,
    max_months = Inf,
    keep_archives = FALSE,
    no_extract = FALSE,
    overwrite_archives = FALSE,
    overwrite_extracted = FALSE,
    dry_run = FALSE
  )

  i <- 1
  while (i <= length(args)) {
    arg <- args[[i]]

    if (identical(arg, "--help")) {
      print_help()
      quit(save = "no", status = 0)
    } else if (identical(arg, "--keep-archives")) {
      opts$keep_archives <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--no-extract")) {
      opts$no_extract <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--overwrite-archives")) {
      opts$overwrite_archives <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--overwrite-extracted")) {
      opts$overwrite_extracted <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--dry-run")) {
      opts$dry_run <- TRUE
      i <- i + 1
      next
    }

    if (i == length(args)) {
      stop("Missing value for option: ", arg, call. = FALSE)
    }

    value <- args[[i + 1]]
    if (identical(arg, "--datasets")) {
      opts$datasets <- value
    } else if (identical(arg, "--licitaciones-start-month")) {
      opts$licitaciones_start_month <- parse_month(value, arg)
    } else if (identical(arg, "--licitaciones-end-month")) {
      opts$licitaciones_end_month <- parse_month(value, arg)
    } else if (identical(arg, "--compra-agil-start-month")) {
      opts$compra_agil_start_month <- parse_month(value, arg)
    } else if (identical(arg, "--compra-agil-end-month")) {
      opts$compra_agil_end_month <- parse_month(value, arg)
    } else if (identical(arg, "--output-root")) {
      opts$output_root <- value
    } else if (identical(arg, "--sleep-seconds")) {
      opts$sleep_seconds <- as.numeric(value)
    } else if (identical(arg, "--timeout-seconds")) {
      opts$timeout_seconds <- as.numeric(value)
    } else if (identical(arg, "--max-months")) {
      opts$max_months <- as.numeric(value)
    } else {
      stop("Unknown option: ", arg, call. = FALSE)
    }
    i <- i + 2
  }

  dataset_norm <- gsub("-", "_", tolower(opts$datasets))
  if (!dataset_norm %in% c("all", "licitaciones", "compra_agil")) {
    stop("--datasets must be one of: all, licitaciones, compra_agil.", call. = FALSE)
  }
  opts$datasets <- dataset_norm

  if (opts$licitaciones_start_month > opts$licitaciones_end_month) {
    stop("Licitaciones start month must be <= end month.", call. = FALSE)
  }
  if (opts$compra_agil_start_month > opts$compra_agil_end_month) {
    stop("Compra Agil start month must be <= end month.", call. = FALSE)
  }
  if (!is.finite(opts$sleep_seconds) || opts$sleep_seconds < 0) {
    stop("--sleep-seconds must be non-negative.", call. = FALSE)
  }
  if (!is.finite(opts$timeout_seconds) || opts$timeout_seconds <= 0) {
    stop("--timeout-seconds must be positive.", call. = FALSE)
  }
  if (!is.finite(opts$max_months) || opts$max_months <= 0) {
    opts$max_months <- Inf
  }

  opts$output_root <- normalizePath(opts$output_root, winslash = "/", mustWork = FALSE)
  opts
}


# -- Manifests ----------------------------------------------------------------
read_manifest <- function(path) {
  if (!file.exists(path)) {
    return(data.frame(stringsAsFactors = FALSE))
  }
  tryCatch(
    read.csv(path, stringsAsFactors = FALSE, na.strings = c("", "NA")),
    error = function(e) data.frame(stringsAsFactors = FALSE)
  )
}

dedupe_rows <- function(df, key_cols, keep_last = FALSE) {
  if (!nrow(df) || !length(key_cols)) {
    return(df)
  }
  keep <- !duplicated(df[key_cols], fromLast = keep_last)
  df[keep, , drop = FALSE]
}

upsert_manifest <- function(path, new_df, key_cols) {
  old_df <- read_manifest(path)
  if (!nrow(old_df)) {
    out <- new_df
  } else if (!nrow(new_df)) {
    out <- old_df
  } else {
    all_cols <- union(names(old_df), names(new_df))
    for (col in setdiff(all_cols, names(old_df))) old_df[[col]] <- NA
    for (col in setdiff(all_cols, names(new_df))) new_df[[col]] <- NA
    out <- rbind(old_df[all_cols], new_df[all_cols])
    out <- dedupe_rows(out, key_cols, keep_last = TRUE)
  }
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  write.csv(out, path, row.names = FALSE, na = "")
  out
}


# -- Download/extract helpers -------------------------------------------------
USER_AGENT <- paste(
  "procurement-chile-local/raw-bulk-downloader",
  "(research script; contact: datosabiertos@chilecompra.cl if needed)"
)

month_sequence <- function(start_month, end_month) {
  seq.Date(start_month, end_month, by = "month")
}

existing_csvs <- function(extract_dir) {
  if (!dir.exists(extract_dir)) {
    return(character())
  }
  list.files(extract_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
}

remote_head <- function(url, timeout_seconds) {
  resp <- RETRY(
    verb = "HEAD",
    url = url,
    times = 3,
    pause_base = 1,
    pause_cap = 8,
    terminate_on = c(400, 401, 403, 404),
    user_agent(USER_AGENT),
    timeout(timeout_seconds)
  )
  hdr <- headers(resp)
  list(
    status_code = status_code(resp),
    content_length = suppressWarnings(as.numeric(hdr[["content-length"]])),
    last_modified = ifelse(is.null(hdr[["last-modified"]]), NA_character_, hdr[["last-modified"]])
  )
}

download_zip <- function(url, archive_path, opts) {
  head <- remote_head(url, opts$timeout_seconds)
  if (head$status_code != 200L) {
    return(list(
      status = "missing_remote",
      http_status = head$status_code,
      content_length = head$content_length,
      last_modified = head$last_modified,
      downloaded = FALSE
    ))
  }

  if (
    file.exists(archive_path)
      && !isTRUE(opts$overwrite_archives)
      && is.finite(head$content_length)
      && file.info(archive_path)$size == head$content_length
  ) {
    return(list(
      status = "archive_exists",
      http_status = head$status_code,
      content_length = head$content_length,
      last_modified = head$last_modified,
      downloaded = FALSE
    ))
  }

  dir.create(dirname(archive_path), recursive = TRUE, showWarnings = FALSE)
  tmp_path <- paste0(archive_path, ".tmp")
  if (file.exists(tmp_path)) {
    unlink(tmp_path)
  }

  resp <- RETRY(
    verb = "GET",
    url = url,
    write_disk(tmp_path, overwrite = TRUE),
    times = 3,
    pause_base = 2,
    pause_cap = 30,
    terminate_on = c(400, 401, 403, 404),
    user_agent(USER_AGENT),
    timeout(opts$timeout_seconds)
  )

  status <- status_code(resp)
  if (status != 200L) {
    if (file.exists(tmp_path)) {
      unlink(tmp_path)
    }
    return(list(
      status = "download_error",
      http_status = status,
      content_length = head$content_length,
      last_modified = head$last_modified,
      downloaded = FALSE
    ))
  }

  file.rename(tmp_path, archive_path)
  list(
    status = "downloaded",
    http_status = status,
    content_length = head$content_length,
    last_modified = head$last_modified,
    downloaded = TRUE
  )
}

extract_zip <- function(archive_path, extract_dir, opts) {
  csvs <- existing_csvs(extract_dir)
  if (length(csvs) && !isTRUE(opts$overwrite_extracted)) {
    return(list(status = "extracted_exists", extracted = FALSE, n_csv_files = length(csvs)))
  }

  if (!file.exists(archive_path)) {
    return(list(status = "archive_missing", extracted = FALSE, n_csv_files = 0L))
  }

  parent <- dirname(extract_dir)
  dir.create(parent, recursive = TRUE, showWarnings = FALSE)
  tmp_dir <- tempfile(pattern = paste0(basename(extract_dir), "_"), tmpdir = parent)
  dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)

  ans <- tryCatch(
    {
      utils::unzip(archive_path, exdir = tmp_dir)
      TRUE
    },
    error = function(e) {
      message("Extraction failed for ", archive_path, ": ", conditionMessage(e))
      FALSE
    }
  )

  if (!ans) {
    unlink(tmp_dir, recursive = TRUE, force = TRUE)
    return(list(status = "extract_error", extracted = FALSE, n_csv_files = 0L))
  }

  if (dir.exists(extract_dir)) {
    unlink(extract_dir, recursive = TRUE, force = TRUE)
  }
  ok <- file.rename(tmp_dir, extract_dir)
  if (!ok) {
    unlink(tmp_dir, recursive = TRUE, force = TRUE)
    return(list(status = "extract_error", extracted = FALSE, n_csv_files = 0L))
  }

  csvs <- existing_csvs(extract_dir)
  list(status = "extracted", extracted = TRUE, n_csv_files = length(csvs))
}

month_spec <- function(dataset, month_date, output_root) {
  year <- as.integer(format(month_date, "%Y"))
  month <- as.integer(format(month_date, "%m"))

  if (identical(dataset, "licitaciones")) {
    month_key <- sprintf("%04d_%d", year, month)
    archive_name <- sprintf("lic_%04d-%d.zip", year, month)
    list(
      dataset = dataset,
      year = year,
      month = month,
      month_key = month_key,
      url = sprintf("https://transparenciachc.blob.core.windows.net/lic-da/%04d-%d.zip", year, month),
      archive_path = file.path(output_root, "archives", "licitaciones", archive_name),
      extract_dir = file.path(output_root, "licitaciones", month_key)
    )
  } else if (identical(dataset, "compra_agil")) {
    month_key <- sprintf("COT_%04d-%02d", year, month)
    archive_name <- paste0(month_key, ".zip")
    list(
      dataset = dataset,
      year = year,
      month = month,
      month_key = month_key,
      url = sprintf("https://transparenciachc.blob.core.windows.net/trnspchc/COT_%04d-%02d.zip", year, month),
      archive_path = file.path(output_root, "archives", "compra_agil", archive_name),
      extract_dir = file.path(output_root, "compra_agil", month_key)
    )
  } else {
    stop("Unsupported dataset: ", dataset, call. = FALSE)
  }
}

process_month <- function(spec, opts, manifest_path) {
  started_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")

  csvs_before <- existing_csvs(spec$extract_dir)
  if (
    length(csvs_before)
      && !isTRUE(opts$overwrite_extracted)
      && !isTRUE(opts$no_extract)
      && !isTRUE(opts$overwrite_archives)
  ) {
    row <- data.frame(
      dataset = spec$dataset,
      year = spec$year,
      month = spec$month,
      month_key = spec$month_key,
      source_url = spec$url,
      status = "extracted_exists",
      http_status = NA_integer_,
      content_length = NA_real_,
      last_modified = NA_character_,
      archive_path = ifelse(opts$keep_archives, spec$archive_path, NA_character_),
      extract_dir = spec$extract_dir,
      downloaded = FALSE,
      extracted = FALSE,
      n_csv_files = length(csvs_before),
      started_at = started_at,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
      stringsAsFactors = FALSE
    )
    upsert_manifest(manifest_path, row, c("dataset", "year", "month"))
    return(row)
  }

  dl <- download_zip(spec$url, spec$archive_path, opts)
  if (!dl$status %in% c("downloaded", "archive_exists")) {
    row <- data.frame(
      dataset = spec$dataset,
      year = spec$year,
      month = spec$month,
      month_key = spec$month_key,
      source_url = spec$url,
      status = dl$status,
      http_status = dl$http_status,
      content_length = dl$content_length,
      last_modified = dl$last_modified,
      archive_path = ifelse(opts$keep_archives, spec$archive_path, NA_character_),
      extract_dir = spec$extract_dir,
      downloaded = dl$downloaded,
      extracted = FALSE,
      n_csv_files = 0L,
      started_at = started_at,
      finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
      stringsAsFactors = FALSE
    )
    upsert_manifest(manifest_path, row, c("dataset", "year", "month"))
    return(row)
  }

  if (isTRUE(opts$no_extract)) {
    extract_status <- list(status = "not_extracted", extracted = FALSE, n_csv_files = 0L)
  } else {
    extract_status <- extract_zip(spec$archive_path, spec$extract_dir, opts)
  }

  if (!isTRUE(opts$keep_archives) && file.exists(spec$archive_path)) {
    unlink(spec$archive_path)
  }

  final_status <- if (identical(extract_status$status, "extracted")) {
    "ok"
  } else if (identical(extract_status$status, "extracted_exists")) {
    "ok_cached"
  } else if (identical(extract_status$status, "not_extracted")) {
    "downloaded_not_extracted"
  } else {
    extract_status$status
  }

  row <- data.frame(
    dataset = spec$dataset,
    year = spec$year,
    month = spec$month,
    month_key = spec$month_key,
    source_url = spec$url,
    status = final_status,
    http_status = dl$http_status,
    content_length = dl$content_length,
    last_modified = dl$last_modified,
    archive_path = ifelse(opts$keep_archives, spec$archive_path, NA_character_),
    extract_dir = spec$extract_dir,
    downloaded = dl$downloaded,
    extracted = extract_status$extracted,
    n_csv_files = extract_status$n_csv_files,
    started_at = started_at,
    finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    stringsAsFactors = FALSE
  )
  upsert_manifest(manifest_path, row, c("dataset", "year", "month"))
  row
}


# -- Main ---------------------------------------------------------------------
main <- function() {
  opts <- parse_args(commandArgs(trailingOnly = TRUE))

  dirs <- list(
    root = opts$output_root,
    manifests = file.path(opts$output_root, "manifests")
  )
  for (d in dirs) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }
  manifest_path <- file.path(dirs$manifests, "raw_bulk_downloads.csv")

  datasets <- if (identical(opts$datasets, "all")) {
    c("licitaciones", "compra_agil")
  } else {
    opts$datasets
  }

  specs <- list()
  if ("licitaciones" %in% datasets) {
    months <- month_sequence(opts$licitaciones_start_month, opts$licitaciones_end_month)
    specs <- c(specs, lapply(months, month_spec, dataset = "licitaciones", output_root = dirs$root))
  }
  if ("compra_agil" %in% datasets) {
    months <- month_sequence(opts$compra_agil_start_month, opts$compra_agil_end_month)
    specs <- c(specs, lapply(months, month_spec, dataset = "compra_agil", output_root = dirs$root))
  }
  if (is.finite(opts$max_months)) {
    specs <- head(specs, opts$max_months)
  }

  on_bouchet <- is_bouchet_run(dirs$root)
  if (!on_bouchet) {
    cat(
      "NOTICE: This does not look like a Bouchet/Yale-server run.\n",
      "        Full raw ChileCompra downloads should be run on Bouchet.\n",
      "        This local/non-Bouchet run is capped at ",
      LOCAL_RUN_DOWNLOAD_CAP,
      " source ZIP files.\n\n",
      sep = ""
    )
    if (length(specs) > LOCAL_RUN_DOWNLOAD_CAP) {
      specs <- head(specs, LOCAL_RUN_DOWNLOAD_CAP)
    }
  }

  cat("Repo root             :", REPO_ROOT, "\n")
  cat("Output root           :", dirs$root, "\n")
  cat("Bouchet run           :", on_bouchet, "\n")
  cat("Datasets              :", paste(datasets, collapse = ", "), "\n")
  cat("Licitaciones months   :", format_month(opts$licitaciones_start_month), "to", format_month(opts$licitaciones_end_month), "\n")
  cat("Compra Agil months    :", format_month(opts$compra_agil_start_month), "to", format_month(opts$compra_agil_end_month), "\n")
  cat("Months queued         :", length(specs), "\n")
  cat("Keep archives         :", opts$keep_archives, "\n")
  cat("Extract ZIPs          :", !opts$no_extract, "\n")
  cat("Manifest              :", manifest_path, "\n\n")

  if (isTRUE(opts$dry_run)) {
    preview <- do.call(rbind, lapply(specs, function(s) {
      data.frame(
        dataset = s$dataset,
        year = s$year,
        month = s$month,
        month_key = s$month_key,
        url = s$url,
        extract_dir = s$extract_dir,
        stringsAsFactors = FALSE
      )
    }))
    print(preview)
    cat("\nDry run complete.\n")
    return(invisible(preview))
  }

  rows <- vector("list", length(specs))
  for (i in seq_along(specs)) {
    spec <- specs[[i]]
    cat(sprintf(
      "[%d/%d] %s %04d-%02d\n",
      i, length(specs), spec$dataset, spec$year, spec$month
    ))
    rows[[i]] <- process_month(spec, opts, manifest_path)
    cat("  status:", rows[[i]]$status, "| csv files:", rows[[i]]$n_csv_files, "\n")
    Sys.sleep(opts$sleep_seconds)
  }

  out <- do.call(rbind, rows)
  cat("\nDownload summary\n")
  print(table(out$dataset, out$status, useNA = "ifany"))
  cat("\nDone.\n")
  invisible(out)
}

main()
