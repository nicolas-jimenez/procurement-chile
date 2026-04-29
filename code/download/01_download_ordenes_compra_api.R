#!/usr/bin/env Rscript

# Download Mercado Público purchase-order data via the official API.
#
# Why the script is two-stage:
#   1) The daily endpoint returns a basic listing of orders sent on a date.
#   2) The by-code endpoint returns the detailed order payload, including buyer,
#      supplier, monetary fields, and items.
#
# Official references used:
#   - "Documentación API de Mercado Público - Órdenes de Compra" (PDF)
#   - https://api.mercadopublico.cl/modules/ejemplo_10.aspx
#
# Usage examples:
#   Rscript code/download/01_download_ordenes_compra_api.R \
#     --start-date 2010-01-02 \
#     --end-date 2014-12-31
#
#   MERCADO_PUBLICO_TICKET=... \
#   Rscript code/download/01_download_ordenes_compra_api.R \
#     --start-date 2024-01-01 \
#     --end-date 2024-12-31 \
#     --codigo-organismo 6945

suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
})


# ── Helpers: paths / env ─────────────────────────────────────────────────────
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

resolve_dropbox_root <- function() {
  from_env <- Sys.getenv("PROCUREMENT_CHILE_DB", unset = "")
  if (nzchar(from_env)) {
    return(normalizePath(from_env, winslash = "/", mustWork = FALSE))
  }
  if (!is.null(ENV_ENTRIES$PROCUREMENT_CHILE_DB) && nzchar(ENV_ENTRIES$PROCUREMENT_CHILE_DB)) {
    return(normalizePath(ENV_ENTRIES$PROCUREMENT_CHILE_DB, winslash = "/", mustWork = FALSE))
  }
  REPO_ROOT
}

DEFAULT_DATA_ROOT <- file.path(
  resolve_dropbox_root(),
  "data", "raw", "chilecompra", "ordenes_compra"
)


# ── Helpers: CLI parsing ─────────────────────────────────────────────────────
print_help <- function() {
  cat(
    paste(
      "Usage:",
      "  Rscript code/download/01_download_ordenes_compra_api.R [options]",
      "",
      "Options:",
      "  --ticket <value>             API ticket. Defaults to MERCADO_PUBLICO_TICKET or",
      "                               CHILECOMPRA_API_TICKET.",
      "  --start-date <YYYY-MM-DD>    First issue date to query. Default: 2010-01-02",
      "                               (earliest nonempty date verified locally).",
      "  --end-date <YYYY-MM-DD>      Last issue date to query. Default: today.",
      "  --output-root <path>         Root folder for downloaded files.",
      "  --codigo-organismo <value>   Optional official buyer code filter.",
      "  --codigo-proveedor <value>   Optional official supplier code filter.",
      "  --sleep-seconds <value>      Pause between API calls. Default: 0.25.",
      "  --max-sleep-seconds <value>  Adaptive throttle ceiling. Default: 30.",
      "  --timeout-seconds <value>    Request timeout in seconds. Default: 60.",
      "  --max-retries <value>        Per-request retry cap for 429/5xx/network errors. Default: 8.",
      "  --retry-base-seconds <value> Initial retry wait when Retry-After is absent. Default: 2.",
      "  --retry-cap-seconds <value>  Maximum retry wait when Retry-After is absent. Default: 120.",
      "  --max-details <value>        Optional cap on detail downloads (for testing).",
      "  --manifest-flush-every <n>   Upsert detail_downloads.csv every n rows. Default: 1000.",
      "  --run-id <value>             Optional identifier recorded in trace manifests.",
      "  --reverse-details            Download detailed orders newest-to-oldest.",
      "  --no-adaptive-throttle       Disable adaptive sleep increases after 429 retries.",
      "  --daily-only                 Only build daily listings and order-code manifest.",
      "  --details-only               Skip daily discovery and use existing order-code manifest.",
      "  --overwrite-daily            Re-download daily listing files even if present.",
      "  --overwrite-detail           Re-download detailed order files even if present.",
      "  --help                       Print this message.",
      "",
      "Output layout:",
      "  <output-root>/daily_json/YYYY/ordenes_YYYY-MM-DD.json",
      "  <output-root>/daily_codes/YYYY/ordenes_YYYY-MM-DD.csv",
      "  <output-root>/detail_json/YYYY/MM/<codigo>.json",
      "  <output-root>/manifests/daily_batches.csv",
      "  <output-root>/manifests/order_codes.csv",
      "  <output-root>/manifests/detail_downloads.csv",
      "  <output-root>/manifests/detail_attempts.csv",
      "  <output-root>/manifests/detail_runs.csv",
      sep = "\n"
    )
  )
}

parse_args <- function(args) {
  ticket_default <- Sys.getenv("MERCADO_PUBLICO_TICKET", unset = "")
  if (!nzchar(ticket_default)) {
    ticket_default <- Sys.getenv("CHILECOMPRA_API_TICKET", unset = "")
  }

  opts <- list(
    ticket = ticket_default,
    start_date = "2010-01-02",
    end_date = as.character(Sys.Date()),
    output_root = DEFAULT_DATA_ROOT,
    codigo_organismo = NULL,
    codigo_proveedor = NULL,
    sleep_seconds = 0.25,
    max_sleep_seconds = 30,
    timeout_seconds = 60,
    max_retries = 8,
    retry_base_seconds = 2,
    retry_cap_seconds = 120,
    max_details = Inf,
    manifest_flush_every = 1000,
    run_id = "",
    reverse_details = FALSE,
    adaptive_throttle = TRUE,
    daily_only = FALSE,
    details_only = FALSE,
    overwrite_daily = FALSE,
    overwrite_detail = FALSE
  )

  i <- 1
  while (i <= length(args)) {
    arg <- args[[i]]

    if (identical(arg, "--help")) {
      print_help()
      quit(save = "no", status = 0)
    } else if (identical(arg, "--daily-only")) {
      opts$daily_only <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--details-only")) {
      opts$details_only <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--overwrite-daily")) {
      opts$overwrite_daily <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--overwrite-detail")) {
      opts$overwrite_detail <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--reverse-details")) {
      opts$reverse_details <- TRUE
      i <- i + 1
      next
    } else if (identical(arg, "--no-adaptive-throttle")) {
      opts$adaptive_throttle <- FALSE
      i <- i + 1
      next
    }

    if (i == length(args)) {
      stop("Missing value for option: ", arg, call. = FALSE)
    }

    value <- args[[i + 1]]
    if (identical(arg, "--ticket")) {
      opts$ticket <- value
    } else if (identical(arg, "--start-date")) {
      opts$start_date <- value
    } else if (identical(arg, "--end-date")) {
      opts$end_date <- value
    } else if (identical(arg, "--output-root")) {
      opts$output_root <- value
    } else if (identical(arg, "--codigo-organismo")) {
      opts$codigo_organismo <- value
    } else if (identical(arg, "--codigo-proveedor")) {
      opts$codigo_proveedor <- value
    } else if (identical(arg, "--sleep-seconds")) {
      opts$sleep_seconds <- as.numeric(value)
    } else if (identical(arg, "--max-sleep-seconds")) {
      opts$max_sleep_seconds <- as.numeric(value)
    } else if (identical(arg, "--timeout-seconds")) {
      opts$timeout_seconds <- as.numeric(value)
    } else if (identical(arg, "--max-retries")) {
      opts$max_retries <- as.integer(value)
    } else if (identical(arg, "--retry-base-seconds")) {
      opts$retry_base_seconds <- as.numeric(value)
    } else if (identical(arg, "--retry-cap-seconds")) {
      opts$retry_cap_seconds <- as.numeric(value)
    } else if (identical(arg, "--max-details")) {
      opts$max_details <- as.numeric(value)
    } else if (identical(arg, "--manifest-flush-every")) {
      opts$manifest_flush_every <- as.integer(value)
    } else if (identical(arg, "--run-id")) {
      opts$run_id <- value
    } else {
      stop("Unknown option: ", arg, call. = FALSE)
    }
    i <- i + 2
  }

  if (isTRUE(opts$daily_only) && isTRUE(opts$details_only)) {
    stop("Use at most one of --daily-only or --details-only.", call. = FALSE)
  }

  if (!nzchar(opts$ticket)) {
    stop(
      "No API ticket provided. Use --ticket or set MERCADO_PUBLICO_TICKET / CHILECOMPRA_API_TICKET.",
      call. = FALSE
    )
  }

  opts$start_date <- as.Date(opts$start_date)
  opts$end_date <- as.Date(opts$end_date)
  if (is.na(opts$start_date) || is.na(opts$end_date)) {
    stop("Invalid date. Use YYYY-MM-DD.", call. = FALSE)
  }
  if (opts$start_date > opts$end_date) {
    stop("--start-date must be <= --end-date.", call. = FALSE)
  }
  if (!is.finite(opts$sleep_seconds) || opts$sleep_seconds < 0) {
    stop("--sleep-seconds must be a non-negative number.", call. = FALSE)
  }
  if (!is.finite(opts$max_sleep_seconds) || opts$max_sleep_seconds < opts$sleep_seconds) {
    stop("--max-sleep-seconds must be >= --sleep-seconds.", call. = FALSE)
  }
  if (!is.finite(opts$timeout_seconds) || opts$timeout_seconds <= 0) {
    stop("--timeout-seconds must be a positive number.", call. = FALSE)
  }
  if (!is.finite(opts$max_retries) || opts$max_retries < 0) {
    stop("--max-retries must be a non-negative integer.", call. = FALSE)
  }
  if (!is.finite(opts$retry_base_seconds) || opts$retry_base_seconds <= 0) {
    stop("--retry-base-seconds must be a positive number.", call. = FALSE)
  }
  if (!is.finite(opts$retry_cap_seconds) || opts$retry_cap_seconds < opts$retry_base_seconds) {
    stop("--retry-cap-seconds must be >= --retry-base-seconds.", call. = FALSE)
  }
  if (!is.finite(opts$max_details) || opts$max_details <= 0) {
    opts$max_details <- Inf
  }
  if (!is.finite(opts$manifest_flush_every) || opts$manifest_flush_every < 0) {
    stop("--manifest-flush-every must be a non-negative integer.", call. = FALSE)
  }
  if (!nzchar(opts$run_id)) {
    opts$run_id <- paste0("oc_", format(Sys.time(), "%Y%m%dT%H%M%SZ", tz = "UTC"))
  }

  opts$output_root <- normalizePath(opts$output_root, winslash = "/", mustWork = FALSE)
  opts
}


# ── Helpers: JSON parsing / manifests ────────────────────────────────────────
safe_chr <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_character_)
  }
  as.character(x[[1]])
}

safe_num <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_real_)
  }
  as.numeric(x[[1]])
}

safe_int <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_integer_)
  }
  as.integer(x[[1]])
}

as_named_list <- function(x) {
  if (is.null(x)) {
    return(list())
  }
  if (is.list(x) && !is.null(names(x)) && length(names(x)) > 0) {
    return(x)
  }
  list()
}

normalize_records <- function(x) {
  if (is.null(x)) {
    return(list())
  }
  if (is.list(x) && length(x) == 0) {
    return(list())
  }
  if (is.list(x) && !is.null(names(x)) && length(names(x)) > 0) {
    return(list(x))
  }
  x
}

read_json_text <- function(path) {
  paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
}

write_text_file <- function(text, path) {
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  con <- file(path, open = "wb")
  on.exit(close(con), add = TRUE)
  writeBin(charToRaw(enc2utf8(text)), con)
}

read_manifest <- function(path) {
  if (!file.exists(path)) {
    return(data.frame(stringsAsFactors = FALSE))
  }
  out <- tryCatch(
    read.csv(path, stringsAsFactors = FALSE, na.strings = c("", "NA")),
    error = function(e) data.frame(stringsAsFactors = FALSE)
  )
  out
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
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  write.csv(out, path, row.names = FALSE, na = "")
  out
}

append_manifest_rows <- function(path, new_df) {
  if (!nrow(new_df)) {
    return(invisible(NULL))
  }
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  write_header <- !file.exists(path) || file.info(path)$size == 0
  write.table(
    new_df,
    file = path,
    append = !write_header,
    sep = ",",
    row.names = FALSE,
    col.names = write_header,
    na = "",
    qmethod = "double",
    fileEncoding = "UTF-8"
  )
  invisible(NULL)
}

rbind_fill <- function(rows) {
  rows <- Filter(function(x) !is.null(x) && nrow(x), rows)
  if (!length(rows)) {
    return(data.frame(stringsAsFactors = FALSE))
  }
  all_cols <- Reduce(union, lapply(rows, names))
  normalized <- lapply(rows, function(x) {
    for (col in setdiff(all_cols, names(x))) {
      x[[col]] <- NA
    }
    x[all_cols]
  })
  do.call(rbind, normalized)
}

safe_code_path <- function(code) {
  gsub("[^A-Za-z0-9._-]", "_", code)
}

now_utc <- function() {
  format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

detail_path_for <- function(code_row, dirs) {
  requested_date <- as.Date(code_row[["requested_date"]])
  file.path(
    dirs$detail_json,
    format(requested_date, "%Y"),
    format(requested_date, "%m"),
    paste0(safe_code_path(code_row[["codigo"]]), ".json")
  )
}


# ── API helpers ──────────────────────────────────────────────────────────────
API_BASE <- "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
USER_AGENT <- paste(
  "procurement-chile-local/ordenes-compra-downloader",
  "(research script; contact: api@chilecompra.cl if needed)"
)

parse_retry_after <- function(resp) {
  value <- headers(resp)[["retry-after"]]
  if (is.null(value) || !nzchar(value)) {
    return(NA_real_)
  }

  numeric_value <- suppressWarnings(as.numeric(value))
  if (is.finite(numeric_value)) {
    return(max(0, numeric_value))
  }

  date_value <- suppressWarnings(
    as.POSIXct(value, format = "%a, %d %b %Y %H:%M:%S", tz = "GMT")
  )
  if (!is.na(date_value)) {
    return(max(0, as.numeric(difftime(date_value, Sys.time(), units = "secs"))))
  }

  NA_real_
}

is_retryable_status <- function(status_code_value) {
  is.na(status_code_value) || status_code_value %in% c(408L, 409L, 425L, 429L, 500L, 502L, 503L, 504L)
}

safe_source_url <- function(query) {
  redacted_query <- query
  if ("ticket" %in% names(redacted_query)) {
    redacted_query$ticket <- "REDACTED"
  }
  modify_url(API_BASE, query = redacted_query)
}

request_json <- function(query, timeout_seconds, opts) {
  total_wait <- 0
  last_error <- NA_character_
  last_status <- NA_integer_
  last_text <- ""
  attempts <- opts$max_retries + 1L

  for (attempt in seq_len(attempts)) {
    resp <- tryCatch(
      GET(
        url = API_BASE,
        query = query,
        user_agent(USER_AGENT),
        timeout(timeout_seconds)
      ),
      error = function(e) e
    )

    if (inherits(resp, "error")) {
      last_status <- NA_integer_
      last_text <- ""
      last_error <- conditionMessage(resp)
      retryable <- TRUE
      retry_after <- NA_real_
    } else {
      last_status <- status_code(resp)
      last_text <- content(resp, as = "text", encoding = "UTF-8")
      last_error <- NA_character_
      retryable <- is_retryable_status(last_status)
      retry_after <- if (last_status == 429L) parse_retry_after(resp) else NA_real_
    }

    if (!retryable || last_status == 200L || attempt >= attempts) {
      return(list(
        status_code = last_status,
        text = last_text,
        attempt_count = attempt,
        retry_count = attempt - 1L,
        retry_wait_seconds = total_wait,
        last_error = last_error
      ))
    }

    fallback_wait <- min(
      opts$retry_cap_seconds,
      opts$retry_base_seconds * (2 ^ (attempt - 1L))
    )
    wait_seconds <- if (is.finite(retry_after)) retry_after else fallback_wait
    wait_seconds <- min(opts$retry_cap_seconds, max(opts$retry_base_seconds, wait_seconds))
    wait_seconds <- wait_seconds + runif(1, min = 0, max = min(1, wait_seconds * 0.1))

    status_label <- ifelse(is.na(last_status), "network_error", as.character(last_status))
    message(sprintf("Request failed [%s]. Retrying in %.1f seconds...", status_label, wait_seconds))
    Sys.sleep(wait_seconds)
    total_wait <- total_wait + wait_seconds
  }

  list(
    status_code = last_status,
    text = last_text,
    attempt_count = attempts,
    retry_count = attempts - 1L,
    retry_wait_seconds = total_wait,
    last_error = last_error
  )
}

parse_daily_listing <- function(json_text, requested_date, source_url) {
  parsed <- fromJSON(json_text, simplifyVector = FALSE)
  listado <- normalize_records(parsed$Listado)

  rows <- lapply(listado, function(rec) {
    rec <- as_named_list(rec)
    data.frame(
      requested_date = as.character(requested_date),
      codigo = safe_chr(rec$Codigo),
      nombre = safe_chr(rec$Nombre),
      codigo_estado = safe_int(rec$CodigoEstado),
      source_url = source_url,
      stringsAsFactors = FALSE
    )
  })

  if (!length(rows)) {
    rows_df <- data.frame(
      requested_date = character(),
      codigo = character(),
      nombre = character(),
      codigo_estado = integer(),
      source_url = character(),
      stringsAsFactors = FALSE
    )
  } else {
    rows_df <- do.call(rbind, rows)
  }

  summary_df <- data.frame(
    requested_date = as.character(requested_date),
    api_reported_count = safe_int(parsed$Cantidad),
    parsed_count = nrow(rows_df),
    response_created_at = safe_chr(parsed$FechaCreacion),
    api_version = safe_chr(parsed$Version),
    source_url = source_url,
    stringsAsFactors = FALSE
  )

  list(rows = rows_df, summary = summary_df)
}

parse_detail_summary <- function(json_text, requested_code, source_url, detail_path) {
  parsed <- fromJSON(json_text, simplifyVector = FALSE)
  listado <- normalize_records(parsed$Listado)

  if (!length(listado)) {
    return(data.frame(
      codigo = requested_code,
      status = "empty",
      http_status = 200L,
      api_reported_count = safe_int(parsed$Cantidad),
      response_created_at = safe_chr(parsed$FechaCreacion),
      api_version = safe_chr(parsed$Version),
      detail_path = detail_path,
      source_url = source_url,
      stringsAsFactors = FALSE
    ))
  }

  rec <- as_named_list(listado[[1]])
  fechas <- as_named_list(rec$Fechas)
  comprador <- as_named_list(rec$Comprador)
  proveedor <- as_named_list(rec$Proveedor)
  items <- as_named_list(rec$Items)
  item_list <- normalize_records(items$Listado)

  data.frame(
    codigo = safe_chr(rec$Codigo),
    requested_code = requested_code,
    status = "ok",
    http_status = 200L,
    api_reported_count = safe_int(parsed$Cantidad),
    response_created_at = safe_chr(parsed$FechaCreacion),
    api_version = safe_chr(parsed$Version),
    nombre = safe_chr(rec$Nombre),
    codigo_estado = safe_int(rec$CodigoEstado),
    estado = safe_chr(rec$Estado),
    codigo_licitacion = safe_chr(rec$CodigoLicitacion),
    codigo_tipo = safe_chr(rec$CodigoTipo),
    tipo = safe_chr(rec$Tipo),
    tipo_moneda = safe_chr(rec$TipoMoneda),
    codigo_estado_proveedor = safe_int(rec$CodigoEstadoProveedor),
    estado_proveedor = safe_chr(rec$EstadoProveedor),
    fecha_creacion_oc = safe_chr(fechas$FechaCreacion),
    fecha_envio_oc = safe_chr(fechas$FechaEnvio),
    fecha_aceptacion_oc = safe_chr(fechas$FechaAceptacion),
    fecha_cancelacion_oc = safe_chr(fechas$FechaCancelacion),
    fecha_ultima_modificacion_oc = safe_chr(fechas$FechaUltimaModificacion),
    total_neto = safe_num(rec$TotalNeto),
    porcentaje_iva = safe_num(rec$PorcentajeIva),
    impuestos = safe_num(rec$Impuestos),
    total = safe_num(rec$Total),
    descuentos = safe_num(rec$Descuentos),
    cargos = safe_num(rec$Cargos),
    financiamiento = safe_chr(rec$Financiamiento),
    pais = safe_chr(rec$Pais),
    tipo_despacho = safe_chr(rec$TipoDespacho),
    forma_pago = safe_chr(rec$FormaPago),
    comprador_codigo_organismo = safe_chr(comprador$CodigoOrganismo),
    comprador_nombre_organismo = safe_chr(comprador$NombreOrganismo),
    comprador_rut_unidad = safe_chr(comprador$RutUnidad),
    comprador_codigo_unidad = safe_chr(comprador$CodigoUnidad),
    comprador_nombre_unidad = safe_chr(comprador$NombreUnidad),
    comprador_comuna = safe_chr(comprador$ComunaUnidad),
    comprador_region = safe_chr(comprador$RegionUnidad),
    proveedor_codigo = safe_chr(proveedor$Codigo),
    proveedor_nombre = safe_chr(proveedor$Nombre),
    proveedor_codigo_sucursal = safe_chr(proveedor$CodigoSucursal),
    proveedor_nombre_sucursal = safe_chr(proveedor$NombreSucursal),
    proveedor_rut_sucursal = safe_chr(proveedor$RutSucursal),
    proveedor_comuna = safe_chr(proveedor$Comuna),
    proveedor_region = safe_chr(proveedor$Region),
    n_items_reported = safe_int(items$Cantidad),
    n_items_parsed = length(item_list),
    detail_path = detail_path,
    source_url = source_url,
    stringsAsFactors = FALSE
  )
}


# ── Download logic ───────────────────────────────────────────────────────────
download_daily_batch <- function(date_value, opts, dirs) {
  date_iso <- as.character(date_value)
  date_ddmmyyyy <- format(date_value, "%d%m%Y")
  year_str <- format(date_value, "%Y")

  daily_json_path <- file.path(dirs$daily_json, year_str, paste0("ordenes_", date_iso, ".json"))
  daily_csv_path <- file.path(dirs$daily_codes, year_str, paste0("ordenes_", date_iso, ".csv"))

  query <- list(
    fecha = date_ddmmyyyy,
    ticket = opts$ticket
  )
  if (!is.null(opts$codigo_organismo) && nzchar(opts$codigo_organismo)) {
    query$CodigoOrganismo <- opts$codigo_organismo
  }
  if (!is.null(opts$codigo_proveedor) && nzchar(opts$codigo_proveedor)) {
    query$CodigoProveedor <- opts$codigo_proveedor
  }
  source_url <- safe_source_url(query)

  from_cache <- file.exists(daily_json_path) && !isTRUE(opts$overwrite_daily)
  if (from_cache) {
    json_text <- read_json_text(daily_json_path)
    http_status <- 200L
  } else {
    ans <- request_json(query = query, timeout_seconds = opts$timeout_seconds, opts = opts)
    http_status <- ans$status_code
    json_text <- ans$text
    if (isTRUE(http_status == 200L)) {
      write_text_file(json_text, daily_json_path)
    }
  }

  if (!isTRUE(http_status == 200L)) {
    return(list(
      rows = data.frame(stringsAsFactors = FALSE),
      summary = data.frame(
        requested_date = date_iso,
        api_reported_count = NA_integer_,
        parsed_count = 0L,
        response_created_at = NA_character_,
        api_version = NA_character_,
        source_url = source_url,
        http_status = http_status,
        file_path = daily_json_path,
        cached = from_cache,
        status = "http_error",
        stringsAsFactors = FALSE
      )
    ))
  }

  parsed <- tryCatch(
    parse_daily_listing(json_text, requested_date = date_value, source_url = source_url),
    error = function(e) {
      list(error = conditionMessage(e))
    }
  )

  if (!is.null(parsed$error)) {
    return(list(
      rows = data.frame(stringsAsFactors = FALSE),
      summary = data.frame(
        requested_date = date_iso,
        api_reported_count = NA_integer_,
        parsed_count = 0L,
        response_created_at = NA_character_,
        api_version = NA_character_,
        source_url = source_url,
        http_status = http_status,
        file_path = daily_json_path,
        cached = from_cache,
        status = paste0("parse_error: ", parsed$error),
        stringsAsFactors = FALSE
      )
    ))
  }

  rows <- parsed$rows
  if (nrow(rows)) {
    dir.create(dirname(daily_csv_path), showWarnings = FALSE, recursive = TRUE)
    write.csv(rows, daily_csv_path, row.names = FALSE, na = "")
  }

  summary <- parsed$summary
  summary$http_status <- http_status
  summary$file_path <- daily_json_path
  summary$cached <- from_cache
  summary$status <- "ok"

  list(rows = rows, summary = summary)
}

download_order_detail <- function(code_row, opts, dirs) {
  code <- code_row[["codigo"]]
  requested_date <- as.Date(code_row[["requested_date"]])
  detail_path <- detail_path_for(code_row, dirs)

  query <- list(
    codigo = code,
    ticket = opts$ticket
  )
  source_url <- safe_source_url(query)

  from_cache <- file.exists(detail_path) && !isTRUE(opts$overwrite_detail)
  if (from_cache) {
    json_text <- read_json_text(detail_path)
    http_status <- 200L
  } else {
    ans <- request_json(query = query, timeout_seconds = opts$timeout_seconds, opts = opts)
    http_status <- ans$status_code
    json_text <- ans$text
    if (isTRUE(http_status == 200L)) {
      write_text_file(json_text, detail_path)
    }
  }

  if (!isTRUE(http_status == 200L)) {
    return(data.frame(
      codigo = code,
      requested_code = code,
      status = "http_error",
      http_status = http_status,
      api_reported_count = NA_integer_,
      response_created_at = NA_character_,
      api_version = NA_character_,
      detail_path = detail_path,
      source_url = source_url,
      cached = from_cache,
      requested_date = as.character(requested_date),
      attempted_at_utc = now_utc(),
      attempt_count = ans$attempt_count,
      retry_count = ans$retry_count,
      retry_wait_seconds = ans$retry_wait_seconds,
      last_error = ans$last_error,
      stringsAsFactors = FALSE
    ))
  }

  parsed <- tryCatch(
    parse_detail_summary(
      json_text = json_text,
      requested_code = code,
      source_url = source_url,
      detail_path = detail_path
    ),
    error = function(e) {
      data.frame(
        codigo = code,
        requested_code = code,
        status = paste0("parse_error: ", conditionMessage(e)),
        http_status = http_status,
        api_reported_count = NA_integer_,
        response_created_at = NA_character_,
        api_version = NA_character_,
        detail_path = detail_path,
        source_url = source_url,
        requested_date = as.character(requested_date),
        attempted_at_utc = now_utc(),
        attempt_count = if (exists("ans")) ans$attempt_count else 0L,
        retry_count = if (exists("ans")) ans$retry_count else 0L,
        retry_wait_seconds = if (exists("ans")) ans$retry_wait_seconds else 0,
        last_error = conditionMessage(e),
        stringsAsFactors = FALSE
      )
    }
  )

  parsed$cached <- from_cache
  parsed$requested_date <- as.character(requested_date)
  parsed$attempted_at_utc <- now_utc()
  parsed$attempt_count <- if (from_cache) 0L else ans$attempt_count
  parsed$retry_count <- if (from_cache) 0L else ans$retry_count
  parsed$retry_wait_seconds <- if (from_cache) 0 else ans$retry_wait_seconds
  parsed$last_error <- if (from_cache) NA_character_ else ans$last_error
  parsed
}

new_throttle_state <- function(opts) {
  state <- new.env(parent = emptyenv())
  state$base_sleep <- opts$sleep_seconds
  state$current_sleep <- opts$sleep_seconds
  state$max_sleep <- opts$max_sleep_seconds
  state$success_streak <- 0L
  state
}

update_throttle_state <- function(state, detail_row, opts) {
  if (!isTRUE(opts$adaptive_throttle) || isTRUE(detail_row$cached[[1]])) {
    return(invisible(state))
  }

  retries <- suppressWarnings(as.integer(detail_row$retry_count[[1]]))
  status <- suppressWarnings(as.integer(detail_row$http_status[[1]]))
  if (is.na(retries)) retries <- 0L

  if (retries > 0L || identical(status, 429L)) {
    state$current_sleep <- min(
      state$max_sleep,
      max(state$current_sleep + 1, state$current_sleep * 1.5)
    )
    state$success_streak <- 0L
  } else {
    state$success_streak <- state$success_streak + 1L
    if (state$success_streak >= 50L && state$current_sleep > state$base_sleep) {
      state$current_sleep <- max(state$base_sleep, state$current_sleep * 0.9)
      state$success_streak <- 0L
    }
  }

  invisible(state)
}

detail_attempt_trace <- function(detail_row, code_row, opts, throttle_state) {
  data.frame(
    run_id = opts$run_id,
    attempted_at_utc = safe_chr(detail_row$attempted_at_utc),
    requested_date = as.character(as.Date(code_row[["requested_date"]])),
    codigo = safe_chr(code_row[["codigo"]]),
    result_codigo = safe_chr(detail_row$codigo),
    status = safe_chr(detail_row$status),
    http_status = safe_int(detail_row$http_status),
    cached = as.logical(detail_row$cached[[1]]),
    attempt_count = safe_int(detail_row$attempt_count),
    retry_count = safe_int(detail_row$retry_count),
    retry_wait_seconds = safe_num(detail_row$retry_wait_seconds),
    throttle_sleep_seconds = throttle_state$current_sleep,
    detail_path = safe_chr(detail_row$detail_path),
    source_url = safe_chr(detail_row$source_url),
    last_error = safe_chr(detail_row$last_error),
    stringsAsFactors = FALSE
  )
}


# ── Main ─────────────────────────────────────────────────────────────────────
main <- function() {
  opts <- parse_args(commandArgs(trailingOnly = TRUE))

  dirs <- list(
    root = opts$output_root,
    daily_json = file.path(opts$output_root, "daily_json"),
    daily_codes = file.path(opts$output_root, "daily_codes"),
    detail_json = file.path(opts$output_root, "detail_json"),
    manifests = file.path(opts$output_root, "manifests")
  )
  for (d in dirs) {
    dir.create(d, showWarnings = FALSE, recursive = TRUE)
  }

  daily_manifest_path <- file.path(dirs$manifests, "daily_batches.csv")
  codes_manifest_path <- file.path(dirs$manifests, "order_codes.csv")
  detail_manifest_path <- file.path(dirs$manifests, "detail_downloads.csv")
  detail_attempts_path <- file.path(dirs$manifests, "detail_attempts.csv")
  detail_runs_path <- file.path(dirs$manifests, "detail_runs.csv")

  cat("Repo root        :", REPO_ROOT, "\n")
  cat("Output root      :", dirs$root, "\n")
  cat("Run ID           :", opts$run_id, "\n")
  cat("Date range       :", as.character(opts$start_date), "to", as.character(opts$end_date), "\n")
  cat("Buyer filter     :", ifelse(is.null(opts$codigo_organismo), "<none>", opts$codigo_organismo), "\n")
  cat("Supplier filter  :", ifelse(is.null(opts$codigo_proveedor), "<none>", opts$codigo_proveedor), "\n")
  cat("Sleep seconds    :", opts$sleep_seconds, "\n")
  cat("Adaptive throttle:", opts$adaptive_throttle, "\n")
  cat("Max sleep seconds:", opts$max_sleep_seconds, "\n")
  cat("Max retries      :", opts$max_retries, "\n")
  cat("Reverse details  :", opts$reverse_details, "\n")
  cat("Daily only       :", opts$daily_only, "\n")
  cat("Details only     :", opts$details_only, "\n")
  cat("Overwrite daily  :", opts$overwrite_daily, "\n")
  cat("Overwrite detail :", opts$overwrite_detail, "\n\n")

  append_manifest_rows(
    detail_runs_path,
    data.frame(
      run_id = opts$run_id,
      started_at_utc = now_utc(),
      start_date = as.character(opts$start_date),
      end_date = as.character(opts$end_date),
      daily_only = opts$daily_only,
      details_only = opts$details_only,
      reverse_details = opts$reverse_details,
      sleep_seconds = opts$sleep_seconds,
      adaptive_throttle = opts$adaptive_throttle,
      max_sleep_seconds = opts$max_sleep_seconds,
      max_retries = opts$max_retries,
      retry_base_seconds = opts$retry_base_seconds,
      retry_cap_seconds = opts$retry_cap_seconds,
      manifest_flush_every = opts$manifest_flush_every,
      output_root = dirs$root,
      stringsAsFactors = FALSE
    )
  )

  if (!isTRUE(opts$details_only)) {
    dates <- seq.Date(opts$start_date, opts$end_date, by = "day")
    all_daily_rows <- vector("list", length(dates))
    all_daily_summaries <- vector("list", length(dates))

    cat("Stage 1/2 — Daily order discovery\n")
    for (i in seq_along(dates)) {
      d <- dates[[i]]
      cat(sprintf("  [%d/%d] %s\n", i, length(dates), as.character(d)))
      batch <- download_daily_batch(d, opts = opts, dirs = dirs)
      all_daily_rows[[i]] <- batch$rows
      all_daily_summaries[[i]] <- batch$summary
      Sys.sleep(opts$sleep_seconds)
    }

    daily_rows_df <- do.call(rbind, Filter(nrow, all_daily_rows))
    if (is.null(daily_rows_df)) {
      daily_rows_df <- data.frame(stringsAsFactors = FALSE)
    }
    daily_summary_df <- do.call(rbind, all_daily_summaries)

    if (nrow(daily_summary_df)) {
      upsert_manifest(
        path = daily_manifest_path,
        new_df = daily_summary_df,
        key_cols = c("requested_date")
      )
    }

    if (nrow(daily_rows_df)) {
      daily_rows_df <- dedupe_rows(daily_rows_df, c("requested_date", "codigo"))
      upsert_manifest(
        path = codes_manifest_path,
        new_df = daily_rows_df,
        key_cols = c("codigo")
      )
    } else if (!file.exists(codes_manifest_path)) {
      write.csv(
        data.frame(
          requested_date = character(),
          codigo = character(),
          nombre = character(),
          codigo_estado = integer(),
          source_url = character(),
          stringsAsFactors = FALSE
        ),
        codes_manifest_path,
        row.names = FALSE
      )
    }

    cat("\n  Daily batches processed :", nrow(daily_summary_df), "\n")
    cat("  Order codes discovered  :", nrow(daily_rows_df), "\n\n")
  }

  if (!isTRUE(opts$daily_only)) {
    codes_df <- read_manifest(codes_manifest_path)
    if (!nrow(codes_df)) {
      stop(
        "No order codes available. Run the script without --details-only first or check the date range.",
        call. = FALSE
      )
    }

    if (!"requested_date" %in% names(codes_df)) {
      stop("order_codes.csv is missing required column `requested_date`.", call. = FALSE)
    }
    if (!"codigo" %in% names(codes_df)) {
      stop("order_codes.csv is missing required column `codigo`.", call. = FALSE)
    }

    codes_df$requested_date <- as.Date(codes_df$requested_date)
    codes_df <- codes_df[
      !is.na(codes_df$requested_date)
        & codes_df$requested_date >= opts$start_date
        & codes_df$requested_date <= opts$end_date,
      ,
      drop = FALSE
    ]
    if (!nrow(codes_df)) {
      stop(
        "No order codes remain after applying the requested date window to order_codes.csv.",
        call. = FALSE
      )
    }
    order_index <- order(codes_df$requested_date, codes_df$codigo, decreasing = isTRUE(opts$reverse_details))
    codes_df <- codes_df[order_index, , drop = FALSE]

    existing_detail <- read_manifest(detail_manifest_path)
    already_done <- character()
    if (nrow(existing_detail) && "codigo" %in% names(existing_detail) && "status" %in% names(existing_detail)) {
      ok_mask <- existing_detail$status == "ok"
      already_done <- unique(existing_detail$codigo[ok_mask])
    }

    if (!isTRUE(opts$overwrite_detail) && length(already_done)) {
      codes_df <- codes_df[!(codes_df$codigo %in% already_done), , drop = FALSE]
    }

    if (is.finite(opts$max_details)) {
      codes_df <- head(codes_df, opts$max_details)
    }

    cat("Stage 2/2 — Detailed order downloads\n")
    cat("  Codes queued:", nrow(codes_df), "\n")

    detail_rows <- list()
    throttle_state <- new_throttle_state(opts)
    ok_n <- 0L
    err_n <- 0L
    retry_n <- 0L
    cached_n <- 0L
    flushed_n <- 0L

    for (i in seq_len(nrow(codes_df))) {
      row <- codes_df[i, , drop = FALSE]
      cat(sprintf(
        "  [%d/%d] %s (sleep %.1fs)\n",
        i,
        nrow(codes_df),
        row$codigo[[1]],
        throttle_state$current_sleep
      ))

      detail_row <- download_order_detail(row, opts = opts, dirs = dirs)
      update_throttle_state(throttle_state, detail_row, opts)
      append_manifest_rows(
        detail_attempts_path,
        detail_attempt_trace(detail_row, row, opts, throttle_state)
      )

      detail_rows[[length(detail_rows) + 1L]] <- detail_row
      ok_n <- ok_n + as.integer(detail_row$status[[1]] == "ok")
      err_n <- err_n + as.integer(detail_row$status[[1]] != "ok")
      retry_value <- safe_int(detail_row$retry_count)
      if (is.na(retry_value)) retry_value <- 0L
      retry_n <- retry_n + retry_value
      cached_n <- cached_n + as.integer(isTRUE(detail_row$cached[[1]]))

      if (
        opts$manifest_flush_every > 0L &&
          length(detail_rows) >= opts$manifest_flush_every
      ) {
        detail_df <- rbind_fill(detail_rows)
        upsert_manifest(
          path = detail_manifest_path,
          new_df = detail_df,
          key_cols = c("codigo")
        )
        flushed_n <- flushed_n + nrow(detail_df)
        detail_rows <- list()
        cat(sprintf("  Manifest flushed: %d rows total this run\n", flushed_n))
      }

      if (!isTRUE(detail_row$cached[[1]])) {
        Sys.sleep(throttle_state$current_sleep)
      }
    }

    if (length(detail_rows)) {
      detail_df <- rbind_fill(detail_rows)
      upsert_manifest(
        path = detail_manifest_path,
        new_df = detail_df,
        key_cols = c("codigo")
      )
    }

    cat("\n  Detailed downloads written:", ok_n, "\n")
    cat("  Cached detail rows skipped :", cached_n, "\n")
    cat("  Errors / non-ok rows      :", err_n, "\n")
    cat("  Retry attempts observed   :", retry_n, "\n")
    cat("  Attempt trace             :", detail_attempts_path, "\n")
  }

  cat("\nDone.\n")
  cat("Daily manifest :", daily_manifest_path, "\n")
  cat("Codes manifest :", codes_manifest_path, "\n")
  cat("Detail manifest:", detail_manifest_path, "\n")
  cat("Attempt trace  :", detail_attempts_path, "\n")
  cat("Run trace      :", detail_runs_path, "\n")
}


main()
