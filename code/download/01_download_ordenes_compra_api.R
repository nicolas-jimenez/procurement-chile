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
      "  --timeout-seconds <value>    Request timeout in seconds. Default: 60.",
      "  --max-details <value>        Optional cap on detail downloads (for testing).",
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
    timeout_seconds = 60,
    max_details = Inf,
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
    } else if (identical(arg, "--timeout-seconds")) {
      opts$timeout_seconds <- as.numeric(value)
    } else if (identical(arg, "--max-details")) {
      opts$max_details <- as.numeric(value)
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
  if (!is.finite(opts$timeout_seconds) || opts$timeout_seconds <= 0) {
    stop("--timeout-seconds must be a positive number.", call. = FALSE)
  }
  if (!is.finite(opts$max_details) || opts$max_details <= 0) {
    opts$max_details <- Inf
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

safe_code_path <- function(code) {
  gsub("[^A-Za-z0-9._-]", "_", code)
}


# ── API helpers ──────────────────────────────────────────────────────────────
API_BASE <- "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
USER_AGENT <- paste(
  "procurement-chile-local/ordenes-compra-downloader",
  "(research script; contact: api@chilecompra.cl if needed)"
)

is_quota_response <- function(text) {
  # Mercado Público returns {"Codigo":203,"Mensaje":"Ticket superó la cuota diaria asignada."}
  # as a 2xx body when the daily ticket quota is exhausted.
  grepl("cuota", text, ignore.case = TRUE) || grepl("Ticket", text, fixed = TRUE)
}

request_json <- function(query, timeout_seconds) {
  resp <- RETRY(
    verb = "GET",
    url = API_BASE,
    query = query,
    times = 3,
    pause_base = 1,
    pause_cap = 8,
    terminate_on = c(400, 401, 403, 404),
    user_agent(USER_AGENT),
    timeout(timeout_seconds)
  )

  text <- content(resp, as = "text", encoding = "UTF-8")
  sc   <- status_code(resp)

  # Treat quota-exceeded responses as a distinct non-2xx status so callers
  # know not to write the file and can stop early.
  if (sc %/% 100L == 2L && is_quota_response(text)) {
    sc <- 429L
  }

  list(status_code = sc, text = text)
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
  source_url <- modify_url(API_BASE, query = query)

  from_cache <- file.exists(daily_json_path) && !isTRUE(opts$overwrite_daily)
  if (from_cache) {
    json_text <- read_json_text(daily_json_path)
    http_status <- 200L
  } else {
    ans <- request_json(query = query, timeout_seconds = opts$timeout_seconds)
    http_status <- ans$status_code
    json_text <- ans$text
    if (http_status %/% 100L == 2L) {
      write_text_file(json_text, daily_json_path)
    }
  }

  if (http_status %/% 100L != 2L) {
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
  year_str <- format(requested_date, "%Y")
  month_str <- format(requested_date, "%m")

  detail_path <- file.path(
    dirs$detail_json,
    year_str,
    month_str,
    paste0(safe_code_path(code), ".json")
  )

  query <- list(
    codigo = code,
    ticket = opts$ticket
  )
  source_url <- modify_url(API_BASE, query = query)

  from_cache <- file.exists(detail_path) && !isTRUE(opts$overwrite_detail)
  if (from_cache) {
    json_text <- read_json_text(detail_path)
    http_status <- 200L
  } else {
    ans <- request_json(query = query, timeout_seconds = opts$timeout_seconds)
    http_status <- ans$status_code
    json_text <- ans$text
    if (http_status %/% 100L == 2L) {
      write_text_file(json_text, detail_path)
    }
  }

  if (http_status %/% 100L != 2L) {
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
        stringsAsFactors = FALSE
      )
    }
  )

  parsed$cached <- from_cache
  parsed
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

  cat("Repo root        :", REPO_ROOT, "\n")
  cat("Output root      :", dirs$root, "\n")
  cat("Date range       :", as.character(opts$start_date), "to", as.character(opts$end_date), "\n")
  cat("Buyer filter     :", ifelse(is.null(opts$codigo_organismo), "<none>", opts$codigo_organismo), "\n")
  cat("Supplier filter  :", ifelse(is.null(opts$codigo_proveedor), "<none>", opts$codigo_proveedor), "\n")
  cat("Daily only       :", opts$daily_only, "\n")
  cat("Details only     :", opts$details_only, "\n")
  cat("Overwrite daily  :", opts$overwrite_daily, "\n")
  cat("Overwrite detail :", opts$overwrite_detail, "\n\n")

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
    codes_df <- codes_df[order(codes_df$requested_date, codes_df$codigo), , drop = FALSE]

    existing_detail <- read_manifest(detail_manifest_path)
    already_done <- character()
    if (nrow(existing_detail) && "codigo" %in% names(existing_detail) && "status" %in% names(existing_detail)) {
      ok_mask <- existing_detail$status == "ok" & file.exists(existing_detail$detail_path)
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

    detail_rows <- vector("list", nrow(codes_df))
    for (i in seq_len(nrow(codes_df))) {
      row <- codes_df[i, , drop = FALSE]
      cat(sprintf("  [%d/%d] %s\n", i, nrow(codes_df), row$codigo[[1]]))
      detail_rows[[i]] <- download_order_detail(row, opts = opts, dirs = dirs)
      Sys.sleep(opts$sleep_seconds)
    }

    if (length(detail_rows)) {
      detail_df <- do.call(rbind, detail_rows)
      upsert_manifest(
        path = detail_manifest_path,
        new_df = detail_df,
        key_cols = c("codigo")
      )

      ok_n <- sum(detail_df$status == "ok", na.rm = TRUE)
      err_n <- sum(detail_df$status != "ok", na.rm = TRUE)
      cat("\n  Detailed downloads written:", ok_n, "\n")
      cat("  Errors / non-ok rows      :", err_n, "\n")
    }
  }

  cat("\nDone.\n")
  cat("Daily manifest :", daily_manifest_path, "\n")
  cat("Codes manifest :", codes_manifest_path, "\n")
  cat("Detail manifest:", detail_manifest_path, "\n")
}


main()
