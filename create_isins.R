library(tidyverse)
library(readxl)
library(DBI)

db <- dbConnect(duckdb::duckdb())

fix_names <- function(names) {
  names <- str_to_lower(names)
  names <- str_replace_all(names, "[^a-z0-9]+", "_")
  names
}

save_parquet <- function(df, name, schema = "",
                         path = Sys.getenv("DATA_DIR")) {
  file_dir <- file.path(path, schema)
  if (!dir.exists(file_dir)) dir.create(file_dir)

  file_path <- file.path(path, schema, str_c(name, ".parquet"))
  arrow::write_parquet(collect(df), sink = file_path)
}

t <- tempfile(fileext = ".xls")
url <- "https://www.asx.com.au/content/dam/asx/issuers/ISIN.xls"
download.file(url, t)
isins <-
  read_excel(t, .name_repair = fix_names) |>
  filter(!is.na(isin_code)) |>
  save_parquet("isins", "asx")

dbDisconnect(db)
