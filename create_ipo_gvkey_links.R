library(tidyverse)
library(DBI)
library(farr)
library(googlesheets4)

db <- dbConnect(duckdb::duckdb())

asx_ipos <- load_parquet(db, schema = "asx", table = "asx_ipos")
isins <- load_parquet(db, schema = "asx", table = "isins")
g_secd <- load_parquet(db, schema = "comp", table = "g_secd")
g_security <- load_parquet(db, schema = "comp", table = "g_security")

gs_gvkeys <- as_sheets_id("1VsBzixDUMSSSdfiXmY16Y1j9qblfABHEMAWfWXJLqxQ")

manual_matches <-
  read_sheet(gs_gvkeys, sheet = "manual_matches",
             col_types = "cccDcccc-") |>
  copy_to(db, df = _, name = "manual_matches", overwrite = TRUE) |>
  select(code, listed, isin_code, gvkey, iid)

comp_isins <-
  g_security |>
  select(gvkey, iid, isin)

ipo_gvkey_links <-
  asx_ipos |>
  anti_join(manual_matches, join_by(code, listed)) |>
  left_join(
    isins |>
      select(asx_code, isin_code), join_by(code == asx_code)) |>
  left_join(comp_isins, join_by(isin_code == isin)) |>
  mutate(matched = !is.na(isin_code),
         matched_gvkey = !is.na(gvkey)) |>
  select(code, listed, isin_code, gvkey, iid) |>
  union_all(manual_matches) |>
  compute(name = "ipo_gvkey_links")

pq_dir <- file.path(Sys.getenv("DATA_DIR"), "asx")
if (!dir.exists(pq_dir)) dir.create(pq_dir)

pq_path <- file.path(Sys.getenv("DATA_DIR"), "asx", "ipo_gvkey_links.parquet")
sql <- paste0("COPY (FROM ipo_gvkey_links) TO '", pq_path, "'")
dbExecute(db, sql)

dbDisconnect(db)
