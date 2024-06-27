library(rvest)
library(tidyverse)

options(HTTPUserAgent = str_c("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                              " AppleWebKit/605.1.15 (KHTML, like Gecko)",
                              " Version/17.5 Safari/605.1.15"))

get_names <- function(text) {
  unique(str_match_all(text,
                       "class='wl-([a-z0-9]{1,20})-td")[[1]][, 2])[-1:-2]
}

parse_millions <- function(x) {
  millions <- str_detect(x, "m")
  billions <- str_detect(x, "b")
  y <-
    x |>
    str_remove_all("[mb]") |>
    parse_number()

  case_when(millions ~ y * 1e6,
            billions ~ y * 1e9,
            .default = y)
}

get_ipos <- function(year) {
  url <- str_c("https://clients3.weblink.com.au/Clients/",
               "SmallCaps/v2/IPOTracker.aspx?year=", year)
  html <- read_html(url)
  text <- html_text(html)

  df <-
    str_extract(text, "contents = \"(<tbody>.*</tbody>)\"", group = 1) |>
    minimal_html() |>
    html_element("tbody") |>
    html_table()

  names(df) <- get_names(text)

  df |>
    mutate(across(issueprice:low, parse_number),
           across(marketcap, parse_millions),
           across(listed, \(x) parse_date(x, format = "%d/%m/%Y")))
}

asx_ipos <- map(2017:2024, get_ipos) |> list_rbind()

pq_dir <- file.path(Sys.getenv("DATA_DIR"), "asx")
if (!dir.exists(pq_dir)) dir.create(pq_dir)
pq_file <- file.path(pq_dir, "asx_ipos.parquet")

asx_ipos |> arrow::write_parquet(pq_file)
