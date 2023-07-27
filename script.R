library(dplyr)
library(arrow)
library(h3)
library(DBI)
library(sf)
library(h3jsr)
library(stringi)
library(jsonlite)
library(glue)

# configuration

res <- 7
export_file <- "obis_20230726.parquet"
shapefile <- "https://github.com/iobis/mwhs-shapes/raw/master/output/marine_world_heritage.gpkg"

# index occurrences and load into sqlite

row_to_geo <- function(row) {
  geo_to_h3(c(row$decimalLatitude, row$decimalLongitude), res)
}

reader <- open_dataset(export_file) %>%
  select(decimalLongitude, decimalLatitude, phylum, class, order, family, genus, species, scientificName, date_year, redlist_category) %>%
  as_record_batch_reader()

if (file.exists("temp.db")) {
  file.remove("temp.db")
}
con <- dbConnect(RSQLite::SQLite(), "temp.db")

while (TRUE) {
  batch <- reader$read_next_batch()
  if (is.null(batch)) {
    break
  }
  batch %>%
    as.data.frame() %>%
    mutate(h3 = row_to_geo(.)) %>%
    group_by(h3, phylum, class, order, family, genus, species, scientificName, redlist_category) %>%
    summarize(records = n(), max_year = max(date_year, na.rm = TRUE)) %>%
    dbWriteTable(con, "occurrence", ., append = TRUE)
}

# index site shapes and load into sqlite

sites <- read_sf(shapefile)
polys <- polygon_to_cells(sites, res)
sites_h3 <- sites %>%
  st_drop_geometry() %>%
  select(name) %>%
  mutate(h3 = polys) %>%
  tidyr::unnest(h3)

dbWriteTable(con, "sites", sites_h3, append = FALSE, overwrite = TRUE)

# query sqlite

df <- dbGetQuery(con, "select phylum, class, `order`, family, genus, species, scientificName, redlist_category, sites.name as site_name, sum(records) as records, max(max_year) as max_year from occurrence left join sites on sites.h3 = occurrence.h3 group by phylum, class, `order`, family, species, scientificName, redlist_category, sites.name") %>%
  filter(!is.na(site_name) & !is.na(species) & !is.na(phylum)) %>%
  mutate(max_year = ifelse(is.infinite(max_year), NA, max_year))

fish_classes <- c("Actinopteri", "Cladistii", "Coelacanthi", "Elasmobranchii", "Holocephali", "Myxini", "Petromyzonti", "Teleostei")
turtle_orders <- c("Testudines")
mammal_classes <- c("Mammalia")

scope <- df %>%
  mutate(
    group = case_when(
      class %in% fish_classes ~ "fish",
      order %in% turtle_orders ~ "turtle",
      class %in% mammal_classes ~ "mammal"
    )
  ) %>%
  filter(!is.na(group) & scientificName != "Homo sapiens") %>%
  mutate(
    site_name_simple = gsub("_+", "_", gsub("[^[:alnum:]]", "_", tolower(stri_trans_general(site_name, "latin-ascii"))))
  )

sites <- unique(scope$site_name_simple)

for (site in sites) {
  site_list <- scope %>%
    filter(site_name_simple == site) %>%
    select(-site_name_simple) %>%
    arrange(group, phylum, class, order, scientificName)
  json = toJSON(list(
    created = unbox(strftime(as.POSIXlt(Sys.time(), "UTC"), "%Y-%m-%dT%H:%M:%S")),
    species = site_list
  ), pretty = TRUE)
  write(json, glue("lists/{site}.json"))
}
