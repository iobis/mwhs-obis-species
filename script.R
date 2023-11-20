library(dplyr)
library(arrow)
library(h3)
library(DBI)
library(sf)
library(h3jsr)
library(stringi)
library(jsonlite)
library(glue)
library(robis)
library(tidyr)
library(nngeo)
library(rredlist)
library(readr)
library(purrr)
library(rgbif)
library(worrms)

# WARNING: DELETE STORR CACHE IF NECESSARY

# configuration

shapefile <- "https://github.com/iobis/mwhs-shapes/raw/master/output/marine_world_heritage.gpkg"
sf_use_s2(FALSE)
fish_classes <- c("Actinopteri", "Cladistii", "Coelacanthi", "Elasmobranchii", "Holocephali", "Myxini", "Petromyzonti", "Teleostei")
turtle_orders <- c("Testudines")
mammal_classes <- c("Mammalia")
h3_res <- 5 # TODO: increase resolution

# read shapefile

sites <- read_sf(shapefile) %>%
  mutate(name_simplified = gsub("_+", "_", gsub("[^[:alnum:]]", "_", tolower(stri_trans_general(name, "latin-ascii"))))) %>%
  group_by(name_simplified) %>%
  summarize() %>%
  st_remove_holes()

# OBIS occurrences for geometry from API

obis_occ_for_geom <- function(geom) {
  wkt <- st_as_text(st_as_sfc(st_bbox(geom)), digits = 6)
  occ <- occurrence(geometry = wkt, fields = c("decimalLongitude", "decimalLatitude", "coordinateUncertaintyInMeters", "date_year", "phylum", "class", "order", "family", "genus", "species", "aphiaID"))
  return(occ)
}

# utilities

check_scope <- function(df) {
  df %>%
    mutate(
      group = case_when(
        class %in% fish_classes ~ "fish",
        order %in% turtle_orders ~ "turtle",
        class %in% mammal_classes ~ "mammal"
      )
    ) %>%
    filter(!is.na(group) & species != "Homo sapiens")
}

# OBIS species for site

obis_species_for_site <- function(site) {

  # get occurrences after applying buffer and bounding box
  
  site_buffered_bbox <- st_as_sfc(site %>% st_buffer(1) %>% st_bbox()) 
  
  occ <- obis_occ_for_geom(site_buffered_bbox) %>%
    filter(!is.na(species)) %>%
    rename(c("AphiaID" = "aphiaID"))
  
  occ_grouped <- occ %>%
    group_by(decimalLongitude, decimalLatitude, phylum, class, order, family, genus, species, AphiaID) %>%
    summarize(
      records = n(),
      max_year = max(date_year, na.rm = TRUE),
      coordinateUncertaintyInMeters = max(as.numeric(coordinateUncertaintyInMeters), na.rm = TRUE)
    ) %>%
    ungroup()
  
  # calculate distance from points to geometry

  points <- occ_grouped %>%
    group_by(decimalLongitude, decimalLatitude) %>%
    summarize() %>%
    st_as_sf(coords = c("decimalLongitude", "decimalLatitude"), crs = 4326, remove = FALSE) %>%
    mutate(distance = as.numeric(st_distance(geometry, site))) %>%
    st_drop_geometry()
  
  occ_grouped <- occ_grouped %>%
    left_join(points, by = c("decimalLongitude", "decimalLatitude")) %>%
    mutate_at(vars(coordinateUncertaintyInMeters), ~ if_else(is.na(.) | is.infinite(.), 10, .))
  
  species <- occ_grouped %>%
    filter(distance <= coordinateUncertaintyInMeters) %>%
    group_by(phylum, class, order, family, genus, species, AphiaID) %>%
    summarize(
      records = sum(records),
      max_year = max(max_year, na.rm = TRUE)
    ) %>%
    mutate(max_year = ifelse(is.infinite(max_year), NA, max_year)) %>%
    check_scope()
  species
}

# GBIF species for site
# limitations for this approach: no max year, no number of records, missing red list flag

worms_for_names <- possibly(function(x) {
  worrms::wm_records_names(x, marine_only = FALSE) %>%
    setNames(x) %>%
    bind_rows(.id = "input")
}, otherwise = NULL)

gbif_species_for_site <- function(site) {
  cells <- polygon_to_cells(site$geom, h3_res)
  if (all(is.na(cells))) {
    return(data.frame())
  }
  polys <- cell_to_polygon(cells) %>% st_as_text()
  res <- map(polys, function(wkt) {
    occ_search(geometry = wkt, facet = "speciesKey", facetLimit = 10000, limit = 0)$facets$speciesKey
  }, .progress = TRUE)
  species_keys <- res %>% bind_rows() %>% pull(name) %>% unique()
  usages <- map(species_keys, function(x) { name_usage(x)$data }, .progress = TRUE)
  species_names <- usages %>% bind_rows() %>% pull(canonicalName) %>% unique()
  name_batches <- split(species_names, as.integer((seq_along(species_names) - 1) / 50))
  plan(multisession, workers = 10)
  aphiaids <- future_map(name_batches, worms_for_names, .progress = TRUE) %>%
    bind_rows() %>%
    pull(valid_AphiaID) %>%
    unique()
  aphiaid_batches <- split(aphiaids, as.integer((seq_along(aphiaids) - 1) / 50))
  species <- future_map(aphiaid_batches, wm_record, .progress = TRUE) %>%
    bind_rows() %>%
    mutate_at(c("isMarine", "isBrackish"), ~replace_na(., 0)) %>%
    filter(isMarine + isBrackish > 0) %>%
    select(phylum, class, order, family, genus, species = scientificname, AphiaID) %>%
    check_scope()
  species  
}

# run

st <- storr::storr_rds("storr")

process_site <- function(site_name) {
  message(site_name)
  site <- sites %>%
    filter(name_simplified == site_name) 
  if (!st$exists(site_name)) {
    obis_species <- obis_species_for_site(site)
    gbif_species <- gbif_species_for_site(site)
    st$set(site_name, list(obis = obis_species, gbif = gbif_species))
  }
}

for (site_name in sites$name_simplified) {
  process_site(site_name)
}

# get redlist

redlist <- data.frame()
page <- 0
while (TRUE) {
  res <- rl_sp(page, key = "a936c4f78881e79a326e73c4f97f34a6e7d8f9f9e84342bff73c3ceda14992b9")$result
  if (length(res) == 0) {
    break
  }
  redlist <- bind_rows(redlist, res)
  page <- page + 1
}
redlist <- redlist %>%
  filter(is.na(population)) %>%
  filter(category %in% c("CR", "EN", "EW", "EX", "VU", "NT")) %>%
  select(species = scientific_name, redlist_category = category)

# output

for (site_name in sites$name_simplified) {
  message(site_name)
  site_lists <- st$get(site_name)
  obis <- site_lists[["obis"]] %>%
    mutate(
      obis = TRUE,
      gbif = AphiaID %in% site_lists[["gbif"]]$AphiaID
    )
  if (nrow(site_lists[["gbif"]]) > 0) {
    only_gbif <- site_lists[["gbif"]] %>%
      filter(!AphiaID %in% site_lists[["obis"]]$AphiaID) %>%
      mutate(
        obis = FALSE,
        gbif = TRUE
      )
  }
  combined <- bind_rows(obis, only_gbif) %>%
    left_join(redlist, by = "species")
  json = toJSON(list(
    created = unbox(strftime(as.POSIXlt(Sys.time(), "UTC"), "%Y-%m-%dT%H:%M:%S")),
    species = combined
  ), pretty = TRUE)
  write(json, glue("lists/{site_name}.json"))
}

write(toJSON(sites$name_simplified), "lists/sites.json")
