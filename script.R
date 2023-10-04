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

# configuration

h3_res <- 7
shapefile <- "https://github.com/iobis/mwhs-shapes/raw/master/output/marine_world_heritage.gpkg"
sf_use_s2(FALSE)
st <- storr::storr_rds("storr")
fish_classes <- c("Actinopteri", "Cladistii", "Coelacanthi", "Elasmobranchii", "Holocephali", "Myxini", "Petromyzonti", "Teleostei")
turtle_orders <- c("Testudines")
mammal_classes <- c("Mammalia")

# read shapefile

sites <- read_sf(shapefile) %>%
  mutate(name_simplified = gsub("_+", "_", gsub("[^[:alnum:]]", "_", tolower(stri_trans_general(name, "latin-ascii"))))) %>%
  group_by(name_simplified) %>%
  summarize()

# get occurrences from API

occ_for_geom <- function(geom) {
  wkt <- st_as_text(st_as_sfc(st_bbox(geom)), digits = 6)
  occ <- occurrence(geometry = wkt, fields = c("decimalLongitude", "decimalLatitude", "coordinateUncertaintyInMeters", "date_year", "phylum", "class", "order", "family", "genus", "species", "aphiaID", "category"))
  return(occ)
}

for (site_name in sites$name_simplified) {

  message(site_name)
  site <- sites %>% filter(name_simplified == site_name)
  
  if (!st$exists(site_name)) {

    # get occurrences after applying buffer and bounding box
    
    site_buffered_bbox <- st_as_sfc(site %>% st_buffer(1) %>% st_bbox()) 

    occ <- occ_for_geom(site_buffered_bbox) %>%
      filter(!is.na(species)) %>%
      rename(c("redlist_category" = "category", "AphiaID" = "aphiaID"))
    
    occ_grouped <- occ %>%
      group_by(decimalLongitude, decimalLatitude, phylum, class, order, family, genus, species, AphiaID, redlist_category) %>%
      summarize(
        records = n(),
        max_year = max(date_year, na.rm = TRUE),
        coordinateUncertaintyInMeters = max(as.numeric(coordinateUncertaintyInMeters), na.rm = TRUE)
      ) %>%
      ungroup()

    # calculate distance from points to geometry
    
    site_noholes <- site %>% st_remove_holes()

    points <- occ_grouped %>%
      group_by(decimalLongitude, decimalLatitude) %>%
      summarize() %>%
      st_as_sf(coords = c("decimalLongitude", "decimalLatitude"), crs = 4326, remove = FALSE) %>%
      mutate(distance = as.numeric(st_distance(geometry, site_noholes))) %>%
      st_drop_geometry()
  
    occ_grouped <- occ_grouped %>%
      left_join(points, by = c("decimalLongitude", "decimalLatitude")) %>%
      mutate_at(vars(coordinateUncertaintyInMeters), ~ if_else(is.na(.) | is.infinite(.), 10, .))

    species <- occ_grouped %>%
      filter(distance <= coordinateUncertaintyInMeters) %>%
      group_by(phylum, class, order, family, genus, species, AphiaID, redlist_category) %>%
      summarize(
        records = sum(records),
        max_year = max(max_year, na.rm = TRUE)
      ) %>%
      mutate(max_year = ifelse(is.infinite(max_year), NA, max_year)) %>%
      mutate(
        group = case_when(
          class %in% fish_classes ~ "fish",
          order %in% turtle_orders ~ "turtle",
          class %in% mammal_classes ~ "mammal"
        )
      ) %>%
      filter(!is.na(group) & species != "Homo sapiens")
    
    st$set(site_name, species)

  }
  
}

for (site_name in sites$name_simplified) {
  site_list <- st$get(site_name)
  json = toJSON(list(
    created = unbox(strftime(as.POSIXlt(Sys.time(), "UTC"), "%Y-%m-%dT%H:%M:%S")),
    species = site_list
  ), pretty = TRUE)
  write(json, glue("lists/{site_name}.json"))
}

write(toJSON(sites$name_simplified), "lists/sites.json")
