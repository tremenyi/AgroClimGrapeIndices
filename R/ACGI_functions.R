#' Get mainland regions from filenames in a directory
#'
#' Search target directory for files matching "region_<region_name>" and returns unique instances
#'
#' @inheritParams get_mainland_shapes_from_sf_dir
#'
#' @return A vector with all mainland region names
#' @export
#'
#' @examples
#' my_directory <- '~/Dev/regions'
#' get_mainland_regions_from_filenames(my_directory)
get_mainland_regions_from_filenames <- function(dir_with_shape_files) {
  target_shape_dir_filenames <- dir(dir_with_shape_files, full.names = TRUE)
  target_shape_regions_all <- unique(unlist(lapply(target_shape_dir_filenames[grep("/region",target_shape_dir_filenames)],function(x){str_split(str_split(x, "/")[[1]][7], fixed("."))[[1]][1]})))

  # remove Tasmania from shape regions, as it is treated differently
  # (split into regions based on BoM Forecast Districts, rather than treated as only one region)
  target_shape_regions_all[-grep("region_tasmania", target_shape_regions_all)]
}


#' Load shape files from directory
#'
#' @param dir_with_shape_files Directory containing shape files for mainland regions
#'
#' @return List of simple features from the loaded shape files, st_cast to MULTIPLOYGON
#' @export
#'
#' @examples
#' my_directory <- '~/Dev/regions'
#' get_mainland_regions_from_filenames(my_directory)
get_mainland_shapes_from_sf_dir <- function(dir_with_shape_files) {
  target_shape_regions <- get_mainland_regions_from_filenames(dir_with_shape_files)
  shps <- lapply(target_shape_regions, function(x) { read_sf(target_shape_dir, x) } )
  lapply(shps, st_cast, "MULTIPOLYGON")
}


#' Extract specified list of regions from BOM shape file
#'
#' @param bom_tas_shape_file Shape file with BOM districts for Tasmanian regions
#' @param target_bom_districts_tas List of regions to extract by label name, in the format list(..., c(prepend = TRUE,  label = "South East")) where prepend = TRUE will prepend 'Tasmania' to the label
#'
#' @return List of simple features of each Tasmanian region in the BOM shape file, st_cast to MULTIPLOYGON
#' @export
#'
#' @examples
#' src_bom_district_shapes_tas <- "/mnt/external-data/BoM-Forecast-Regions/Tasmanian_post_gfe_forecast_districts.shp"
#' target_bom_districts_tas <- list(
#'   c(prepend = FALSE, label = "King Island"),
#'   c(prepend = FALSE, label = "Furneaux Islands"),
#'   c(prepend = TRUE,  label = "North West Coast"),
#'   c(prepend = TRUE,  label = "North East"),
#'   c(prepend = TRUE,  label = "Central North"),
#'   c(prepend = TRUE,  label = "East Coast"),
#'   c(prepend = TRUE,  label = "Upper Derwent Valley"),
#'   c(prepend = TRUE,  label = "South East"))
#' shps_tas <- get_tasmania_shapes_from_bom_shape_file(src_bom_district_shapes_tas, target_bom_districts_tas)
get_tasmania_shapes_from_bom_shape_file <- function(bom_tas_shape_file, target_bom_districts_tas) {
  Tasmanian_BoM_districts <- read_sf(bom_tas_shape_file)

  tmp_wine_districts_tas <- vector("list")
  for(i in 1:length(target_bom_districts_tas)[1]) {
    region <- target_bom_districts_tas[[i]]
    region_sf <- Tasmanian_BoM_districts[c(Tasmanian_BoM_districts$NAME %in% region[["label"]]), ]

    shp_row_ <- region_sf[c("NAME", "NAME", "PERIMETER", "AREA")]
    names(shp_row_) <- c("RegionName", "label", "Shape_Leng", "Shape_Area", "geometry")

    if (as.logical(region[["prepend"]])) {
      shp_row_$label <- paste('Tasmania', shp_row_$label)
    }

    shp_row_$RegionName <- str_replace_all(str_to_lower(shp_row_$RegionName), " ", "_")

    tmp_wine_districts_tas[[i]] <- combine_bom_region_collection(shp_row_)
  }

  lapply(tmp_wine_districts_tas, st_cast, "MULTIPOLYGON")
}


#' Combine SF with multiple geometry objects into one
#'
#' Combine the geometry objects of simple feature with multiple polygons into one multi-polygon.
#' Metadata such as Shape_Len and Shape_Area will be aggregated for the final polygon.
#'
#' @param sf Simple feature with multiple polygons stored in $geometry
#'
#' @return The input simple feature, but with the polygons combined
#'
combine_bom_region_collection <- function(sf) {
  # combine list of geometry objects into multi polygon
  multi <- st_combine(sf$geometry)

  # aggregate stats
  output <- sf %>%
    group_by(RegionName, label) %>%
    dplyr::summarise(Shape_Leng = sum(Shape_Leng), Shape_Area = sum(Shape_Area))

  # save new geometry
  output$geometry <- multi

  st_sf(output)
}


#' Get shapes for all wine regions (mainland and Tasmania)
#'
#' @inheritParams get_mainland_shapes_from_sf_dir
#' @inheritParams get_tasmania_shapes_from_bom_shape_file
#'
#' @return Vector with mainland shapes and Tasmania shapes
#' @export
#'
#' @examples
#' target_shape_dir <- "/path/to/mainland_regions/"
#' src_bom_district_shapes_tas <- "/path/to/bom_shape_file.shp"
#' target_bom_districts_tas <- list(  # see .get_tasmania_shapes_from_bom_shape_file
#' )
#' shapes <- get_all_wine_region_shapes(target_shape_dir, src_bom_district_shapes_tas, target_bom_districts_tas)
get_all_wine_region_shapes <- function(dir_with_mainland_shapes, file_with_bom_district_shapes_tas, target_bom_districts_tas) {
  # load mainland region shapes from shape files in target directory
  shapes_mainland <- get_mainland_shapes_from_sf_dir(dir_with_mainland_shapes)

  # load tasmania region shapes from bom forecast district shape file
  shapes_tas <- get_tasmania_shapes_from_bom_shape_file(file_with_bom_district_shapes_tas, target_bom_districts_tas)

  # combine mainland and tasmania geometries
  c(shapes_mainland, shapes_tas)
}



#' Extract geometry from vector of shapes
#'
#' @param shapes Shapes to process
#'
#' @return Vector of same length as input, with just the st_geometry of each shape for each item
#' @export
#'
extract_geom_for_shapes <- function(shapes) {
  do.call(c, lapply(shapes, function(x) st_geometry(x)))
}



#' Make Transformed Spatial Shape
#'
#' Transform the given shape geometries onto the projection of brick
#'
#' @param shape_geometries List of shape geometries to transform
#' @param brick Brick of which to use projection
#'
#' @return
#' @export
#'
#' @examples
make_transformed_spatial_shape <- function(shape_geometries, brick) {
  shp_tab <- data.frame(p = seq_along(shape_geometries))
  shp_tab$geometry <- shape_geometries
  shp_tab <- st_as_sf(shp_tab)
  shp_spatial <- as(shp_tab, "Spatial")

  spTransform(shp_spatial, CRS(projection(brick)))
}



#' Spatial cell extract for a list of files with the same spatial layer
#'
#' Apply the \code{cell_extract_spatial} function to a list of files, row binding the result
#' and
#'
#' @param raster_filenames List of files to process
#' @inheritParams cell_extract_spatial
#'
#' @return
#' @export
#'
#' @examples
cell_extract_spatial_file_list <- function(raster_filenames, spatial_layer, subset_layer = NULL, fun_list = list(mean = mean, var = var, n = length)) {

  # final output
  full_df <- data.frame()

  next_layer_offset <- 0

  for (f in raster_filenames) {
    # do spatial extract for this file
    df <- cell_extract_spatial(f, spatial_layer, subset_layer, fun_list)

    # layer index will start from one for every file
    # offset layer index to continue from previous file then convert back to char
    df$layer <- as.character(as.numeric(df$layer) + next_layer_offset)

    # add data from this file to final output
    full_df <- rbind(full_df, df)

    # prepare offset for next file
    prev_highest_layer <- full_df$layer[length(full_df$layer)]
    next_layer_offset <- as.numeric(prev_highest_layer) + 1
  }

  full_df
}


#' Spatial cell extract
#'
#' Identify the cells that are touched by the regions in the spatial layer and extract the data from those cells
#'
#' @param raster_filename File to process
#' @param spatial_layer Spatial layer to categorise data values by
#' @param subset_layer Permits extracting a subset of layers from the raster file
#' @param fun_list Functions to apply to each data point in the brick
#'
#' @return A data frame with extracted values from the raster file, grouped by each object in the spatial layer
#' @export
#'
#' @examples
cell_extract_spatial <- function(raster_filename, spatial_layer, subset_layer = NULL, fun_list = list(mean = mean, var = var, n = length)) {
  raster_brick <- raster::brick(raster_filename)
  if (is.null(subset_layer)) subset_layer <- 1:nlayers(raster_brick)
  cell_object_index <- tabularaster::cellnumbers(raster_brick, spatial_layer)

  future::plan(multiprocess)
  future::future_lapply(
    # allow processing within subset layer (ilayer)
    seq_along(subset_layer),
    function(ilayer) {
      cell_object_index %>%
        dplyr::mutate(value = raster::extract(raster_brick[[ilayer]], cell_object_index$cell_)) %>%
        dplyr::select(-cell_) %>%
        dplyr::group_by(object_) %>%
        summarize_if(purrr::is_bare_double, fun_list)
    } #close function ilayer inside future_lapply
  ) %>%
    bind_rows(.id = "layer") #close future_lapply and bind_rows output
}



cell_extract_spatial_variant <- function(target_files, wine_region_polygon_cells_per_domain_lookup) {

  # the unique list of cells in this domain that cover all of the regions
  target_cell_indices <- get_target_cell_indices_for_domain(target_domain, wine_region_polygon_cells_per_domain_lookup)

  data_series_for_all_files <- lapply(target_files, function(raster_filename) {
    tlog(raster_filename)

    raster_brick <- raster::brick(raster_filename)
    subset_layer <- 1:nlayers(raster_brick)

    future::plan(multiprocess)
    out <- future::future_lapply(
      raster::extract(raster_brick, target_cell_indices)
    ) %>%
      bind_rows(.id = "layer") #close future_lapply and bind_rows output

    colnames(out) <- sprintf("X%i",target_cell_indices)
    rownames(out) <- NULL

    return(as.data.frame(out))
  })

  gathered <- dplyr::bind_rows(data_series_for_all_files) %>%
    mutate(layer = row_number()) %>%
    gather(key = cell, value = mean, -layer) %>%
    mutate(domain = target_domain,
           cell = replace(cell, TRUE, substring(cell, 2))
    ) %>%
    dplyr::select(domain,
                  layer,
                  cell,
                  mean
    )

  return(gathered)
}


#' Get target cell indices for a domain from lookup object
#'
#' Return the cells in the specified domain that cover all of the regions.
#' Duplicate cells (i.e. cells that exist in multiple regions) will be listed only once.
#'
#' @param target_domain The domain in the lookup object to get the matching cells for
#' @param wine_region_polygon_cells_per_domain_lookup Pre-calculated lookup listing the cells that exist in each region for each domain
#'
#' @return An integer vector of unique cells, not necessarily in sequencial order
#' @export
#'
#' @examples
get_target_cell_indices_for_domain <- function(target_domain, wine_region_polygon_cells_per_domain_lookup) {
  target_domain_key <- paste("domain_", target_domain, sep="")

  unique(
    unlist(
      lapply(wine_region_polygon_cells_per_domain_lookup, function(x) { x[["matching_cells"]][[target_domain_key]] })
    )
  )
}



#' Extract values from cells matching a particular domain from a list of NetCDF files
#'
#' @param target_files List of input NetCDF files
#' @inheritParams get_target_cell_indices_for_domain
#'
#' @return
#' @export
#'
#' @examples
extract_variable_from_files_by_cells <- function(target_files, target_domain, wine_region_polygon_cells_per_domain_lookup) {

  # the unique list of cells in this domain that cover all of the regions
  target_cell_indices <- get_target_cell_indices_for_domain(target_domain, wine_region_polygon_cells_per_domain_lookup)

  if(!is.numeric(target_cell_indices)){
    if_na_return_NA <- NA
    return(if_na_return_NA)
  } else {
    data_series_for_all_files <- lapply(target_files, function(target_file) {
      tlog(target_file)

      # per some benchmarks, it appears to be faster to crop the the brick before performing the data extract
      # we map the cell indices from the entire brick to XYs, then from XYs to the indices of the crop
      # after the data is extracted from the brick, the indices for the whole brick are imposed back on the data

      # without the crop, the code would simply be this:
      # brick_of_nc <- raster::brick(target_file)
      # timeseries_per_cell <- t(raster::extract(brick_of_nc, target_cell_indices))

      brick_of_nc <- raster::brick(target_file)
      cropped_brick_of_nc <- crop(brick_of_nc, extentFromCells(brick_of_nc, target_cell_indices))

      target_XYs_parent <- xyFromCell(brick_of_nc, target_cell_indices)
      target_cells_in_child <- cellFromXY(cropped_brick_of_nc, target_XYs_parent)

      timeseries_per_cell <- t(raster::extract(cropped_brick_of_nc, target_cells_in_child))

      colnames(timeseries_per_cell) <- sprintf("X%i",target_cell_indices)
      rownames(timeseries_per_cell) <- NULL

      return(as.data.frame(timeseries_per_cell))
    })

    # above outputs data with a column per cell, and a row per layer in the brick (unit time)
    # need to unpivot/gather the data to have a cell column (with leading 'X' stripped off)
    gathered <- dplyr::bind_rows(data_series_for_all_files) %>%
      mutate(layer = row_number()) %>%
      gather(key = cell, value = mean, -layer) %>%
      mutate(cell = replace(cell, TRUE, substring(cell, 2))
      ) %>%
      dplyr::select(layer, cell, mean)

    return(gathered)
  }
}



#' Make time dataframe from time dimension of a list of NetCDF files
#'
#' Fields produced are: year, month, day, day_of_year, Austral_season_year, Austral_season_month, Austral_day_of_season
#'
#' @param nc_files List of NetCDF files
#'
#' @return A data frame with the described fields
#' @export
#'
#' @examples
make_time_dataframe_from_nc_files <- function(nc_files) {

  nc_files_time_string <- climatematchR::extract_and_combine_time_from_netcdf_files(nc_files)
  nc_files_time_lt <- as.POSIXlt(nc_files_time_string)

  time_df <- data.frame(
    # date = format(nc_files_time_lt, "%Y-%m-%d"),  # makes factors?
    date = as.Date(nc_files_time_lt),
    year=NA, month=NA, day=NA, day_of_year=NA,
    stringsAsFactors = FALSE
  )

  # http://en.wikipedia.org/wiki/Leap_year
  is_leapyear <- function(year) { return(((year %% 4 == 0) & (year %% 100 != 0)) | (year %% 400 == 0)) }

  # account for POSIXlt offsets
  time_df$year  = nc_files_time_lt$year + 1900
  time_df$month = nc_files_time_lt$mon + 1
  time_df$day   = nc_files_time_lt$mday
  time_df$day_of_year = nc_files_time_lt$yday + 1

  time_df$Austral_season_year <- case_when(
    (time_df$month) %in% 1:6  ~ time_df$year - 1,
    (time_df$month) %in% 7:12 ~ time_df$year
  )
  time_df$Austral_season_month <- case_when(
    (time_df$month) %in% 1:6  ~ time_df$month + 6,
    (time_df$month) %in% 7:12 ~ time_df$month - 6
  )
  time_df$Austral_day_of_season <- case_when(
    time_df$month > 6 & is_leapyear(time_df$year)==FALSE ~ c(time_df$day_of_year - 181),
    time_df$month > 6 & is_leapyear(time_df$year)==TRUE  ~ c(time_df$day_of_year - 182),
    time_df$month < 7 ~ c(time_df$day_of_year + 184) # (Jul=31 + Aug=31 + Sep=30 + Oct=31 + Nov=30 + Dec=31)
  )

  time_df$harvest_year <- time_df$Austral_season_year + 1

  time_df$vintage_years_start <- time_df$Austral_season_year
  time_df$vintage_years_end <- time_df$harvest_year

  integer_cols <- c("year", "month", "day", "day_of_year",
                    "Austral_season_year", "Austral_season_month", "Austral_day_of_season",
                    "harvest_year", "vintage_years_start", "vintage_years_end")

  time_df[integer_cols] <- lapply(time_df[integer_cols], function (x) { as.integer(x) })

  return(as.data.frame(time_df, stringsAsFactors = FALSE))
}



#' Extract and combine time values from multiple netcdf files.
#'
#' Time handling in any file format is constanly a pain. It can be described
#' in a range of different ways, relative to a range of different starting points.
#' For example, "Minutes since 1800-01-01 00:00:00", or "Seconds since 1960-01-01 00:00:00".
#' This tool is a wrapper around RNetCDF tools that extract and transform the time
#' representation in netcdf files into a useable, standard reference, human readable format.
#'
#' @param nc_files_list A list of files you wish to combine together, but have independently defined "time" variables.
#'
#' @return Returns a vector of character strings in the form of "YYYY-MM-DD hh:mm:ss".
#' This can then be used by POSIXct or POSIXlt to get date or time class objects.
#' @export
#'
#' @examples
#' files <- dir(path = "/path/to/nc/files/", pattern = "*.nc", full.names = TRUE)
#' ts <- extract_and_combine_time_from_netcdf_files(files)
extract_and_combine_time_from_netcdf_files <- function(nc_files_list){
  if(!requireNamespace("RNetCDF")){stop("RNetCDF package required.")}
  out_time_POSIXct <- unlist(lapply(nc_files_list, function(target_file){
    tmp_nc <- RNetCDF::open.nc(target_file)
    tmp_time_unitstring <- RNetCDF::att.get.nc(ncfile = tmp_nc, variable = "time", attribute = "units")
    tmp_time_raw <- RNetCDF::var.get.nc(ncfile = tmp_nc, variable = "time")
    RNetCDF::close.nc(tmp_nc)
    tmp_time_string <- RNetCDF::utcal.nc(unitstring = tmp_time_unitstring, value = tmp_time_raw, type = "s")
  })) #close unlist
  return(out_time_POSIXct)
}



#' Prepare a data table for processing,  from the time dataframe, tmax, tmin and rain data to be
#'
#' @param time_df Data frame with time series data, produced from `make_time_dataframe_from_brick`
#' @param tmax_data Spatial cell extract for Tmax data, produced from `cell_extract_spatial`
#' @param tmin_data Spatial cell extract for Tmin data, produced from `cell_extract_spatial`
#' @param rain_data Spatial cell extract for rainfall data, produced from `cell_extract_spatial`
#'
#' @return A data frame based on \code{time_df} with the Tmax, Tmin, and rainfall data
#' @export
#'
#' @examples
build_data_tbl <- function(time_df, tmax_data, tmin_data, rain_data, convert_temps_from_K_to_C = FALSE) {
  # Kelvin / Celsius correction
  temp_offset <- ifelse(convert_temps_from_K_to_C, 273.15, 0)

  tbl <- time_df[as.numeric(pull(tmax_data, layer)),]
  tbl$layer <- pull(tmax_data, layer)
  tbl$cell <- pull(tmax_data, cell)
  tbl$tasmax <- pull(tmax_data, mean) - temp_offset
  tbl$tasmin <- pull(tmin_data, mean) - temp_offset
  tbl$rain <- pull(rain_data, mean)

  return(tbl)
}

build_data_tbl_for_generic_area <- function(time_df, tmax_data, tmin_data, rain_data, convert_temps_from_K_to_C = FALSE) {
  # Kelvin / Celsius correction
  temp_offset <- ifelse(convert_temps_from_K_to_C, 273.15, 0)

  tbl <- time_df[as.numeric(pull(tmax_data, layer)),]
  tbl$layer <- pull(tmax_data, layer)
  tbl$object_ <- pull(tmax_data, object_)
  tbl$tasmax <- pull(tmax_data, mean) - temp_offset
  tbl$tasmin <- pull(tmin_data, mean) - temp_offset
  tbl$rain <- pull(rain_data, mean)

  return(tbl)
}



# Index Calculation Functions ---------------------------------------------

#' Calculate Growing Degree Days
#'
#' Calculates the GDD from daily temperatures Tmax and Tmin
#' with respect to a base temperature Tbase.
#' Averages the max and min temperatures and subtracts the
#' base temperature. Results with negative values are set
#' to zero (rather than having a 'negative' GDD).
#'
#' @param T_max Daily Maximum Temperature
#' @param T_min Daily Minimum Temperature
#' @param T_base Base Temperature
#'
#' @return The GDD for the given data per the formula: \code{GDD = (T_max - T_min) / 2 - T_base}
#' @export
#'
#' @examples
#' calc_GDD(24.12, 15.94, 10) #no need to specify upper limit
#' calc_GDD(24.12, 15.94, 10, 35) #but can specify an upper limit if desired
calc_GDD <- function(T_max, T_min, T_base, T_upper=1000) {
  GDD <- ((T_max + T_min) / 2) - T_base
  GDD[GDD < 0 & GDD > T_upper] <- 0
  return(GDD)
}



#' Calculate Weighted Growing Degree Days
#'
#' Calculates the cumulative weighted GDD where:
#' \itemize{
#'   \item{\code{GDD_base_budb} is used when cumulative GDD (x) meets: \code{0 < x < params[["bracket_budb_leaf"]]}}
#'   \item{\code{GDD_base_leaf} is used when cumulative GDD (x) meets: \code{params[["bracket_budb_leaf"]] < x < params[["bracket_leaf_pick"]]}}
#'   \item{\code{GDD_base_10} is used when cumulative GDD (x) meets: \code{x > params[["bracket_leaf_pick"]]}}
#' }
#'
#' @param GDD_base_budb Vector of GDD values with \code{T_base} as the budbreak base temperature
#' @param GDD_base_leaf Vector of GDD values with \code{T_base} as the leaf appearance temperature
#' @param GDD_base_10  Vector of GDD values with \code{T_base} set to 10 degrees C
#'
#' @return Vector of weighted GDD values
#' @export
#'
#' @examples
#' calc_GDD_weighted(GDD_base_budb, GDD_base_leaf, GDD_base_10, params)
calc_GDD_weighted <- function(T_max, Tmin, T_budb=4, T_leaf=7, T_vera=10, T_upper=1000, GDD_budb_threshold=350, GDD_leaf_threshold=1000, GDD_harvest_threshold=1300){
  GDD_base_budb <- calc_GDD(T_max, T_min, T_base = T_budb, T_upper = T_upper)
  GDD_base_leaf <- calc_GDD(T_max, T_min, T_base = T_leaf, T_upper = T_upper)
  GDD_base_vera <- calc_GDD(T_max, T_min, T_base = T_vera, T_upper = T_upper)

  # calculated each of the three periods p1, p2, p3 separately
  # and create output vector by combining those

  # period 1: from pre bud-break until cumulative GDD threshold
  # determine index at which threshold is crossed
  # result will use from beginning to this index
  # if threshold is not crossed set p1 to entire vector length

  p1 <- cumsum(GDD_base_budb)
  p1_end_ind <- which(p1 > GDD_budb_threshold)[1]
  if (is.na(p1_end_ind)) p1_end_ind <- length(GDD_base_budb)
  p1_end <- p1[p1_end_ind]

  # if threshold was not crossed, then skip next periods
  if (p1_end_ind == length(GDD_base_budb)) {
    # TODO: this case not tested yet
    p2 <- c()
    p3 <- c()
    p2_end_ind <- 0  # so that p2[1:p2_end_ind] later is NULL
  } else {
    # period 2:
    # calculate cumsum from the next element after the end of the first period
    # use the p1_end value as the fixed value to add to the cumsum (which starts at zero)
    p2 <- p1_end + cumsum(GDD_base_leaf[(p1_end_ind + 1):length(GDD_base_leaf)])
    p2_end_ind <- which(p2 > GDD_leaf_threshold)[1]
    if (is.na(p2_end_ind)) p2_end_ind <- length(GDD_base_leaf)
    p2_end <- p2[p2_end_ind]

    # if threshold not crossed, then skip next period
    if (p2_end_ind == length(GDD_base_leaf)) {
      p3 <- c()
    } else {
      p3 <- p2_end + cumsum(GDD_base_vera[(p1_end_ind + p2_end_ind + 1):length(GDD_base_vera)])
    }
  }

  # create vector using the calculated bounds for the partical vectors
  GDD_weighted <- c(
    p1[1:p1_end_ind],
    p2[1:p2_end_ind],
    p3)

  # because we are creating the GDD_weighted vector by manually joining vectors
  # we have to ensure it is the same length as corresponding time series
  # it may be created too long
  if(length(GDD_weighted) > length(GDD_base_vera)) {
    GDD_weighted <- GDD_weighted[1:length(GDD_base_vera)]
  }

  return(GDD_weighted)
}



#' Calculate significant event Excess Heat Index
#'
#' \code{EHI_sig = (Avg DMT over spread of days) - DMT_pc_thresh} as per Nairn 2013.
#'
#' @param temps Vector of daily mean temperatures to process
#' @param DMT_pc_thresh Threshold of daily temperature for the desired climate reference period (calculated using all days of the year)
#' @param sig_spread Length of period to average for significance value
#'
#' @return Vector with EHI_sig for given temperatures, padded with NA values at the start to maintain the same length as the input vector
#' @export
#'
#' @examples
#' t <- c(20.48, 17.81, 19.06, 16.40, 19.34, 20.79, 19.82, 16.28, 18.25, 19.56)
#' DMT_95_pctile <- 19.2
#' calc_EHI_sig(t, DMT_95_pctile)
calc_EHI_sig <- function(temps, DMT_pc_thresh, sig_spread = 3) {
  zoo::rollapply(temps, width = sig_spread, FUN = function(x) { sum(x)/sig_spread }, align = 'right', fill = NA) - DMT_pc_thresh
}



#' Calculate acclimatisation Excess Heat Index
#'
#' @param accl_length Length of acclimatisation period
#' @inheritParams calc_EHI_sig
#'
#' @return Vector with EHI_accl for given temperatures, padded with NA values at the start to maintain the same length as the input vector
#' @export
#'
#' @examples
#' t <- rnorm(n = 80, mean = 20)
#' calc_EHI_accl(t, 30, 3)
calc_EHI_accl <- function(temps, accl_length = 30, sig_spread = 3) {
  # figures to average for acclimatisation period
  width_ <- list( -(accl_length+sig_spread-1) : -sig_spread )

  # (first 3 days)
  # days: i, i-1, i-2
  part1 <- zoo::rollapply(temps, width = sig_spread, FUN = function(x) { sum(x)/sig_spread }, align = 'right', fill = NA)

  # (next 30 days after that)
  #: i-3, i-4, ..., i-32
  part2 <- zoo::rollapply(temps, width = width_, FUN = function(x) { sum(x)/accl_length }, align = 'right', fill = NA)

  part1 - part2
}



#' Calculate Excess Heat Factor from indicators for Excess Heat and Heat Stress
#'
#' @param EHI_sig Significant event EHI from \code{calc_EHI_sig}
#' @param EHI_accl Acclimatisation EHI from \code{calc_EHI_accl}
#'
#' @return Vector with EHF values for given temperatures, padded with NA values at the start to maintain the same length as the input vector
#' @export
#'
#' @examples
#' t <- rnorm(n = 80, mean = 20)
#' DMT_95_pctile <- 19.2
#' EHI_sig <- calc_EHI_sig(t, DMT_95_pctile)
#' EHI_accl <- calc_EHI_accl(t, 30, 3)
#' calc_EHF(EHI_sig, EHI_accl)
calc_EHF <- function(EHI_sig, EHI_accl) {
  # EHF = EHI_sign * max(1, EHI_accl)
  # note: can't use "max" function as it aggregates over the group
  EHI_sig * ifelse(EHI_accl < 1, 1, EHI_accl)
}


#' Is Austral Season Month October to April (Inclusive)
#'
#' Determine if an Austral season month is between Oct and Apr inclusive
#'
#' @param Austral_season_month Number of the month in the Austral season calendar (where 1 is July, through to 12 is June)
#'
#' @return TRUE or FALSE; if the supplied month is within the target range
#' @export
#'
is_Oct_to_Apr <- function(Austral_season_month) {
  Austral_season_month >= 4 & Austral_season_month <= 10
}



#' Is Austral Season Month March to April (Inclusive)
#'
#' Determine if an Austral season month is between Mar and Apr inclusive
#'
#' @param Austral_season_month Number of the month in the Austral season calendar (where 1 is July, through to 12 is June)
#'
#' @return TRUE or FALSE; if the supplied month is within the target range
#' @export
#'
is_Mar_to_Apr <- function(Austral_season_month) {
  Austral_season_month >= 9 & Austral_season_month <= 10
}



#' Is Austral Season Month September to April (Inclusive)
#'
#' Determine if an Austral season month is between Sep and Apr inclusive
#'
#' @param Austral_season_month Number of the month in the Austral season calendar (where 1 is July, through to 12 is June)
#'
#' @return TRUE or FALSE; if the supplied month is within the target range
#' @export
#'
is_Sep_to_Apr <- function(Austral_season_month) {
  Austral_season_month >= 3 & Austral_season_month <= 10
}



#' Calculate accumulated heat
#'
#' For vector of GDD values and a data frame containing associated date information,
#' calculate the accumulated heat (cumulative sum of GDD values) for the
#'
#' @param df Data frame with date information
#' @param GDD_values Vector of GDD values from the data frame to process
#' @param from_Austral_month Number of the month in the Austral season (1 = Jul ... 12 = Jun) to start calculating from
#' @param to_Austral_month Number of the month in the Austral season (1 = Jul ... 12 = Jun) to stop calculating from, defaults to 12
#'
#' @return Vector of calculated results, padded with NA values for dates that are not in the range,
#' so that length of vector is the same as the length of the vector of GDD values
#' @export
#'
#' @examples
#' data <- data.frame(date = ..., Austral_season_month = ..., GDD_values = c(...))
#' calc_accumulated_heat(data, data$GDD_Values, from_Austral_month = 4, to_Austral_month = 10)
calc_accumulated_heat <- function(df, GDD_values, from_Austral_month, to_Austral_month = 12) {
  # which days are within the from/to range?
  x_inds <- which(df$Austral_season_month >= from_Austral_month & df$Austral_season_month <= to_Austral_month)

  # insert NA for days prior the start of 'from month'
  # cumsum of days of interest
  # insert NA for days after the end of 'to month'
  c(
    rep(NA, (x_inds[1]-1) ),
    cumsum(GDD_values[x_inds]),
    rep(NA, (length(GDD_values) - x_inds[length(x_inds)]) )
  )
}


#' Find last date in series that value was under a particular threshold
#'
#' @param df_dates Vector of dates from data frame
#' @param df_values Vector of values from data frame
#' @param threshold Vector of values from data frame
#'
#' @return The item in the date vector which corresponds to the item in the values vector first being below the threshold value.
#' If the value vector never drops below the threshold, then NA is returned.
#' @export
#'
#' @examples
#' df <- data.frame( date = seq(as.Date("1961-07-01"), as.Date("1961-07-31"), "days"),
#'                   vals = seq(from = 31, to = 1, by = 1) )
#' find_last_date_under(df$date, df$vals, 4)
find_last_date_under <- function(df_dates, df_values, threshold) {
  last_inst <- tail(which(df_values < threshold), 1)

  if (length(last_inst) > 0) {
    df_dates[last_inst]
  } else {
    NA
  }
}



#' Identify Date that Value of Vectors Exceeds a Threshold
#'
#' For a given data frame and corresponding vector of cumulatively summed GDD values (indexed to the same dates),
#' calculate the date on which a threshold value is passed
#'
#' @param df_dates The date vector from a data frame
#' @param df_values The values vector from a data frame which to check against the threshold
#' @param threshold Threshold value to scan for in `df_values`
#'
#' @return Date that `threshold` is passed, or `NA` if it is not reached
#' @export
#'
#' @examples
#' df <- data.frame(
#'   date = seq(as.Date("1961-07-01"), as.Date("1961-07-31"), "days"),
#'   value = c(1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9)
#'   )
#' date_vector_exceeds_threshold(df$date, df$value, 5)
date_vector_exceeds_threshold <- function(df_dates, df_values, threshold) {
  df_dates[which(df_values > threshold)[1]]
}



#' Calculate Downy Mildew Pressure Index
#'
#' Calculates the Downy Mildew Pressure index for a set of daily
#' rainfall and temperature min/max data. This is true if:
#' there is a daily rainfall of > 10mm and the
#' temperature is > 10 degrees C for more than 10 hours.
#'
#' Approximates temperature curve to be from \code{T_min} at start of day,
#' rising linearly to linear rising to \code{T_max} at middle of day,
#' and then falling linearly to \code{T_min} againt at the end of the day.
#'
#' The threshold value defaults to 10, but can be changed if necessary.
#'
#' @param rain Daily rainfall
#' @param T_max Maximum daily temperature
#' @param T_min Minimum daily temperature
#' @param T_threshold Tempature above which must be sustained during the day for 10 hours
#'
#' @return A logical vector specifying if the index was calculated to be TRUE of FALSE for the given data points
#' @export
#'
#' @examples
#' rain <- rnorm(n = 30, mean = 9, sd = 4)
#' tmax <- rnorm(n = 30, mean = 12, sd = 4)
#' tmin <- rnorm(n = 30, mean = 8, sd = 4)
#' downy_mildew_pressure_index(rain, tmax, tmin)
downy_mildew_pressure_index <- function(rain, T_max, T_min, T_threshold = 10) {
  # "high-school trigonometry!"
  hours_gt_threshold <- (24 * (T_max - T_threshold)) / (T_max - T_min)

  ifelse(rain >= 10 & hours_gt_threshold >= 10, TRUE, FALSE)
}




# Data Processing Functions -----------------------------------------------

#' Calculate agroclimatic grape indices from the given data table with the named param list
#'
#' @param data_table Data to process
#' @param params
#'
#' @return A data frame with the full list of calculated indices and statistics grouped by a generic area object
#' @export
#'
#' @examples
#' params <- c(base_budb = 3.5,
#'             base_leaf = 7.1,
#'             bracket_budb_leaf = 350,
#'             bracket_leaf_pick = 1000,
#'             DMT_threshold_pctile = 95,
#'             climatology_start = 1962,
#'             climatology_end = 1990,
#'             min_duration = 3,
#'             downy_mildew_threshold_temp = 10)
#'                                                                                                 )
calculate_stats_for_generic_area <- function(data_table, params) {

  variables_with_stats <- data_table %>%

    # pass over data when grouped by area, year, month
    do(log_calc_stats_by_area_year_month(.)) %>%
    group_by(object_, Austral_season_year, Austral_season_month) %>%
    arrange(object_, Austral_season_year, Austral_season_month) %>%
    do(calc_stats_by_area_year_month(., params)) %>%
    ungroup() %>%

    # pass over data when grouped by area, calendar year
    do(log_calc_stats_by_area_calendar_year(.)) %>%
    group_by(object_, year) %>%
    arrange(year) %>%
    do(calc_stats_by_area_calendar_year(., params)) %>%
    ungroup() %>%

    # pass over data when grouped by area, year
    do(log_calc_stats_by_area_year(.)) %>%
    group_by(object_, Austral_season_year) %>%
    arrange(Austral_day_of_season) %>%
    do(calc_stats_by_area_year(., params)) %>%
    ungroup() %>%

    # pass over data when grouped by area
    do(log_calc_stats_by_area(.)) %>%
    group_by(object_) %>%
    arrange(object_) %>%
    do(calc_stats_by_area(., params)) %>%
    ungroup()

  # calculate seasonal heatwave
  # variables_with_stats <- heatwave_detect_seasonal(variables_with_stats, params)
}



#' Calculate agroclimatic grape indices from the given data table for the shapes and params specified
#'
#' @param shapes List of shapes
#' @inheritParams calculate_stats_for_generic_area
#'
#' @return A data frame with the full list of calculated indices and statistics grouped by wine region
#' @export
#'
#' @examples
calculate_stats_for_wine_regions <- function(shapes, data_table, params) {

  # calculate the stats
  variables_with_stats <- calculate_stats_for_generic_area(data_table, params)

  # retrieve Region information
  tbl_regions <- get_region_data(shapes)

  # prepare final dataset by joining region lookup to stats data table on object_ id
  prep <- inner_join(tbl_regions, variables_with_stats, c("object_" = "object_"))

  # final output with columns renamed/reordered/removed
  rearrange_stats_output(prep)
}



#' Calculate agroclimatic grape indices from the given data table for the shapes and params specified
#'
#' @inheritParams calculate_stats_for_generic_area
#'
#' @return A data frame with the full list of calculated indices and statistics grouped by cells
#' @export
#'
#' @examples
calculate_stats_for_cells <- function(shapes, data_table, params) {
  # rename column to fit generic functions
  data_table <- data_table %>%
    dplyr::rename(object_ = cell)

  # calculate the stats
  variables_with_stats <- calculate_stats_for_generic_area(data_table, params)

  # final output with columns renamed/reordered/removed
  variables_with_stats <- rearrange_stats_output(variables_with_stats)

  # unrename column to previous name
  variables_with_stats <- variables_with_stats %>%
    dplyr::rename(cell = object_)
}



#' Rearrange columns of final calculated stats data frame to group by topical groups
#'
#' @param stats_prep A complete set of calculated stats; note: all columns must be present otherwise error will occur
#'
#' @return Data frame with stats ordered by indice groups
#'
rearrange_stats_output <- function(stats_prep) {
  stats_prep %>%
    # rename(region_id = object_,
    #        region_name = label
    # ) %>%
    dplyr::select(-layer,
           -rain_calendar_year_May_to_Sep
    ) %>%
    dplyr::select(
                  object_,
                  date,

                  year,
                  month,
                  day,
                  day_of_year,
                  Austral_season_year,
                  Austral_season_month,
                  Austral_day_of_season,
                  harvest_year,
                  vintage_years_start,
                  vintage_years_end,

                  tasmax,
                  tasmin,
                  rain,

                  ## (1)

                  mean_temperature,

                  tasmax_month,
                  tasmin_month,

                  GST_mean_temp_Oct_to_Apr,
                  GST_mean_temp_Sep_to_Apr,
                  GST_mean_temp_Jan,

                  GDD_base_00,
                  GDD_base_10,
                  GDD_base_budb,
                  GDD_base_leaf,
                  GDD_season_weighted,

                  GDD_season_base10_from_Jul,
                  GDD_season_base10_from_Jul_date_gt_1100,
                  GDD_season_base10_from_Jul_date_gt_1300,
                  GDD_season_base10_from_Jul_date_gt_1500,
                  GDD_season_base10_from_Aug,
                  GDD_season_base10_from_Aug_date_gt_1100,
                  GDD_season_base10_from_Aug_date_gt_1300,
                  GDD_season_base10_from_Aug_date_gt_1500,
                  GDD_season_base10_from_Sep,
                  GDD_season_base10_from_Sep_date_gt_1100,
                  GDD_season_base10_from_Sep_date_gt_1300,
                  GDD_season_base10_from_Sep_date_gt_1500,
                  GDD_season_base10_from_Oct,
                  GDD_season_base10_from_Oct_date_gt_1100,
                  GDD_season_base10_from_Oct_date_gt_1300,
                  GDD_season_base10_from_Oct_date_gt_1500,

                   ## (2)

                  tasmax_pc_90,
                  tasmax_pc_95,
                  tasmax_pc_99,
                  tasmax_days_gt_30,
                  tasmax_days_gt_35,
                  tasmax_days_gt_40,

                  EHF_DMT_threshold,

                  HW_EHI_sig,
                  HW_EHI_accl,
                  HW_EHF_value,
                  HW_EHF_event,

                  HW_seasonal_event,
                  HW_seasonal_event_num,
                  HW_seasonal_cumulative_intensity,

                  ## (3)

                  tasmin_pc_10,
                  tasmin_pc_05,
                  tasmin_pc_01,
                  last_date_neg2_degC,
                  last_date_zero_degC,
                  last_date_pos2_degC,
                  diurnal_range,

                  tasmin_lt_neg1,
                  tasmin_days_in_month_lt_neg1,

                  ## (4)

                  downy_mildew_pressure_index,

                  ## (5)

                  rain_month,

                  rain_pre_season_May_to_Sep,
                  rain_season_JJ,
                  rain_season_OA,
                  rain_season_JJ_cumulative,
                  rain_season_OA_cumulative,

                  rain_pc_99_type_5,
                  rain_pc_99_type_7,

                  rain_gt_1mm,
                  rain_days_Mar_to_Apr,
                  rain_days_Mar_to_Apr_date_gt_20,

                  # captures unspecified columns
                  everything()
    )

}



#' Calculate agroclimatic grape indices and statistics (by area only)
#'
#' EHF calculations need to persist across years for the same regions, as they are calculated using rolling functions
#' and if grouped by region and year, then the start of each year will not yet be calculated
#'
#' @param x Data frame to process
#' @param params Named list of common parameters for calculations
#'
#' @return The input data frame, but with extra fields calculated for this grouping
#'
calc_stats_by_area <- function(x, params) {

  # get bounds from params
  clim_start <- as.Date(paste(params[["climatology_start"]], "01", "01", sep="-"))
  clim_end <- as.Date(paste(params[["climatology_end"]], "12", "31", sep="-"))
  percentile <- params[["DMT_threshold_pctile"]]

  # calc DMT percentile from days in the climatology period
  clim_temps <- x$mean_temperature[which(x$date >= clim_start & x$date <= clim_end)]
  EHF_DMT_threshold <- stats::quantile(clim_temps, prob=(percentile/100), na.rm=TRUE)[[1]]

  # EHF components
  HW_EHI_sig <- calc_EHI_sig(x$mean_temperature, EHF_DMT_threshold)
  HW_EHI_accl <- calc_EHI_accl(x$mean_temperature)
  HW_EHF_value <- calc_EHF(HW_EHI_sig, HW_EHI_accl)
  EHF_gt_zero <- ifelse(is.na(HW_EHF_value), FALSE, HW_EHF_value > 0)  # set NA to false

  tmp_df = data.frame(
    x,
    EHF_DMT_threshold,

    HW_EHI_sig,
    HW_EHI_accl,
    HW_EHF_value,
    HW_EHF_event = heatwave_detect_ehf(EHF_gt_zero, params)
  )

  return(tmp_df)
}



#' Calculate agroclimatic grape indices and statistics (by area, season year)
#'
#' @param x Data frame to process
#' @param params Named list of common parameters for calculations
#'
#' @return The input data frame, but with extra fields calculated for this grouping
#'
calc_stats_by_area_year <- function(x, params) {

  # constants
  base_budb <- params[["base_budb"]]
  base_leaf <- params[["base_leaf"]]
  bracket_budb_leaf <- params[["bracket_budb_leaf"]]
  bracket_leaf_pick <- params[["bracket_leaf_pick"]]
  downy_mildew_threshold_temp <- params[["downy_mildew_threshold_temp"]]


  ##
  ## Calculate relevant climate statistics for each group
  ##

  ## (0) General

  mean_temperature <- (x$tasmax + x$tasmin)/2


  ## (1) Temperature Indices

  # Growing Season Temperature

  GST_mean_temp_Oct_to_Apr <- mean(mean_temperature[which(is_Oct_to_Apr(x$Austral_season_month))])
  GST_mean_temp_Sep_to_Apr <- mean(mean_temperature[which(is_Sep_to_Apr(x$Austral_season_month))])
  GST_mean_temp_Jan <- mean(mean_temperature[which(x$Austral_season_month == 7)])

  # Daily Growing Degree Day Figures

  GDD_base_00 <- calc_GDD(x$tasmax, x$tasmin, 0)
  GDD_base_10 <- calc_GDD(x$tasmax, x$tasmin, 10)
  GDD_base_budb <- calc_GDD(x$tasmax, x$tasmin, base_budb)
  GDD_base_leaf <- calc_GDD(x$tasmax, x$tasmin, base_leaf)

  # Accumulated Heat / Season Growing Degree Day Tallies

  GDD_season_weighted <- calc_GDD_weighted(GDD_base_budb, GDD_base_leaf, GDD_base_10, params)

  GDD_season_base10_from_Jul <- calc_accumulated_heat(x, GDD_base_10, from_Austral_month = 1)
  GDD_season_base10_from_Aug <- calc_accumulated_heat(x, GDD_base_10, from_Austral_month = 2)
  GDD_season_base10_from_Sep <- calc_accumulated_heat(x, GDD_base_10, from_Austral_month = 3)
  GDD_season_base10_from_Oct <- calc_accumulated_heat(x, GDD_base_10, from_Austral_month = 4)


  ## (5) Rainfall

  rain_season_JJ <- sum(x$rain)
  rain_season_OA <- sum(x$rain[which(is_Oct_to_Apr(x$Austral_season_month))])

  # is_Oct_to_Apr returns a bool, multiplying casts to numeric (0 for FALSE, 1 for TRUE)
  rain_season_JJ_cumulative <- cumsum(x$rain)
  rain_season_OA_cumulative <- cumsum(x$rain * is_Oct_to_Apr(x$Austral_season_month))


  rain_gt_1mm <- x$rain > 1
  rain_days_Mar_to_Apr <- cumsum(rain_gt_1mm * is_Mar_to_Apr(x$Austral_season_month))



  ## Final Output

  tmp_df = data.frame(
    # time series and raw data
    x,

    # calculated fields
    mean_temperature,

    GST_mean_temp_Oct_to_Apr,
    GST_mean_temp_Sep_to_Apr,
    GST_mean_temp_Jan,

    GDD_base_00,
    GDD_base_10,
    GDD_base_budb,
    GDD_base_leaf,

    GDD_season_weighted,

    GDD_season_base10_from_Jul,
    GDD_season_base10_from_Jul_date_gt_1100 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Jul, 1100),
    GDD_season_base10_from_Jul_date_gt_1300 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Jul, 1300),
    GDD_season_base10_from_Jul_date_gt_1500 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Jul, 1500),

    GDD_season_base10_from_Aug,
    GDD_season_base10_from_Aug_date_gt_1100 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Aug, 1100),
    GDD_season_base10_from_Aug_date_gt_1300 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Aug, 1300),
    GDD_season_base10_from_Aug_date_gt_1500 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Aug, 1500),

    GDD_season_base10_from_Sep,
    GDD_season_base10_from_Sep_date_gt_1100 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Sep, 1100),
    GDD_season_base10_from_Sep_date_gt_1300 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Sep, 1300),
    GDD_season_base10_from_Sep_date_gt_1500 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Sep, 1500),

    GDD_season_base10_from_Oct,
    GDD_season_base10_from_Oct_date_gt_1100 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Oct, 1100),
    GDD_season_base10_from_Oct_date_gt_1300 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Oct, 1300),
    GDD_season_base10_from_Oct_date_gt_1500 = date_vector_exceeds_threshold(x$date, GDD_season_base10_from_Oct, 1500),


    # (2) Heat Extremes

    tasmax_pc_90 = stats::quantile(x$tasmax, prob=0.90, na.rm=TRUE)[[1]],
    tasmax_pc_95 = stats::quantile(x$tasmax, prob=0.95, na.rm=TRUE)[[1]],
    tasmax_pc_99 = stats::quantile(x$tasmax, prob=0.99, na.rm=TRUE)[[1]],

    tasmax_days_gt_30 = length(which(x$tasmax > 30)),
    tasmax_days_gt_35 = length(which(x$tasmax > 35)),
    tasmax_days_gt_40 = length(which(x$tasmax > 40)),

    # seaonal heatwave populated externally
    HW_seasonal_event = NA,
    HW_seasonal_event_num = NA,
    HW_seasonal_cumulative_intensity = NA,

    # EHF calculated later


    ## (3) Frost and Cold Extremes

    tasmin_pc_10 = stats::quantile(x$tasmin, prob=0.10, na.rm=TRUE)[[1]],
    tasmin_pc_05 = stats::quantile(x$tasmin, prob=0.05, na.rm=TRUE)[[1]],
    tasmin_pc_01 = stats::quantile(x$tasmin, prob=0.01, na.rm=TRUE)[[1]],

    last_date_neg2_degC = find_last_date_under(x$date, x$tasmin, -2),
    last_date_zero_degC = find_last_date_under(x$date, x$tasmin, 0),
    last_date_pos2_degC = find_last_date_under(x$date, x$tasmin, 2),

    diurnal_range = (x$tasmax - x$tasmin),


    ## (4) Weather conducive to disease

    downy_mildew_pressure_index = downy_mildew_pressure_index(x$rain, x$tasmax, x$tasmin, downy_mildew_threshold_temp),


    ## (5) Rainfall

    rain_pc_99_type_5 = stats::quantile(x$rain, prob=0.99, na.rm=TRUE, type=5)[[1]],
    rain_pc_99_type_7 = stats::quantile(x$rain, prob=0.99, na.rm=TRUE, type=7)[[1]],

    rain_season_JJ,
    rain_season_OA,
    rain_season_JJ_cumulative,
    rain_season_OA_cumulative,

    # southern hemisphere specific:
    # the year component of the first date of the growning season year is the same as the the calendar year
    # so the total rainfall for the pre-season of this growing season is the value for preseason rainfall on the the 1st july
    rain_pre_season_May_to_Sep = x$rain_calendar_year_May_to_Sep[1],

    rain_gt_1mm,
    rain_days_Mar_to_Apr,
    rain_days_Mar_to_Apr_date_gt_20 = date_vector_exceeds_threshold(x$date, rain_days_Mar_to_Apr, 20)
  )

  return(tmp_df)
}



#' Calculate agroclimatic grape indices and statistics (by area, calendar year)
#'
#' @param x Data frame to process
#' @param params Named list of common parameters for calculations
#'
#' @return The input data frame, but with extra fields calculated for this grouping
#'
calc_stats_by_area_calendar_year <- function(x, params) {

  # a bool vector
  # - for cumsum, multiplying by a bool casts to numeric (0 for FALSE, 1 for TRUE)
  is_May_to_Sep_calendar_year <- which(x$month >= 5 & x$month <= 9)

  rain_calendar_year_May_to_Sep <- sum(x$rain[is_May_to_Sep_calendar_year])
  # rain_pre_season_May_to_Sep_cumulative <- cumsum(x$rain * is_May_to_Sep_calendar_year)

  data.frame(
    x,

    rain_calendar_year_May_to_Sep
    # rain_pre_season_May_to_Sep_cumulative
  )
}



#' Calculate agroclimatic grape indices and statistics (by area, year, month)
#'
#' @param x Data frame to process
#' @param params Named list of common parameters for calculations
#'
#' @return The input data frame, but with extra fields calculated for this grouping
#'
calc_stats_by_area_year_month <- function(x, params) {

  tasmin_lt_neg1 = x$tasmin < -1

  tmp_df = data.frame(
    # previous data
    x,

    tasmax_month = max(x$tasmax),
    tasmin_month = min(x$tasmin),

    rain_month = sum(x$rain),

    tasmin_lt_neg1,
    tasmin_days_in_month_lt_neg1 = sum(tasmin_lt_neg1)
  )
}



#' Detect seasonal heatwaves using R Marine Heatwaves package
#'
#' @param data The data set to search for seasonal heatwaves
#' @param params Named vector of common params, which must contain "DMT_threshold_pctile", "climatology_start", "climatology_end", "min_duration"
#'
#' @return The input data, but with the results of the RmarineHeatWaves added
#' @export
#'
heatwave_detect_seasonal <- function(data, params) {

  percentile <- params[["DMT_threshold_pctile"]]
  clim_start <- params[["climatology_start"]]
  clim_end <- params[["climatology_end"]]
  min_duration <- params[["min_duration"]]

  ## loop
  MHW_event_all <- NULL
  num_objects <- max(data$object_)

  # foreach area
  p <- progress_estimated(num_objects, 10)
  for (i in 1:num_objects) {
    p$tick()$print()

    # detect seasonal heatwave for each area
    ts_dat <- data %>%
      filter(object_ == i) %>%
      dplyr::select(object_, t = date, temp = mean_temperature) %>%
      make_whole(t, temp)

    mhw_result <- RmarineHeatWaves::detect(ts_dat,
                                           pctile = percentile,  # default: 90
                                           # window_half_width = 5,
                                           # smooth_percentile = TRUE,
                                           # smooth_percentile_width = 31,
                                           # clim_only = FALSE,
                                           min_duration = min_duration, # default: ,
                                           # join_across_gaps = TRUE,
                                           # max_gap = 2,
                                           # max_pad_length = 3,
                                           # cold_spells = TRUE,  # default: FALSE
                                           climatology_start = clim_start,
                                           climatology_end = clim_end)


    # maybe write these to file to use later?
    clim <- mhw_result$clim
    events <- mhw_result$event

    MHW_event_obj <- data.frame(
      t = clim$t,
      object_ = i,
      event = clim$event,
      event_no = clim$event_no,
      int_cum = NA
    )

    # populate cum intensity for each event
    num_events <- dim(events)[1]

    for (j in 1:num_events) {
      event <- events[j,]
      event_range <- event$index_start:event$index_stop
      MHW_event_obj$int_cum[event_range] <- event$int_cum
    }

    MHW_event_all <- rbind(MHW_event_all, MHW_event_obj)
  }

  # join back to data to populate MHW event detection results
  prep <- left_join(data, MHW_event_all, c("object_" = "object_", "date" = "t"))

  prep %>% mutate(HW_seasonal_event = replace(HW_seasonal_event, TRUE, event),
                  HW_seasonal_event_num = replace(HW_seasonal_event_num, TRUE, event_no),
                  HW_seasonal_cumulative_intensity = replace(HW_seasonal_cumulative_intensity, TRUE, int_cum)) %>%
    dplyr::select(-event, -event_no, -int_cum)
}



#' Detect Heatwaves from EHF Method Calculations
#'
#' @param EHF_vector Vector of logical (TRUE/FALSE) values corresponding to EHF > 0
#' @param min_duration Minimum number of days in a row with EHF > 0 that gets classified as a heatwave
#'
#' @return A vector with detected EHF heatwaves (fulfilling the duration criteria) set to TRUE, and isolated days set to FALSE
#' @export
#'
#' @examples
#' EHF_gt_zero <- c(FALSE, FALSE, FALSE, TRUE, FALSE, TRUE, TRUE, TRUE, FALSE, FALSE,
#'                  TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, FALSE, FALSE, FALSE)
#' heatwave_detect_ehf(EHF_gt_zero)
heatwave_detect_ehf <- function(EHF_vector, params) {

  # params
  min_duration <- params[["min_duration"]]

  v <- EHF_vector

  # set all NA values to false for rle (and prevent false hits)
  v[is.na(v)] <- FALSE

  # reduce vector to groups and add a cumulative index column
  EHF_rle <- rle(v)
  EHF_rle$cum_ind <- cumsum(EHF_rle$lengths)

  EHF_rle$detect <- rep(FALSE, length(EHF_rle$values))

  for (i in 1:length(EHF_rle$values)) {
    # set detect flag for EHF > 0 for consecutive values 3 or more values
    if (EHF_rle$values[i] == TRUE && EHF_rle$lengths[i] >= min_duration) {
      EHF_rle$detect[i] = TRUE
    }
  }

  # overwrite values with true detected instances and apply inverse
  EHF_rle$values <- EHF_rle$detect

  inverse.rle(EHF_rle)
}



#' Get Region Info from Shapes Data
#'
#' @param shapes List of shapes file
#'
#' @return A tibble with a row for each region, with the object_ id and label as columns
#' @export
#'
#' @examples
get_region_data <- function(shapes) {
  # get regions in order of their object_ id
  regions <- unlist(lapply(shapes, function(x) { x$label } ))

  # make tibble with object_ id and corresponding region label
  tbl_regions <- as_tibble(tibble::rowid_to_column(as.data.frame(regions), var ='object_'))
  colnames(tbl_regions) <- c('object_', 'label')
  tbl_regions
}



# Presentation ------------------------------------------------------------



create_day_of_season_to_month_day_legend <- function(wine_region_stats) {
  DOS2Date_lookup_tmp <- wine_region_stats %>%
    filter(year == 1991) %>%
    dplyr::select(Austral_day_of_season, month, day) %>%
    distinct()
  DOS2Date_lookup_tmp$month_day <- sprintf("%s - %0.2i", month.abb[DOS2Date_lookup_tmp$month], DOS2Date_lookup_tmp$day, DOS2Date_lookup_tmp$Austral_day_of_season)

  # only display these days
  pretty_days <- c(
    10, 20, 30,
    41, 51, 59,
    69, 79, 89,
    100, 110, 120,
    130, 140, 150,
    161, 171, 181,
    191, 201, 211,
    222, 232, 242,
    253, 263, 273,
    283, 293, 303,
    314, 324, 334,
    344, 354, 364
  )

  DOS2Date_lookup_tmp[pretty_days,]
}


calculate_mean_gdd_gt_1000_for_each_polygon <- function(variables_with_stats, from_year, to_year) {
  # summarise all cells into a single mean value for each polygon
  variables_with_stats %>%
    filter(Austral_season_year >= from_year & Austral_season_year <= to_year) %>%
    group_by(object_) %>%
    #dplyr::summarise(mean = mean(GDD_season_base10_OA_dos_gt_1000, na.rm=TRUE))
    dplyr::summarise(mean = mean(GDD_season_base10_JJ_dos_gt_1000, na.rm=TRUE))
}


#' Produce Data Summary for Market Insights
#'
#' @param variables_with_stats
#' @param from_year
#' @param to_year
#'
#' @return
#' @export
#'
#' @examples
produce_data_table_for_market_insights <- function(variables_with_stats, from_year, to_year) {
  # Produce yearly climate-related statistics for Market Insights for a given time period
  # Growing season rainfall, Mean January temperature, Growing degree days
  #

  # group by object_ / Region first, then period to calculate aggregate functions
  output <- variables_with_stats %>%
    group_by(object_, Region, vintage_years_start, vintage_years_end) %>%
    filter(vintage_years_start >= from_year & vintage_years_start <= to_year) %>%
    dplyr::summarise(
      `Growing Season Rainfall Jul-Jun` = mean(rain_season_JJ, na.rm=TRUE),
      `Growing Season Rainfall Oct-Apr` = mean(rain_season_OA, na.rm=TRUE),
      `Growing Degree Days Jul-Jun` = sum(GDD_base_10, na.rm=TRUE),
      `Growing Degree Days Oct-Apr` = sum(GDD_base_10[which(is_Oct_to_Apr(Austral_season_month))], na.rm=TRUE),
      `Mean January Temperature` = mean(MeanJanTemperature, na.rm=TRUE)
    ) %>%
    ungroup()

  # final output, removing internal object_ column and separate start/end year columns
  # adding year range column, re-ordering columns, and sorting by Region name
  output <- output %>%
    mutate(`Vintage Years` = paste(vintage_years_start, '–', vintage_years_end, sep="")) %>%
    dplyr::select(-object_) %>%
    arrange(Region)

  # reorder columns
  output <- output[c(1,9,2,3,4,5,6,7,8)]
}


#' Produce Market Insights Climatology Report
#'
#' @param market_insights_data as output from produce_data_table_for_market_insights
#' @param period1 Year range for first period to provide data for, e.g. 1961:1990
#' @param period2 Year range for second period to provide data for, e.g. 1991:2015
#'
#' @return
#' @export
#'
#' @examples
produce_climatology_for_market_insights <- function(market_insights_data, period1, period2) {
  # pretty format of period
  period_text <- function(period) {
    sprintf("%s–%s", period[1], period[length(period)])
  }

  # pretty format for the period this row is a part of
  # if year does not lie within either period, the output is NA
  which_period <- function(year, period1_, period2_) {
    period1_text <- period_text(period1_)
    period2_text <- period_text(period2_)
    ifelse(year %in% period1_, period1_text, ifelse(year %in% period2_, period2_text, NA))
  }

  # group by object_ / Region first, then period to calculate aggregate functions
  output <- market_insights_data %>%
    mutate(Period = which_period(vintage_years_start, period1, period2)) %>%
    dplyr::select(-vintage_years_start, -vintage_years_end, -`Vintage Years`) %>%
    group_by(Region, Period) %>%
    dplyr::summarise_all(mean) %>%
    ungroup()

  # exclude rows not matching a period (due to half year)
  output %>%
    filter(!is.na(Period))
}


write_market_insights_csv <- function (data_frame, file_name) {

  format_int <- function(figure) {
    formatC(figure, digits = 0, format = 'd')
  }

  format_temperature <- function(figure) {
    no_data_placeholder = '–'
    ifelse(is.na(figure), no_data_placeholder, formatC(figure, digits = 1, format = 'f'))
  }

  # # sapply?
  # test <- data.frame(
  #   output[1:2],
  #   sapply(output[3:6], format_rainfall, simplify = FALSE, USE.NAMES=TRUE),
  #   `Mean January Temperature` <- format_temperature(output$`Mean January Temperature`)
  # )
  # output has dots in place of spaces

  output <- data_frame[1:2]
  output[["Growing Season Rainfall Jul-Jun"]] <- format_int(data_frame[["Growing Season Rainfall Jul-Jun"]])
  output[["Growing Season Rainfall Oct-Apr"]] <- format_int(data_frame[["Growing Season Rainfall Oct-Apr"]])
  output[["Growing Degree Days Jul-Jun"]] <- format_int(data_frame[["Growing Degree Days Jul-Jun"]])
  output[["Growing Degree Days Oct-Apr"]] <- format_int(data_frame[["Growing Degree Days Oct-Apr"]])

  output[["Mean January Temperature"]] <- format_temperature(data_frame[["Mean January Temperature"]])

  write.csv(output, file_name, row.names = FALSE)
}


plot_WA <- function(data_table, xlims, ylims) {
  ggplotGrob(
    ggplot(data_table, aes(fill = mean, colour = mean)) +
      geom_sf(data=shp_Oz_boundaries, aes(fill=NULL, colour=NULL), fill="grey80", colour="grey60", lwd=0.1) +
      geom_sf(lwd=0.1, colour="grey90") +
      coord_sf(xlims, ylims, expand=FALSE, datum=NA) +
      theme_void() + theme(plot.margin = unit(c(0, 0, 0, 0), "null")) +
      # theme_minimal() + theme(axis.text.x=element_text(angle=90)) +
      scale_fill_gradientn(colours=ColourBlindFriendlyPalette[9:3], name="Date", breaks=DOS2Date_lookup$Austral_day_of_season, labels=DOS2Date_lookup$month_day) +
      guides(fill=FALSE)
  )
}


plot_SEA <- function(data_table, xlims, ylims) {
  ggplot(data_table, aes(fill = mean, colour = mean)) +
    geom_sf(data=shp_Oz_boundaries, aes(fill=NULL, colour=NULL), fill="grey80", colour="grey60", lwd=0.1) +
    geom_sf(lwd=0.1, colour="grey90") +
    coord_sf(xlims, ylims, expand=FALSE, datum=NA) +
    theme_void() +
    theme(plot.margin = unit(c(0, 0, 0, 0), "null"), legend.text=element_text(size=7), legend.text.align = 1) +
    scale_fill_gradientn(colours=ColourBlindFriendlyPalette[9:3], name="Date", breaks=DOS2Date_lookup$Austral_day_of_season, labels=DOS2Date_lookup$month_day) +
    annotate("rect", xmin=127, xmax=136, ymin=-36, ymax=-25, fill="white", colour="white") +
    annotation_custom(region_plot_Western_Australia, xmin=127, xmax=135, ymin=-43.6, ymax=-25)
}



# Utility functions -------------------------------------------------------


#' Create a data frame where each row is a model/domain/variable to extract
#'
#' Use expand.grid on a list of models and domains
#'
#' @param model_list Vector of models (corresponding to directory structure)
#' @param domain_list Vector of domains (corresponding to directory structure)
#' @param variables_list A list where the variables of interest a particular domain
#' are values for a named list with the domain prefixed by "domain_" (see example)
#'
#' @return
#' @export
#'
#' @examples
#' model_list <- c("ERA", "ACCESS1-0")
#' domain_list <- c("5km", "50km", "tas10km", "sea10km")#'
#' variables_list <- vector("list")
#' variables_list[["domain_5km"]] = c("tasmax_day", "tasmin_day")
#' variables_list[["domain_50km"]] = c("tasmax_day")
#' variables_list[["domain_sea10km"]] = c("tasmax_day")
#' variables_list[["domain_tas10km"]] = c("tasmax_day", "tasmin_day")
#'
#' expand_grid_to_target_list(model_list, domain_list, variables_list)
expand_grid_to_target_input_list <- function(model_list, domain_list, variables_list) {
  # model / domain grid
  md_grid <- expand.grid(model = model_list,
                         domain = domain_list,
                         stringsAsFactors = FALSE)

  # domain variable pairs
  dv_pairs <- reshape2::melt(data = variables_list, value.name = "variable")
  colnames(dv_pairs) <- c('variable', 'domain')
  dv_pairs$domain <- str_replace(dv_pairs$domain, 'domain_', '')

  inner_join(md_grid, dv_pairs, by = "domain")
}



#' Log a message with a timestamp
#'
#' @param msg Message to log
#'
#' @return \code{cat} a message to the screen with a timestamp with format %X
#' @export
#'
#' @examples
#' tlog("Start processing something...")
#' # ...
#' tlog("...done")
tlog <- function(msg) {
  cat(format(Sys.time(), "%X"), msg, "\n")
}


#' @name log_calc
#' @rdname log_calc
#'
#' @title Helper log function for calculate stats group
#'
#' @param x The data frame being processed, so that it can be returned unaltered
#'
#' @return \code{cat} a status message to console for particular group
NULL

#' @rdname log_calc
log_calc_stats_by_area <- function (x) {
  cat("\n\n/ Area /\n")
  x
}

#' @rdname log_calc
log_calc_stats_by_area_year <- function (x) {
  cat("\n\n/ Area / Season Year /\n")
  x
}

#' @rdname log_calc
log_calc_stats_by_area_year_month <- function (x) {
  cat("\n\n/ Area / Season Year / Month /\n")
  x
}

#' @rdname log_calc
log_calc_stats_by_area_calendar_year <- function (x) {
  cat("\n\n/ Area / Calendar Year /\n")
  x
}

