##  CREATE SEPARATE TABLES FOR EACH VARIABLE

CreateRTables <- function(df) {

  ## Build ALTERNATIVES table
  alternatives <- data.frame(dplyr::distinct(df, alternative))
  alternatives <- cbind(id=rownames(alternatives), alternatives)
  message(c("ALTERNATIVES table created for: ", file_name))
  
  
  ## Build SOURCES table
  sources <- data.frame(dplyr::distinct(df, source))
  sources <- cbind(id=rownames(sources), sources)
  message(c("SOURCES table created for: ", file_name))
  
  
  ## Build TYPES table
  types <- data.frame(dplyr::distinct(df, type, units, measure))
  types <- cbind(id=rownames(types), types)
  message(c("TYPES table created for: ", file_name))
  
  
  ## Build RIVERS table
  rivers_temp <- data.frame(dplyr::distinct(df, river))
  rivers_temp <- cbind(id=rownames(rivers_temp), rivers_temp)
  rivers <- rivers_temp
  message(c("RIVERS table created for: ", file_name))
  
  
  ## Build LOCATIONS table
  locations <- data.frame(dplyr::distinct(df, location))
  locations$location <- as.character(locations$location)
  locations <- cbind(id=rownames(locations), locations)
  message(c("LOCATIONS table created for: ", file_name))
  
  
  ## Build MODELED_DATES table
  modeled_dates <- data.frame(dplyr::distinct(df, date))
  
  modeled_dates$year <- lubridate::year(modeled_dates$date)
  modeled_dates$month <- lubridate::month(modeled_dates$date)
  modeled_dates$day <- lubridate::day(modeled_dates$date)
  
  modeled_dates <- cbind(id=rownames(modeled_dates), modeled_dates)
  message(c("MODELED_DATES table created for: ", file_name))
  
  
  ## Build MONTHS table
  temp <- data.frame(dplyr::distinct(modeled_dates, date))
  months <- data.frame(month_name=unique(months(temp$date)))
  months <- cbind(id=rownames(months), months)
  message(c("MONTHS table created for: ", file_name))
  
  
  ## Build YEAR_DATES table
  one_year <- df[year(df$date)==1931,]$date
  year_dates <- data.frame(id=1:365)
  year_dates$month_name <- months(unique(one_year))
  year_dates$month <- as.integer(format(unique(one_year), "%m"))
  year_dates$day <- lubridate::day(unique(one_year))
  message(c("YEAR_DATES table created for: ", file_name))
  
  
  ## Build DATA_BRIDGE table
  bridge_table_temp <- df
  
  # Alternative ids
  index <- match(bridge_table_temp$alternative, alternatives$alternative, nomatch = NA_integer_, incomparables = NULL)
  bridge_table_temp$alternative <- alternatives$id[index]
  bridge_table_temp <- dplyr::rename(bridge_table_temp, alt_id = alternative)
  
  # Type ids
  index <- match(bridge_table_temp$type, types$type, nomatch = NA_integer_, incomparables = NULL)
  bridge_table_temp$type <- types$id[index]
  bridge_table_temp <- dplyr::rename(bridge_table_temp, type_id = type)
  
  # Source ids
  index <- match(bridge_table_temp$source, sources$source, nomatch = NA_integer_, incomparables = NULL)
  bridge_table_temp$source <- sources$id[index]
  bridge_table_temp <- dplyr::rename(bridge_table_temp, source_id = source)
  
  # River ids
  index <- match(bridge_table_temp$river, rivers$river, nomatch = NA_integer_, incomparables = NULL)
  bridge_table_temp$river <- rivers$id[index]
  bridge_table_temp <- dplyr::rename(bridge_table_temp, river_id = river)
  
  # Location ids
  index <- match(bridge_table_temp$location, locations$location, nomatch = NA_integer_, incomparables = NULL)
  bridge_table_temp$location <- locations$id[index]
  bridge_table_temp <- dplyr::rename(bridge_table_temp, location_id = location)
  
  # Create bridge_table
  index <-match(c("alt_id", "type_id", "source_id", "river_id", "location_id"),colnames(bridge_table_temp), nomatch = NA_integer_, incomparables = NULL)
  data_bridge <- bridge_table_temp[,index]
  
  data_bridge <- unique(data_bridge)
  rownames(data_bridge) <- seq(length=nrow(data_bridge)) 
  data_bridge <- cbind(id=rownames(data_bridge), data_bridge)
  
  message(c("DATA_BRIDGE table created for: ", file_name))
  
  
  ## Build MODEL_DATA table
  bt <- data_bridge
  data_bridge$code <- paste(bt$alt_id, bt$type_id, bt$source_id, bt$river_id, bt$location_id)
  bridge_table_temp$code <- paste(bridge_table_temp$alt_id, bridge_table_temp$type_id, bridge_table_temp$source_id, bridge_table_temp$river_id, bridge_table_temp$location_id)
  model_data <- dplyr::select(bridge_table_temp, id, date, value, code)
  
  model_data$code <- match(model_data$code, data_bridge$code, nomatch = NA_integer_, incomparables = NULL)
  model_data <- dplyr::rename(model_data, data_bridge_id = code)
  
  # Add year, month, day to data table
  model_data$year <- lubridate::year(model_data$date)
  model_data$month_num <- lubridate::month(model_data$date)
  model_data$day_num <- lubridate::day(model_data$date)
  
  model_data$year_dates_id <- year_dates[year_dates$month==model_data$month && year_dates$day==model_data$day, "id"]
  
  model_data$date <- match(model_data$date, modeled_dates$date, nomatch = NA_integer_, incomparables = NULL)
  model_data <- dplyr::rename(model_data, modeled_dates_id = date)
  
  # Reorder columns
  model_data <-model_data[c("id","data_bridge_id","modeled_dates_id","value","year_dates_id","year","month_num","day_num")]
  message(c("MODEL_DATA table created for: ", file_name))
  
  # Convert values to numeric
  options(digits=9)
  model_data$value <- as.numeric(model_data$value)
  
  
  ## CREATE STATS_TABLE
  stats_data_temp <- tidyr::spread(model_data[,c("data_bridge_id", "year_dates_id", "year", "value")], year, value)
  stats_data_temp <- cbind(id=1:nrow(stats_data_temp), stats_data_temp)
  
  stats <- data.frame(
    minimum=numeric(),
    tenth=numeric(),
    fiftieth=numeric(),
    avgerage=numeric(),
    ninetieth=numeric(),
    maximum=numeric()
  )
  
  start_time <- Sys.time()
  
  message("Processing Stats for STATS_TABLE: ")
  
  pb <- txtProgressBar(min = 1, max = nrow(stats_data_temp), initial = 1, char = "=",
                       width = NA, "title", "label", style = 3, file = "")

  for (i in 1:nrow(stats_data_temp)) {
    # message(c("Processing row ", i, " of ", nrow(stats_data_temp)))

    setTxtProgressBar(pb, i)
    
    stats[i,c("tenth","fiftieth","ninetieth")] <- quantile(stats_data_temp[i,4:ncol(stats_data_temp)],probs=c(0.1, 0.5, 0.9))
    stats[i,"minimum"] <- min(stats_data_temp[i,4:ncol(stats_data_temp)])
    stats[i,"avgerage"] <- mean(unlist(stats_data_temp[i,4:ncol(stats_data_temp)]))
    stats[i,"maximum"] <- max(stats_data_temp[i,4:ncol(stats_data_temp)])
  }
  
  close(pb)

  end_time <- Sys.time()
  
  # elapsed_time <- end_time - start_time
  elapsed_time <- difftime(end_time, start_time, units="secs")
  
  message(c("It took ", round(elapsed_time[[1]],2), " seconds to calculate stats for  ", nrow(stats_data_temp), " rows of data"))
  
  model_stats <- stats_data_temp[,1:3]
  model_stats <- cbind(model_stats,stats)
  
  model_stats_all <- cbind(stats_data_temp, stats)
  message(c("MODEL_STATS table created for: ", file_name))
  message(c("MODEL_STATS_ALL table created for: ", file_name))
  
  tables_list <- list("alternatives"=alternatives, "sources"=sources, "types"=types, "rivers"=rivers, "locations"=locations, "modeled_dates"=modeled_dates, "months"=months, "year_dates"=year_dates, "data_bridge"=data_bridge, "model_data"=model_data, "model_stats"=model_stats, "model_stats_all"=model_stats_all)
  return(tables_list)
}