# Run this following line if testing code without calling the PushToPSQL function
# df <- df_ALL

PushToPSQL <- function(df, DB_selected) {
  message(
    "IN PushToPSQL. First row of df_final: ",
    df[1,]$alternative,
    " ",
    df[1,]$type,
    " ",
    df[1,]$river,
    " ",
    df[1,]$location
  )
  
  # setwd("~/Box Sync/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
  
  if (!"RPostgreSQL" %in% rownames(installed.packages())) {
    install.packages("RPostgreSQL")
  }
  
  if (!"dotenv" %in% rownames(installed.packages())) {
    install.packages("dotenv")
  }
  
  if (!"DBI" %in% rownames(installed.packages())) {
    install.packages("DBI")
  }
  
  if (!"dbplyr" %in% rownames(installed.packages())) {
    install.packages("dbplyr")
  }
  
  if (!"odbc" %in% rownames(installed.packages())) {
    install.packages("odbc")
  }
  
  library("RPostgreSQL")
  library(DBI)
  library(dplyr)
  library(dbplyr)
  library(odbc)
  library("dotenv")
  
  # # loads the PostgreSQL driver
  driver <- dbDriver("PostgreSQL")
  # driver <- dbDriver("odbc")
  
  # Load environment variables
  load_dot_env(file = ".env") # Loads variables from .env into R environment
  
  
  # creates a connection to the postgres database
  if (DB_selected == "Production DB") {
    message("**** Inserting into Production DB ****")
    
    connection <- dbConnect(
      driver,
      dbname = Sys.getenv("dbnameprod"),
      host = Sys.getenv("prodhost"),
      port = Sys.getenv("port"),
      user = Sys.getenv("user"),
      password = Sys.getenv("password")
    )
    
    
  } else if (DB_selected == "Testing DB") {
    message("**** Inserting into Testing DB ****")
    
    connection <- dbConnect(
      driver,
      dbname = Sys.getenv("dbnametest"),
      host = Sys.getenv("testhost"),
      port = Sys.getenv("port"),
      user = Sys.getenv("user"),
      password = Sys.getenv("password")
    )
    
  } else {
    message(" DB NAME NOT RECOGNIZED - STOPPING PushToPSQL")
    return()
  }
  
  
  
  
  message(" ")
  message("-------------------------")
  message("*** INSERTING INTO DB ***")
  message("-------------------------")
  message(" ")
  
  # Start timer
  SQL_module_start <- Sys.time()
  
  # Initialize LOCAL_data_summary and LOCAL_data_bridge
  LOCAL_data_summary <- data.frame(
    alternative = character(),
    type = character(),
    source = character(),
    river = character(),
    location = character(),
    stringsAsFactors = FALSE
  )
  
  LOCAL_data_bridge <- data.frame(
    alt_id = integer(),
    type_id = integer(),
    source_id = integer(),
    river_id = integer(),
    location_id = integer(),
    code = character(),
    dataset_is_new = logical(),
    stringsAsFactors = FALSE
  )
  
  # Initialize flag as FALSE
  LOCAL_data_bridge[1, 'dataset_is_new'] <- FALSE
  
  LOCAL_data_summary[1, "alternative"] <-
    as.character(df$alternative[1])
  LOCAL_data_summary[1, "type"] <- as.character(df$type[1])
  LOCAL_data_summary[1, "source"] <- as.character(df$source[1])
  LOCAL_data_summary[1, "river"] <- as.character(df$river[1])
  LOCAL_data_summary[1, "location"] <- as.character(df$location[1])
  
  # message("LOCAL_data_summary: ", paste(LOCAL_data_summary[1, 1], LOCAL_data_summary[1, 2], LOCAL_data_summary[1, 3], LOCAL_data_summary[1, 4], LOCAL_data_summary[1, 5]))
  
  ## --------------------------
  ## 1. Check ALTERNATIVE in DB
  ## --------------------------
  
  # Does the alternative exist in the DB?
  DB_alternative <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, alternative FROM alternatives WHERE alternative = '",
        LOCAL_data_summary[1, "alternative"] ,
        "'"
      )
    )
  
  if (nrow(DB_alternative) == 0) {
    # If the alternative is NOT in the DB, then insert it and get the new id
    LOCAL_data_bridge[1, 'dataset_is_new'] <- TRUE
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO alternatives (alternative) VALUES ('",
          LOCAL_data_summary[1, "alternative"],
          "') RETURNING id, alternative;"
        )
      )
    
    message("Inserted ALTERNATIVE into DB: ",
            paste(returned_id[1, 1], "-", returned_id[1, 2]))
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "alt_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "alt_id"] <- DB_alternative$id
  }
  
  
  
  ## -----------------------
  ## 2. Check TYPE in DB
  ## -----------------------
  
  # Does the type exist in the DB?
  DB_type <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, type FROM types WHERE type = '",
        LOCAL_data_summary[1, "type"] ,
        "'"
      )
    )
  
  if (nrow(DB_type) == 0) {
    # If the type is NOT in the DB, then insert it and get the new id
    LOCAL_data_bridge[1, 'dataset_is_new'] <- TRUE
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO types (type) VALUES ('",
          LOCAL_data_summary[1, "type"],
          "') RETURNING id, type;"
        )
      )
    
    message("Inserted TYPE into DB: ",
            paste(returned_id[1, 1], "-", returned_id[1, 2]))
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "type_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "type_id"] <- DB_type$id
  }
  
  
  
  ## -----------------------
  ## 3. Check SOURCE in DB
  ## -----------------------
  
  # Does the source exist in the DB?
  DB_source <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, source FROM sources WHERE source = '",
        LOCAL_data_summary[1, "source"] ,
        "'"
      )
    )
  
  if (nrow(DB_source) == 0) {
    # If the source is NOT in the DB, then insert it and get the new id
    LOCAL_data_bridge[1, 'dataset_is_new'] <- TRUE
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO sources (source) VALUES ('",
          LOCAL_data_summary[1, "source"],
          "') RETURNING id, source;"
        )
      )
    
    message("Inserted SOURCE into DB: ",
            paste(returned_id[1, 1], "-", returned_id[1, 2]))
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "source_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "source_id"] <- DB_source$id
  }
  
  
  ## -----------------------
  ## 4. Check RIVER in DB
  ## -----------------------
  
  # Does the river exist in the DB?
  DB_river <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, river FROM rivers WHERE river = '",
        LOCAL_data_summary[1, "river"] ,
        "'"
      )
    )
  
  if (nrow(DB_river) == 0) {
    # If the river is NOT in the DB, then insert it and get the new id
    LOCAL_data_bridge[1, 'dataset_is_new'] <- TRUE
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO rivers (river) VALUES ('",
          LOCAL_data_summary[1, "river"],
          "') RETURNING id, river;"
        )
      )
    
    message("Inserted RIVER into DB: ",
            paste(returned_id[1, 1], "-", returned_id[1, 2]))
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "river_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "river_id"] <- DB_river$id
  }
  
  
  ## -----------------------
  ## 5. Check LOCATION in DB
  ## -----------------------
  
  # Does the location exist in the DB?
  DB_location <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, location FROM locations WHERE location = '",
        LOCAL_data_summary[1, "location"] ,
        "'"
      )
    )
  
  if (nrow(DB_location) == 0) {
    # If the location is NOT in the DB, then insert it and get the new id
    LOCAL_data_bridge[1, 'dataset_is_new'] <- TRUE
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO locations (location) VALUES ('",
          LOCAL_data_summary[1, "location"],
          "') RETURNING id, location;"
        )
      )
    
    message("Inserted LOCATION into DB: ",
            paste(returned_id[1, 1], "-", returned_id[1, 2]))
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "location_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "location_id"] <- DB_location$id
  }
  
  
  ## -----------------------
  ## Calculate data_bridge code
  ## -----------------------
  
  LOCAL_data_bridge[1, "code"] <-
    paste(
      LOCAL_data_bridge[1, 1],
      LOCAL_data_bridge[1, 2],
      LOCAL_data_bridge[1, 3],
      LOCAL_data_bridge[1, 4],
      LOCAL_data_bridge[1, 5]
    )
  
  message("LOCAL_data_bridge: ", LOCAL_data_bridge[1, 6])
  
  
  
  ## -----------------------
  ## PROCESS REMAINING DATA AND INSERT INTO DB
  ## -----------------------
  
  if (LOCAL_data_bridge[1, 'dataset_is_new'] == FALSE) {
    # If all of the fields are already in the DB, then skip processing of data, don't insert, send message that it is already in there
    
    message('Dataset already exists in the DB - Nothing inserted')
    
  } else {
    # CONTINUE PROCESSING DATA TO INSERT INTO THE DB
    
    # -----------------------
    # Insert data_bridge into DB
    # -----------------------
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO data_bridge (alternative_id, type_id, source_id, river_id, location_id, code) VALUES (",
          paste(
            LOCAL_data_bridge[1, 1],
            LOCAL_data_bridge[1, 2],
            LOCAL_data_bridge[1, 3],
            LOCAL_data_bridge[1, 4],
            LOCAL_data_bridge[1, 5],
            sep = ","
          ),
          ", '",
          LOCAL_data_bridge[1, 6],
          "'",
          ") RETURNING id;"
        )
      )
    
    message(
      "Inserted DATA_BRIDGE into DB: id - ",
      paste(returned_id[1, 1], ", CODE -", LOCAL_data_bridge[1, 6])
    )
    
    
    ## -----------------------
    ## Build MONTHS table
    ## -----------------------
    
    # Check if the 'months' table exists. If it does, skip this step.
    DB_months_count <-
      dbGetQuery(connection, "SELECT COUNT(month_name) FROM months")
    
    if ((DB_months_count < 12) && (DB_months_count > 0)) {
      message("Possible DB error - less than 12 months in the 'months' table")
      
    } else if (DB_months_count == 0) {
      # INSERT INTO DB
      # Assuming that the current dataset has all 12 months, so don't need to verify that it does, just insert it
      
      # Get data from the df
      distinctDates <- data.frame(dplyr::distinct(df, date))
      LOCAL_months <-
        data.frame(month_name = unique(months(distinctDates$date)))
      
      LOCAL_months_list <- as.character(LOCAL_months$month)
      LOCAL_months_list <-
        paste0('\'', paste(LOCAL_months_list, collapse = '\',\''), '\'')
      
      dbWriteTable(
        connection,
        "months",
        LOCAL_months_list,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE # to protect current values
      )
      
      message("Month names inserted into DB in 'months' table")
      
    } else {
      message("Months table already in DB - nothing added")
    }
    
    
    
    ## -----------------------
    ## Build MODELED_DATES table
    ## -----------------------
    
    # Check if the 'modeled_dates' table exists and has 29930 entries. If it does, skip this step.
    
    DB_modeled_dates_count <-
      dbGetQuery(connection,
                 "SELECT COUNT(DISTINCT date) FROM modeled_dates")
    
    if ((DB_modeled_dates_count < 29930) &&
        (DB_modeled_dates_count > 0)) {
      message("Possible DB error - less than 29930 modeled_dates in the 'modeled_dates' table")
      
    } else if (DB_modeled_dates_count == 0) {
      # INSERT INTO DB
      # Get data from the df
      # Assuming that the current dataset has all 29930 modeled_dates, so don't need to verify that it does, just insert it
      
      LOCAL_modeled_dates <- data.frame(dplyr::distinct(df, date))
      
      LOCAL_modeled_dates$year <-
        lubridate::year(LOCAL_modeled_dates$date)
      LOCAL_modeled_dates$month <-
        lubridate::month(LOCAL_modeled_dates$date)
      LOCAL_modeled_dates$day <-
        lubridate::day(LOCAL_modeled_dates$date)
      
      LOCAL_modeled_dates_list <-
        as.character(LOCAL_modeled_dates$date)
      LOCAL_modeled_dates_list <-
        paste0('\'',
               paste(LOCAL_modeled_dates_list, collapse = '\',\''),
               '\'')
      
      dbWriteTable(
        connection,
        "modeled_dates",
        LOCAL_modeled_dates_list,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE # to protect current values
      )
      
      message("Modeled_dates inserted into DB in 'modeled_dates' table")
      
    } else {
      message("Modeled_dates table already in DB - nothing added")
    }
    
    
    
    
    
    ## -----------------------
    ## Build YEAR_DATES table
    ## -----------------------
    
    # Check if the 'year_dates' table exists and has 365 entries. If it does, skip this step.
    
    DB_year_dates_count <-
      dbGetQuery(connection, "SELECT COUNT(DISTINCT id) FROM year_dates")
    
    if ((DB_year_dates_count < 365) && (DB_year_dates_count > 0)) {
      message("Possible DB error - less than 365 year_dates in the 'year_dates' table")
      
    } else if (DB_year_dates_count == 0) {
      # INSERT INTO DB
      # Assuming that the current dataset has all 365 year_dates, so don't need to verify that it does, just insert it
      
      # Prep one year of dates to insert in DB
      one_year <- df[lubridate::year(df$date) == 1931, ]$date
      LOCAL_year_dates <- data.frame(id = 1:365)
      LOCAL_year_dates$month_name <- months(unique(one_year))
      LOCAL_year_dates$month <-
        as.integer(format(unique(one_year), "%m"))
      LOCAL_year_dates$day <- lubridate::day(unique(one_year))
      
      to_insert <- LOCAL_year_dates[, 2:4]
      
      dbWriteTable(
        connection,
        "year_dates",
        to_insert,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE
      ) # to protect current values
      
      message("Year_dates inserted into DB in 'modeled_dates' table")
      
      
    } else {
      message("year_dates table already in DB - nothing added")
    }
    
    
    
    
    
    
    
    
    
    
    ## END OF LOOP TRIGGERED IF data_bridge ISN'T IN THE DB
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  ## -----------------------
  ## Build DATA_BRIDGE table
  ## -----------------------
  
  
  # UPDATE LOCAL COPY OF TABLES (since they may have been updated above)
  DB_alternatives <- dbReadTable(connection, "alternatives")
  DB_locations <- dbReadTable(connection, "locations")
  DB_rivers <- dbReadTable(connection, "rivers")
  DB_sources <- dbReadTable(connection, "sources")
  DB_types <- dbReadTable(connection, "types")
  DB_modeled_dates <- dbReadTable(connection, "modeled_dates")
  DB_year_dates <- dbReadTable(connection, "year_dates")
  
  message("Starting DATA_BRIDGE")
  
  bridge_table_temp <- df
  
  # Alternative ids
  index <-
    match(
      bridge_table_temp$alternative,
      DB_alternatives$alternative,
      nomatch = NA_integer_,
      incomparables = NULL
    )
  bridge_table_temp$alternative <- DB_alternatives$id[index]
  bridge_table_temp <-
    dplyr::rename(bridge_table_temp, alternative_id = alternative)
  
  # Type ids
  index <-
    match(
      bridge_table_temp$type,
      DB_types$type,
      nomatch = NA_integer_,
      incomparables = NULL
    )
  bridge_table_temp$type <- DB_types$id[index]
  bridge_table_temp <-
    dplyr::rename(bridge_table_temp, type_id = type)
  
  # Source ids
  index <-
    match(
      bridge_table_temp$source,
      DB_sources$source,
      nomatch = NA_integer_,
      incomparables = NULL
    )
  bridge_table_temp$source <- DB_sources$id[index]
  bridge_table_temp <-
    dplyr::rename(bridge_table_temp, source_id = source)
  
  # River ids
  index <-
    match(
      bridge_table_temp$river,
      DB_rivers$river,
      nomatch = NA_integer_,
      incomparables = NULL
    )
  bridge_table_temp$river <- DB_rivers$id[index]
  bridge_table_temp <-
    dplyr::rename(bridge_table_temp, river_id = river)
  
  # Location ids
  index <-
    match(
      bridge_table_temp$location,
      DB_locations$location,
      nomatch = NA_integer_,
      incomparables = NULL
    )
  bridge_table_temp$location <- DB_locations$id[index]
  bridge_table_temp <-
    dplyr::rename(bridge_table_temp, location_id = location)
  
  # Create bridge_table
  index <-
    match(
      c(
        "alternative_id",
        "type_id",
        "source_id",
        "river_id",
        "location_id"
      ),
      colnames(bridge_table_temp),
      nomatch = NA_integer_,
      incomparables = NULL
    )
  LOCAL_data_bridge <- bridge_table_temp[, index]
  
  LOCAL_data_bridge <- unique(LOCAL_data_bridge)
  rownames(LOCAL_data_bridge) <-
    seq(length = nrow(LOCAL_data_bridge))
  LOCAL_data_bridge <-
    cbind(id = rownames(LOCAL_data_bridge), LOCAL_data_bridge)
  
  # Build CODE
  bt <- LOCAL_data_bridge
  LOCAL_data_bridge$code <-
    paste(bt$alternative_id,
          bt$type_id,
          bt$source_id,
          bt$river_id,
          bt$location_id)
  
  
  # INSERT UNIQUE data_bridge values into DB
  # 1 - query the table to see which values exist in the DB compared to my dataframe
  DB_data_bridge <- dbReadTable(connection, "data_bridge")
  
  # 2 - remove existing values from the dataframe
  db_matches <-
    match(as.character(LOCAL_data_bridge$code), DB_data_bridge$code)
  num_df <- length(LOCAL_data_bridge$id)
  
  L_data_bridge_NOid <-
    dplyr::select(LOCAL_data_bridge,
                  alternative_id,
                  type_id,
                  source_id,
                  river_id,
                  location_id,
                  code)
  
  data_to_insert <- filter(L_data_bridge_NOid, is.na(db_matches))
  DATA_BRIDGE_inserted <- data_to_insert
  num_to_insert <- length(data_to_insert[[1]])
  num_dups <- num_df - num_to_insert
  
  # 3 - dbWriteTable to append the new values
  
  if (num_to_insert > 0) {
    RPostgreSQL::dbWriteTable(
      connection,
      "data_bridge",
      data_to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    )
    message(
      c(
        num_df,
        " DATA_BRIDGEs in df, ",
        num_dups,
        " duplicates, ",
        num_to_insert,
        " inserted in DB"
      )
    )
  } else {
    message(
      c(
        num_df,
        " DATA_BRIDGEs in df, ",
        num_dups,
        " duplicates, ",
        num_to_insert,
        " inserted in DB"
      )
    )
  }
  
  ## -----------------------
  ## Build DATA table
  ## -----------------------
  
  # NOTE: The data_to_insert dataframe has the codes for the data to be inserted in both
  # The 'data' table and the 'stats' table.  Don't need to do a compare by pulling
  # the data and stats table from the DB. Just push any LOCAL_data and LOCAL_stats that have a code
  # that is in data_to_insert.
  
  if (num_to_insert == 0) {
    message("**No data to process**")
    
  } else {
    message("Processing DATA_TABLE")
    
    data_processing_start <- Sys.time()
    
    # Get codes for the data to be inserted
    codes_to_insert <- data_to_insert$code
    
    # Make a LOCAL version of the data with a 'code' field
    bridge_table_temp$code <-
      paste(
        bridge_table_temp$alternative_id,
        bridge_table_temp$type_id,
        bridge_table_temp$source_id,
        bridge_table_temp$river_id,
        bridge_table_temp$location_id
      )
    
    LOCAL_data <-
      dplyr::select(bridge_table_temp, id, date, value, code)
    
    # Count how many rows in the local dataset BEFORE removing rows that exist in the DB
    num_df <- length(LOCAL_data$id)
    
    # query the table to see which values exist in the DB compared to my dataframe
    # This gives us the 'data_bridge_ids' that correspond to the 'codes'
    DB_data_bridge <- dbReadTable(connection, "data_bridge")
    
    # MUST do the step below to get the row index for matches, THEN get the corresponding ID for those rows
    data_bridge_index <-
      match(
        LOCAL_data$code,
        DB_data_bridge$code,
        nomatch = NA_integer_,
        incomparables = NULL
      )
    
    # Find the data_bridge_id where LOCAL_data already exists in the DB
    LOCAL_data$data_bridge_id <-
      DB_data_bridge$id[data_bridge_index]
    
    LOCAL_data_to_insert <-
      dplyr::filter(LOCAL_data, LOCAL_data$code %in% codes_to_insert)
    
    DATA_inserted <- LOCAL_data_to_insert
    
    # Process data to add missing columns before inserting into DB
    # Add year, month, day to data table
    LOCAL_data_to_insert$year <-
      lubridate::year(LOCAL_data_to_insert$date)
    LOCAL_data_to_insert$month_num <-
      lubridate::month(LOCAL_data_to_insert$date)
    LOCAL_data_to_insert$day_num <-
      lubridate::day(LOCAL_data_to_insert$date)
    
    
    # ****CHECK THIS - LOOKS LIKE IT RETURNS 365 VALUES?!
    LOCAL_data_to_insert$year_dates_id <-
      DB_year_dates[DB_year_dates$month == LOCAL_data_to_insert$month &&
                      DB_year_dates$day == LOCAL_data_to_insert$day, "id"]
    
    
    modeled_dates_index <-
      match(
        as.character(LOCAL_data_to_insert$date),
        DB_modeled_dates$date,
        nomatch = NA_integer_,
        incomparables = NULL
      )
    
    LOCAL_data_to_insert$date <-
      DB_modeled_dates$id[modeled_dates_index]
    LOCAL_data_to_insert <-
      dplyr::rename(LOCAL_data_to_insert, modeled_dates_id = date)
    
    # Reorder columns
    LOCAL_data_to_insert <-
      LOCAL_data_to_insert[c(
        "id",
        "data_bridge_id",
        "modeled_dates_id",
        "value",
        "year_dates_id",
        "year",
        "month_num",
        "day_num"
      )]
    
    # Convert values to numeric
    options(digits = 9)
    LOCAL_data_to_insert$value <-
      as.numeric(LOCAL_data_to_insert$value)
    
    end_time <- Sys.time()
    elapsed_time <-
      difftime(end_time, data_processing_start, units = "secs")
    
    message(c(
      "It took ",
      round(elapsed_time[[1]], 2),
      " seconds to process the data table for ",
      nrow(LOCAL_data_to_insert),
      " rows of data."
    ))
    
    # Drop the ID column
    LOCAL_data_to_insert_NO_ID <-
      dplyr::select(
        LOCAL_data_to_insert,
        data_bridge_id,
        modeled_dates_id,
        value,
        year_dates_id,
        year,
        month_num,
        day_num
      )
    
    # INSERT into data table
    message(c("Inserting into 'DATA' table..."))
    
    num_to_insert <- length(LOCAL_data_to_insert_NO_ID[[1]])
    num_dups <- num_df - num_to_insert
    
    SQL_start <- Sys.time()
    
    # Write to DB
    RPostgreSQL::dbWriteTable(
      connection,
      "data",
      LOCAL_data_to_insert_NO_ID,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    )
    
    message(c(
      num_df,
      " DATA in df, ",
      num_dups,
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
    
    end_time <- Sys.time()
    elapsed_time <- difftime(end_time, SQL_start, units = "secs")
    
    message(c(
      "SQL insert into 'DATA' complete. Elapsed time: ",
      round(elapsed_time[[1]], 2),
      " seconds. "
    ))
  }
  
  ## -----------------------
  ## CREATE STATS_TABLE
  ## -----------------------
  
  if (num_to_insert == 0) {
    message("**No stats to process**")
    
  } else {
    message("Processing Stats for STATS_TABLE: ")
    start_time <- Sys.time()
    
    stats_data_temp <-
      tidyr::spread(LOCAL_data_to_insert_NO_ID[, c("data_bridge_id", "year_dates_id", "year", "value")], year, value)
    stats_data_temp <-
      cbind(id = 1:nrow(stats_data_temp), stats_data_temp)
    
    stats <- data.frame(
      minimum = numeric(),
      tenth = numeric(),
      fiftieth = numeric(),
      average = numeric(),
      ninetieth = numeric(),
      maximum = numeric()
    )
    
    pb <-
      txtProgressBar(
        min = 1,
        max = nrow(stats_data_temp),
        initial = 1,
        char = "=",
        width = NA,
        "title",
        "label",
        style = 3,
        file = ""
      )
    
    for (i in 1:nrow(stats_data_temp)) {
      setTxtProgressBar(pb, i)
      
      stats[i, c("tenth", "fiftieth", "ninetieth")] <-
        quantile(stats_data_temp[i, 4:ncol(stats_data_temp)], probs = c(0.1, 0.5, 0.9))
      stats[i, "minimum"] <-
        min(stats_data_temp[i, 4:ncol(stats_data_temp)])
      stats[i, "average"] <-
        mean(unlist(stats_data_temp[i, 4:ncol(stats_data_temp)]))
      stats[i, "maximum"] <-
        max(stats_data_temp[i, 4:ncol(stats_data_temp)])
    }
    
    close(pb)
    
    end_time <- Sys.time()
    elapsed_time <- difftime(end_time, start_time, units = "secs")
    
    message(c(
      "It took ",
      round(elapsed_time[[1]], 2),
      " seconds to calculate stats for  ",
      nrow(stats_data_temp),
      " rows of data"
    ))
    
    LOCAL_model_stats <- stats_data_temp[, 1:3]
    LOCAL_model_stats <- cbind(LOCAL_model_stats, stats)
    LOCAL_model_stats_all <- cbind(stats_data_temp, stats)
    
    num_df <- length(LOCAL_model_stats$id)
    
    # MUST remove the ID column before pushing to the DB
    LOCAL_model_stats_NO_ID <-
      dplyr::select(
        LOCAL_model_stats,
        data_bridge_id,
        year_dates_id,
        minimum,
        tenth,
        fiftieth,
        average,
        ninetieth,
        maximum
      )
    
    # STATS_inserted <- to_insert
    STATS_inserted <- LOCAL_model_stats_NO_ID
    
    num_insert <- length(LOCAL_model_stats_NO_ID[[1]])
    num_dups <- num_df - num_insert
    
    # 3 - dbWriteTable to append the new values
    
    message(c("Inserting into 'STATS' table..."))
    
    SQL_start <- Sys.time()
    
    RPostgreSQL::dbWriteTable(
      connection,
      "stats",
      LOCAL_model_stats_NO_ID,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    )
    
    message(c(
      num_df,
      " STATS in df, ",
      num_dups,
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
    
    end_time <- Sys.time()
    elapsed_time <- difftime(end_time, SQL_start, units = "secs")
    
    message(c(
      "SQL insert into 'STATS' complete. Elapsed time: ",
      round(elapsed_time[[1]], 2),
      " seconds. "
    ))
  }
  
  end_time <- Sys.time()
  elapsed_time <-
    difftime(end_time, SQL_module_start, units = "secs")
  
  message(" ")
  message("-----------------------------")
  message(" *** FINISHED SQL Insert *** ")
  message(c("Elapsed time: ",
            round(elapsed_time[[1]], 2),
            " seconds. "))
  message("-----------------------------")
  message(" ")
  
  dbDisconnect(connection)
  
  return()
  
}
