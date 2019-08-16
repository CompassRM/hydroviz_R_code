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
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "location_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "location_id"] <- DB_location$id
  }
  
  
  
  
  
  
  ## -----------------------
  ## Process remaining data and insert it into the DB
  ## -----------------------
  
  if (LOCAL_data_bridge[1, 'dataset_is_new'] == FALSE) {
    message('Dataset already exists in the DB - Nothing inserted')
  } else {
    # CONTINUE PROCESSING DATA TO INSERT INTO THE DB
  }
  
  
  
  
  

  
  DB_source <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, source FROM sources WHERE source = '",
        LOCAL_data_summary[1, "source"] ,
        "'"
      )
    )
  
  DB_river <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, river FROM rivers WHERE river = '",
        LOCAL_data_summary[1, "river"] ,
        "'"
      )
    )
  
  DB_location <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, location FROM locations WHERE location = '",
        LOCAL_data_summary[1, "location"] ,
        "'"
      )
    )
  
  
  # If all of the fields are already in the DB, then skip processing of data, don't insert, send message that it is already in there
  
  
  
  #
  LOCAL_data_bridge[1, "alt_id"] <- as.character(DB_alternative$id)
  LOCAL_data_bridge[1, "type_id"] <- as.character(DB_type$id)
  LOCAL_data_bridge[1, "source_id"] <- as.character(DB_source$id)
  LOCAL_data_bridge[1, "river_id"] <- as.character(DB_river$id)
  LOCAL_data_bridge[1, "location_id"] <-
    as.character(DB_location$id)
  LOCAL_data_bridge[1, "code"] <-
    paste(
      LOCAL_data_bridge[1, 1],
      LOCAL_data_bridge[1, 2],
      LOCAL_data_bridge[1, 3],
      LOCAL_data_bridge[1, 4],
      LOCAL_data_bridge[1, 5]
    )
  
  message("LOCAL_data_bridge: ", LOCAL_data_bridge[1, 6])
  
  
  
  
  
  
  
  
  
  # DB_alternatives <-
  #   dbGetQuery(
  #     connection,
  #     paste0(
  #       "SELECT id, alternative FROM alternatives WHERE alternative = '",
  #       LOCAL_data_summary[1, "alternative"] ,
  #       "' UNION SELECT id, type FROM types WHERE type = '",
  #       LOCAL_data_summary[1, "type"] ,
  #       "'"
  #     )
  #   )
  
  LOCAL_data_bridge <- data.frame(dplyr::distinct(df, alternative))
  
  
  
  ## -----------------------
  ## Build ALTERNATIVES table
  ## -----------------------
  
  # Get alternative name from the df
  # Each column in the xlsx spreadsheet is for a single alternative - so just grab the first alternative name
  LOCAL_alternatives <- data.frame(dplyr::distinct(df, alternative))
  # ALTERNATIVELY COULD JUST GRAB THE FIRST ALT NAME - THEY'LL ALL BE THE SAME.
  
  # # Build df for testing
  # TESTdf<-data.frame("testing_errr")
  # names(TESTdf) <- "alternative"
  # LOCAL_alternatives <- rbind(LOCAL_alternatives, TESTdf)
  #
  # TESTdf<-data.frame("testing_qwqwerwdf")
  # names(TESTdf) <- "alternative"
  # LOCAL_alternatives <- rbind(LOCAL_alternatives, TESTdf)
  #
  # TESTdf<-data.frame("Res_Ftpk2_SpawnCue_20190523")
  # names(TESTdf) <- "alternative"
  # LOCAL_alternatives <- rbind(LOCAL_alternatives, TESTdf)
  
  # LOCAL_alts_list <- LOCAL_alternatives$alternative
  LOCAL_alts_list <- as.character(LOCAL_alternatives$alternative)
  LOCAL_alts_list <-
    paste0('\'', paste(LOCAL_alts_list, collapse = '\',\''), '\'')
  
  
  # QUERY USING THE STRING to see if any of the LOCAL_alternatives exist in the DB (return all LOCAL_alternatives that are in the DB)
  DB_alternatives <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT id, alternative FROM alternatives WHERE alternative IN (",
        LOCAL_alts_list,
        ")"
      )
    )
  
  
  # NOTE - IF THE ALT IS *NOT* IN THE DB, THEN THE DATASET *MUST* BE NEW - WILL NEED A NEW DATA BRIDGE.
  # IF THE ALT *IS* IN THE DB, THE DATASET MAY BE NEW OR MAY ALREADY BE IN THE DB - NEED TO CONSIDER BOTH CASES
  
  # CHECK SIZE OF DB_alternatives (returned result from DB).
  # If ZERO, then the new alt is not in DB, need to insert it.
  # If NOT zero, then it's in the DB, so just put the id into LOCAL_data_bridge and move on.
  
  
  
  if (nrow(DB_alternatives) == 0) {
    # Insert alternative name into the DB and return the id
    
    returned_id <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO alternatives (alternative) VALUES (",
          LOCAL_alts_list,
          ") RETURNING id, alternative;"
        )
      )
    
    # Add the id to LOCAL_data_bridge
    LOCAL_data_bridge[1, "Alt_id"] <- returned_id$id
    
  } else {
    # Add the id from the SELECT query to LOCAL_data_bridge
    LOCAL_data_bridge[1, "Alt_id"] <- DB_alternatives$id
    
  }
  
  
  # message(
  #   c(
  #     length(LOCAL_alternatives$alternative),
  #     " ALTERNATIVES in df, ",
  #     length(db_matches),
  #     " duplicates, ",
  #     num_insert,
  #     " inserted in DB"
  #   )
  # )
  
  
  
  
  # ALGORITHM:
  # If it's NOT in the DB - add it, get the ID, put that ID as the first number in the temp variable
  # for the data bridge code
  # Otherwise, if it IS in the DB, just continue
  
  
  
  # DON'T NEED THIS STUFF
  # Which of the LOCAL_alternatives is in the DB?
  db_matches <-
    match(DB_alternatives$alternative,
          LOCAL_alternatives$alternative)
  
  # From LOCAL_alternatives, remove all of those that are in the DB to get just those to be inserted
  to_insert <- LOCAL_alternatives[-c(db_matches), , drop = F]
  
  # Check if there are any remaining LOCAL_alternatives, insert them in the DB
  num_insert <- length(to_insert[[1]])
  
  # Rearrange the data as needed for the insert query
  to_insert <- as.character(to_insert$alternative)
  to_insert <-
    paste0("('", paste(to_insert, collapse = '\'),(\''), "')")
  
  if (num_insert > 0) {
    DB_alternatives_inserted <-
      dbGetQuery(
        connection,
        paste0(
          "INSERT INTO alternatives (alternative) VALUES ",
          to_insert,
          " RETURNING id, alternative;"
        )
      )
  }
  
  message(
    c(
      length(LOCAL_alternatives$alternative),
      " ALTERNATIVES in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    )
  )
  
  
  # # ***********************
  # DB_alternatives_after <- dbReadTable(connection, "alternatives")
  # # ***********************
  
  
  
  
  ## -----------------------
  ## Build SOURCES table
  ## -----------------------
  
  # Get data from the df
  LOCAL_sources <- data.frame(dplyr::distinct(df, source))
  
  # Build df for testing
  TESTdf <- data.frame("test_source")
  names(TESTdf) <- "source"
  LOCAL_sources <- rbind(LOCAL_sources, TESTdf)
  
  LOCAL_sources_list <- as.character(LOCAL_sources$source)
  LOCAL_sources_list <-
    paste0('\'', paste(LOCAL_sources_list, collapse = '\',\''), '\'')
  
  # QUERY USING THE STRING to see if any of the LOCAL_sources exist in the DB (return all LOCAL_sources that are in the DB)
  DB_sources <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT source FROM sources WHERE source IN (",
        LOCAL_sources_list,
        ")"
      )
    )
  
  # Which of the LOCAL_sources is in the DB?
  db_matches <- match(DB_sources$source, LOCAL_sources$source)
  
  # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
  to_insert <- LOCAL_sources[-c(db_matches), , drop = F]
  
  # Check if there are any remaining LOCAL_alternatives, insert them in the DB
  num_insert <- length(to_insert[[1]])
  
  if (num_insert > 0) {
    dbWriteTable(
      connection,
      "sources",
      to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    ) # to protect current values
    
    message(c(
      length(LOCAL_sources$source),
      " SOURCES in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
    
  } else {
    message(c(
      length(LOCAL_sources$source),
      " SOURCES in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
  }
  
  # ***********************
  DB_sources_after <- dbReadTable(connection, "sources")
  # ***********************
  
  
  
  
  ## -----------------------
  ## Build TYPES table
  ## -----------------------
  
  # Get data from the df
  LOCAL_types <-
    data.frame(dplyr::distinct(df, type, units, measure))
  
  LOCAL_types_list <- as.character(LOCAL_types$type)
  LOCAL_types_list <-
    paste0('\'', paste(LOCAL_types_list, collapse = '\',\''), '\'')
  
  # QUERY USING THE STRING to see if any of the LOCAL_types exist in the DB (return all LOCAL_types that are in the DB)
  DB_types <-
    dbGetQuery(connection,
               paste0("SELECT type FROM types WHERE type IN (", LOCAL_types_list, ")"))
  # NOTE - THIS QUERY WORKS SO LONG AS THERE IS ONLY ONE VERSION OF 'FLOW' OR 'STAGE' FOR EXAMPLE
  # IF THERE IS ANOTHER VERSION OF FLOW THAT HAS DIFFERENT UNITS OR MEASURE, THIS WILL FAIL.
  
  # Which of the LOCAL_types is in the DB?
  db_matches <- match(DB_types$type, LOCAL_types$type)
  
  # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
  to_insert <- LOCAL_types[-c(db_matches), , drop = F]
  
  # Check if there are any remaining LOCAL_alternatives, insert them in the DB
  num_insert <- length(to_insert[[1]])
  
  if (num_insert > 0) {
    dbWriteTable(
      connection,
      "types",
      to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    ) # to protect current values
    
    message(c(
      length(LOCAL_types$type),
      " TYPES in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
    
  } else {
    message(c(
      length(LOCAL_types$type),
      " TYPES in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
  }
  
  
  
  
  
  ## -----------------------
  ## Build RIVERS table
  ## -----------------------
  
  # Get data from the df
  LOCAL_rivers <- data.frame(dplyr::distinct(df, river))
  LOCAL_rivers$river <- as.character(LOCAL_rivers$river)
  
  LOCAL_rivers_list <- as.character(LOCAL_rivers$river)
  LOCAL_rivers_list <-
    paste0('\'', paste(LOCAL_rivers_list, collapse = '\',\''), '\'')
  
  # QUERY USING THE STRING to see if any of the LOCAL_rivers exist in the DB (return all LOCAL_rivers that are in the DB)
  DB_rivers <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT river FROM rivers WHERE river IN (",
        LOCAL_rivers_list,
        ")"
      )
    )
  
  # Which of the LOCAL_rivers is in the DB?
  db_matches <- match(DB_rivers$river, LOCAL_rivers$river)
  
  # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
  to_insert <- LOCAL_rivers[-c(db_matches), , drop = F]
  
  # Check if there are any remaining LOCAL_alternatives, insert them in the DB
  num_insert <- length(to_insert[[1]])
  
  if (num_insert > 0) {
    dbWriteTable(
      connection,
      "rivers",
      to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    ) # to protect current values
    
    message(c(
      length(LOCAL_rivers$river),
      " RIVERS in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
    
  } else {
    message(c(
      length(LOCAL_rivers$river),
      " RIVERS in df, ",
      length(db_matches),
      " duplicates, ",
      num_insert,
      " inserted in DB"
    ))
  }
  
  
  
  ## -----------------------
  ## Build LOCATIONS table
  ## -----------------------
  
  # NOTE - RUN THE RISK OF COLLISIONS HERE - THE LOCATION NUMBERS MAY NOT BE UNIQUE!
  # IT'S LIKELY THAT WE'LL GET LUCKY AND THEY WILL BE UNIQUE, BUT WHAT IS UNIQUE IS THE
  # COMBINATION OF 'RIVER' + 'LOCATION'
  
  
  # Get data from the df
  LOCAL_months <- data.frame(dplyr::distinct(df, month))
  LOCAL_months$location <- as.character(LOCAL_locations$location)
  
  LOCAL_locations_list <- as.character(LOCAL_locations$location)
  LOCAL_locations_list <-
    paste0('\'', paste(LOCAL_locations_list, collapse = '\',\''), '\'')
  
  # QUERY USING THE STRING to see if any of the LOCAL_locations exist in the DB (return all LOCAL_locations that are in the DB)
  DB_locations <-
    dbGetQuery(
      connection,
      paste0(
        "SELECT location FROM locations WHERE location IN (",
        LOCAL_locations_list,
        ")"
      )
    )
  
  # Which of the LOCAL_locations is in the DB?
  db_matches <-
    match(DB_locations$location, LOCAL_locations$location)
  
  # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
  to_insert <- LOCAL_locations[-c(db_matches), , drop = F]
  
  # Check if there are any remaining LOCAL_alternatives, insert them in the DB
  num_insert <- length(to_insert[[1]])
  
  if (num_insert > 0) {
    dbWriteTable(
      connection,
      "locations",
      to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    ) # to protect current values
    
    message(
      c(
        length(LOCAL_locations$location),
        " LOCATIONS in df, ",
        length(db_matches),
        " duplicates, ",
        num_insert,
        " inserted in DB"
      )
    )
    
  } else {
    message(
      c(
        length(LOCAL_locations$location),
        " LOCATIONS in df, ",
        length(db_matches),
        " duplicates, ",
        num_insert,
        " inserted in DB"
      )
    )
  }
  
  
  ## -----------------------
  ## Build MONTHS table
  ## -----------------------
  
  # Check if the 'months' table exists. If it does, skip this step.
  DB_months_count <-
    dbGetQuery(connection, "SELECT COUNT(DISTINCT month_name) FROM months")
  
  if ((DB_months_count < 12) && (DB_months_count > 0)) {
    message("Possible DB error - less than 12 months in the 'months' table")
    
  } else if (DB_months_count == 0) {
    # INSERT INTO DB
    # Get data from the df
    distinctDates <- data.frame(dplyr::distinct(df, date))
    LOCAL_months <-
      data.frame(month_name = unique(months(distinctDates$date)))
    
    LOCAL_months_list <- as.character(LOCAL_months$month)
    LOCAL_months_list <-
      paste0('\'', paste(LOCAL_months_list, collapse = '\',\''), '\'')
    
    # QUERY USING THE STRING to see if any of the LOCAL_months exist in the DB (return all LOCAL_months that are in the DB)
    DB_months <-
      dbGetQuery(
        connection,
        paste0(
          "SELECT month FROM months WHERE month IN (",
          LOCAL_months_list,
          ")"
        )
      )
    
    # Which of the LOCAL_months is in the DB?
    db_matches <- match(DB_months$month, LOCAL_months$month)
    
    # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
    to_insert <- LOCAL_months[-c(db_matches), , drop = F]
    
    # Check if there are any remaining LOCAL_alternatives, insert them in the DB
    num_insert <- length(to_insert[[1]])
    
    if (num_insert > 0) {
      dbWriteTable(
        connection,
        "months",
        to_insert,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE
      ) # to protect current values
      
      message(c(
        length(LOCAL_months$month),
        " MONTHS in df, ",
        length(db_matches),
        " duplicates, ",
        num_insert,
        " inserted in DB"
      ))
      
    } else {
      message(c(
        length(LOCAL_months$month),
        " MONTHS in df, ",
        length(db_matches),
        " duplicates, ",
        num_insert,
        " inserted in DB"
      ))
    }
    
  } else {
    message("Months table already in DB - nothing added")
  }
  
  
  ## -----------------------
  ## Build MODELED_DATES table
  ## -----------------------
  
  # Check if the 'modeled_dates' table exists and has 29930 entries. If it does, skip this step.
  
  DB_modeled_dates_count <-
    dbGetQuery(connection, "SELECT COUNT(DISTINCT date) FROM modeled_dates")
  
  if ((DB_modeled_dates_count < 29930) &&
      (DB_modeled_dates_count > 0)) {
    message("Possible DB error - less than 29930 modeled_dates in the 'modeled_dates' table")
    
  } else if (DB_modeled_dates_count == 0) {
    # INSERT INTO DB
    # Get data from the df
    
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
    
    # QUERY USING THE STRING to see if any of the LOCAL_modeled_dates exist in the DB (return all LOCAL_modeled_dates that are in the DB)
    DB_modeled_dates <-
      dbGetQuery(
        connection,
        paste0(
          "SELECT date FROM modeled_dates WHERE date IN (",
          LOCAL_modeled_dates_list,
          ")"
        )
      )
    
    # Which of the LOCAL_modeled_dates is in the DB?
    db_matches <-
      match(DB_modeled_dates$date, LOCAL_modeled_dates$date)
    db_matches <- db_matches[complete.cases(db_matches)]
    
    # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
    to_insert <- LOCAL_modeled_dates[-c(db_matches), , drop = F]
    to_insert$date <-
      as.character(to_insert$date) # Change date to character to put in DB
    
    # Check if there are any remaining LOCAL_alternatives, insert them in the DB
    num_insert <- length(to_insert[[1]])
    
    if (num_insert > 0) {
      dbWriteTable(
        connection,
        "modeled_dates",
        to_insert,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE
      ) # to protect current values
      
      message(
        c(
          length(LOCAL_modeled_dates$month),
          " MODELED_DATES in df, ",
          length(db_matches),
          " duplicates, ",
          num_insert,
          " inserted in DB"
        )
      )
      
    } else {
      message(
        c(
          length(LOCAL_modeled_dates$month),
          " MODELED_DATES in df, ",
          length(db_matches),
          " duplicates, ",
          num_insert,
          " inserted in DB"
        )
      )
    }
    
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
    # Get data from the df
    
    # Prep one year of dates to insert in DB
    one_year <- df[lubridate::year(df$date) == 1931, ]$date
    LOCAL_year_dates <- data.frame(id = 1:365)
    LOCAL_year_dates$month_name <- months(unique(one_year))
    LOCAL_year_dates$month <-
      as.integer(format(unique(one_year), "%m"))
    LOCAL_year_dates$day <- lubridate::day(unique(one_year))
    
    LOCAL_year_dates_list <- as.character(LOCAL_year_dates$date)
    LOCAL_year_dates_list <-
      paste0('\'',
             paste(LOCAL_year_dates_list, collapse = '\',\''),
             '\'')
    
    
    
    # CHANGE THIS - Don't need to pull year_dates from the DB and compare since we've checked whether we have 365 or 0 dates in the DB.
    # If 0 dates, then just push the data to the DB. if 365, skip. If between 0 and 365, sends error message
    
    
    
    # QUERY USING THE STRING to see if any of the LOCAL_year_dates exist in the DB (return all LOCAL_year_dates that are in the DB)
    DB_year_dates <-
      dbGetQuery(
        connection,
        paste0(
          "SELECT date FROM year_dates WHERE date IN (",
          LOCAL_year_dates_list,
          ")"
        )
      )
    
    # Which of the LOCAL_year_dates is in the DB?
    db_matches <- match(DB_year_dates$date, LOCAL_year_dates$date)
    db_matches <- db_matches[complete.cases(db_matches)]
    
    # From LOCAL_sources, remove all of those that are in the DB to get just those to be inserted
    to_insert <- LOCAL_year_dates[-c(db_matches), , drop = F]
    to_insert$date <-
      as.character(to_insert$date) # Change date to character to put in DB
    
    # Check if there are any remaining LOCAL_alternatives, insert them in the DB
    num_insert <- length(to_insert[[1]])
    
    if (num_insert > 0) {
      dbWriteTable(
        connection,
        "year_dates",
        to_insert,
        row.names = FALSE,
        append = TRUE,
        overwrite = FALSE
      ) # to protect current values
      
      message(
        c(
          length(LOCAL_year_dates$month),
          " year_dates in df, ",
          length(db_matches),
          " duplicates, ",
          num_insert,
          " inserted in DB"
        )
      )
      
    } else {
      message(
        c(
          length(LOCAL_year_dates$month),
          " year_dates in df, ",
          length(db_matches),
          " duplicates, ",
          num_insert,
          " inserted in DB"
        )
      )
    }
    
  } else {
    message("year_dates table already in DB - nothing added")
  }
  
  
  
  
  
  
  # Prep one year of dates to insert in DB
  one_year <- df[lubridate::year(df$date) == 1931, ]$date
  LOCAL_year_dates <- data.frame(id = 1:365)
  LOCAL_year_dates$month_name <- months(unique(one_year))
  LOCAL_year_dates$month <-
    as.integer(format(unique(one_year), "%m"))
  LOCAL_year_dates$day <- lubridate::day(unique(one_year))
  
  # 1 - query the table to see which values exist in the DB compared to my dataframe
  DB_year_dates <- dbReadTable(connection, "year_dates")
  
  # 2 - remove existing values from the dataframe
  db_matches <-
    match(as.character(LOCAL_year_dates$id), DB_year_dates$id)
  num_df <- length(LOCAL_year_dates$id)
  
  to_insert <- filter(LOCAL_year_dates, is.na(db_matches))
  YEAR_DATES_inserted <- to_insert
  num_insert <- length(to_insert[[1]])
  num_dups <- num_df - num_insert
  
  # 3 - dbWriteTable to append the new values
  
  if (num_insert > 0) {
    RPostgreSQL::dbWriteTable(
      connection,
      "year_dates",
      to_insert,
      row.names = FALSE,
      append = TRUE,
      overwrite = FALSE
    )
    message(
      c(
        num_df,
        " YEAR_DATES in df, ",
        num_dups,
        " duplicates, ",
        num_insert,
        " inserted in DB"
      )
    )
  } else {
    message(
      c(
        num_df,
        " YEAR_DATES in df, ",
        num_dups,
        " duplicates, ",
        num_insert,
        " inserted in DB"
      )
    )
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
