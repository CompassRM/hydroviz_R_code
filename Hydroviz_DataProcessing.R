# TO RUN - copy/paste the following into the command line (without commenting):
# setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
# source("Hydroviz_DataProcessing.R")
# source("Hydroviz_PushToPSQL.R")

ProcessHydrovizData <- function () {
  
  # FLAG
  save_tables <- "no"
  
  # NOTE - must set the working directory to the directory where this R file is run from! E.g.:
  # setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
  setwd("~/Box Sync/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")

    # Source files / functions
  source("Hydroviz_PushToPSQL.R")
  
  # # This will set the working path to the path of this file
  # this.dir <- dirname(parent.frame(2)$ofile)
  # setwd(this.dir)
  
  # CLEAR EVERYTHING AT THE START
  rm(list = ls())
  cat("\014")
  while (dev.cur() > 1)
    dev.off()
  
  # Check for packages - install if necessary
  if (!"openxlsx" %in% rownames(installed.packages())) {
    install.packages("openxlsx")
  }
  
  if (!"svDialogs" %in% rownames(installed.packages())) {
    install.packages("svDialogs")
  }
  
  if (!"lubridate" %in% rownames(installed.packages())) {
    install.packages("lubridate")
  }
  
  if (!"gdata" %in% rownames(installed.packages())) {
    install.packages("gdata")
  }
  
  # Load libraries
  library("lubridate")
  library("openxlsx")
  library("dplyr")
  library("svDialogs")
  library("gdata")
  

  # Declare global variables
  tables_list <- list()
  rawData <- data.frame()
  
  # Choose file and get path
  process_all <-
    dlg_message(
      "Would you like to process all of the files in this folder? Select YES to process ALL files in the folder. Select NO to process only the selected file.",
      "yesno"
    )$res
  
  file_path <-
    dlgOpen(
      getwd(),
      "Please select a file",
      multiple = FALSE,
      filters = dlg_filters["All",],
      gui = .GUI
    )$res
  
  insert_into_DB <-
    dlg_message("Would you like to push this data to the DB?", "yesno")$res
  
  # save_tables <-
  #   dlg_message("Would you like to export the data tables in a list for debugging?",
  #               "yesno")$res
  
  processing_start <- Sys.time()
  
  split_string <-
    strsplit(toString(file_path), "/", fixed = TRUE)[[1]]
  file_name <- split_string[length(split_string)]
  file_path_no_filename <-
    paste(split_string[1:length(split_string) - 1], collapse = '/')
  file_path_no_extension <-
    strsplit(toString(file_path), ".", fixed = TRUE)[[1]][1]
  
  # Get file extension
  file_extension <-
    strsplit(toString(file_path), ".", fixed = TRUE)[[1]][2]
  # NOTE: if there is a '.' in the username, then it will NOT grab the file extension properly!
  # Make this code more robust****
  
  # Create list of file names to process
  if (process_all == "no") {
    file_names <- file_name
    sprintf("The selected FILE is: %s", file_names)
  } else if (process_all == "yes") {
    file_names <-
      file.names <- dir(file_path_no_filename, pattern = ".xls")
    sprintf("The selected FOLDER is: %s", file_path_no_filename)
  }
  
  message(" ")
  message("-----------------------------------")
  message("FILE PROCESSING STARTED (",
          length(file_names),
          " files)")
  message("-----------------------------------")
  message(" ")
  
  ## -------------------------------------------------
  ## READ and PROCESS DATA for each file in file_names
  ## -------------------------------------------------
  
  for (i in 1:length(file_names)) {
    
    start_time <- Sys.time() # Start timing the loop
    
    # Declare/clear variables
    df_LIST <- list()
    df_ALL <- data.frame()
    df_column <- data.frame()
    
    # Get current file name and path
    file_name <- file_names[i]
    file_path <- paste(file_path_no_filename, file_name, sep = '/')
    
    ## READ DATA
    message("-----------------------------------------")
    message("Processing file: ",
            file_name,
            " (",
            i,
            " of ",
            length(file_names),
            ")")
    message("-----------------------------------------")
    
    if (file_extension == "xlsx") {
      rawData <- read.xlsx(file_path, sheet = 1, colNames  = FALSE)
    } else if (file_extension == "xls") {
      rawData <-
        read.xls(
          file_path,
          sheet = 1,
          verbose = FALSE,
          na.strings = c("NA", "#DIV/0!")
        )
    } else
      message ("FILE TYPE NOT SUPPORTED!")
    
    
    end_time <- Sys.time()
    elapsed_time <- difftime(end_time, start_time, units = "secs")
    
    # message("Time to read the file: ", round(elapsed_time[[1]], 2), " seconds")
    
    
    ## -------------------------------------------------
    ## PROCESS DATA
    ## -------------------------------------------------
    
    # How many rows of metadata?
    # *** Make this more robust ***
    meta_rows = 7 # CHECK THE ASSUMPTION THAT THE DATA WILL BE IN THE SAME FORMAT EVERY TIME!

    pb <-
      txtProgressBar(
        min = 1,
        max = length(names(rawData)),
        initial = 1,
        char = "=",
        width = NA,
        "title",
        "label",
        style = 3,
        file = ""
      )
    
    ## RESHAPE DATA
    for (j in 1:length(names(rawData)))
    {
      setTxtProgressBar(pb, j)
      
      # Ignore the first two columns (j = 1, 2)
      if (j >= 3)
      {
        # Get metadata
        # message(c("Processing Data Column ", j - 2, " of ", length(names(rawData)) -
        #             2))
        metadata <- rawData[1:meta_rows, j]
        
        # Get values
        tempValues <- data.frame(rawData[8:nrow(rawData), c(1, 2, j)])
        colnames(tempValues) <- c("id", "date", "value")
        
        # Transpose metadata
        meta_transposed <- data.frame(t(metadata))
        meta_transposed$source <- NA   # CAN I DROP STEPS LIKE THIS TO SAVE COMPUTATIONAL EFFORT??
        meta_names <-
          c(
            "river",
            "location",
            "type",
            "EMPTY",
            "alternative",
            "units",
            "measure",
            "source"
          )
        colnames(meta_transposed) <- meta_names
        
        # Find out if it is RES or RAS data
        # Get the first three letters of the alternative name. If RES then set source=RESERVOIR, if RAS then source=RIVER
        dataset_type <- substr(meta_transposed$alternative, 1, 3)
        
        if (dataset_type == "RAS" || dataset_type == "Ras" || dataset_type == "ras") {
          source = "RIVER"
          # message("* RAS Data *")
        } else if (dataset_type == "RES" | dataset_type == "Res" | dataset_type == "res") {
          source = "RESERVOIR"
          # message("* RES Data *")
        } else {
          source = "UNKNOWN"
          message("*** Unknown data source - not sure if it is RES or RAS ***")
        }
        
        # Add empty columns
        tempValues[, meta_names] <- NA
        
        # Add transposed metadata to each row of values
        tempValues[, 4:length(tempValues)] <- meta_transposed
        tempValues[, "source"] <- source
        
        # Add alternative to the dataframe
        df_column <- rbind(df_column, tempValues)
      }
      close(pb)
    }
    
    
    
    # Update the rownames - should be numbered from 1:nrow(df_column)
    rownames(df_column) <- seq(length = nrow(df_column))
    
    # Reorder columns
    df_reordered <-
      df_column[c(
        "id",
        "alternative",
        "type",
        "source",
        "river",
        "location",
        "date",
        "value",
        "units",
        "measure",
        "EMPTY"
      )]
    
    # Fix date format
    
    if (typeof(df_reordered[1,"date"]) == "character") {
      # If date is type character, need to convert to numeric first
      df_reordered[, "date"] <- as.numeric(df_reordered[, "date"])
    } 
    
    df_reordered[, "date"] <-
      as.Date(df_reordered[, "date"] , origin = "1899-12-30")
    
    ## REMOVE LEAP DAYS AND 1930 (INCOMPLETE YEAR)
    df_no1930 <-
      df_reordered[lubridate::year(df_reordered$date) != 1930, ]
    df_final <-
      df_no1930[!(lubridate::month(df_no1930$date) == 2 &
                    lubridate::day(df_no1930$date) == 29), ]
    
    df_final$location <- as.character(df_final$location)
    
    # Renumber ID column
    rownames(df_final) <- seq(length = nrow(df_final))
    df_final$id <- rownames(df_final)
    
    # Convert EMPTY value fields to NaN for Postgres
    
    df_final$value[is.na(df_final$value)] <- NaN
    
    
    # Bind it to df_ALL and add it in df_LIST
    df_ALL <- rbind(df_ALL, df_final)
    df_LIST[[i]] <-
      df_final  # For debugging purposes only - comment out or delete
    
    end_time <- Sys.time()
    elapsed_time <-
      difftime(end_time, processing_start, units = "secs")
    
    message(" ")
    message("-----------------------------")
    message(c("Finished processing '", file_name, "'. " ,i, "/", length(file_names), " files processed. Elapsed time: ",
              round(elapsed_time[[1]], 2),
              " seconds."))
    message("-----------------------------")

    ## INSERT in DB
    if (insert_into_DB == "yes") {
      tables_list <- PushToPSQL(df_ALL)
      # message(c("Finished processing '", file_name, "'. " ,i, "/", length(file_names), " files processed"))
    }
    
    # Add the data frames created in this R file to the tables_list for debugging purposes
    # If tables_list is empty (length=0) when returning from PushToPSQL, that means the user
    # indicated they did NOT want to export/save those tables. So don't add anything to the tables from here.
    
    # NOTE: tables_list$alternatives will access the alternatives dataframe
    
    
    if (length(tables_list) != 0) {
      message("Adding tables to tables_list")
      tables_list$df_ALL <- df_ALL
      tables_list$df_LIST <- df_LIST
      tables_list$rawData <- rawData
      tables_list$file_names <- file_names
    }
    
    # CURRENTLY, THIS CODE WITH KEEP ONLY THE DATA FROM THE LAST LOOP.  TOO BIG TO APPEND THE tables_lists.
    # Change this so that it SAVES the tables to disk (prompt first)
  }
  

  ## PROCESSING COMPLETE - display message
  end_time <- Sys.time()
  elapsed_time <- difftime(end_time, start_time, units = "secs")
  
  message(" ")
  message("----------------------------------------------")
  message(
    c(
      "DATA PROCESSING COMPLETE. Elapsed time: ",
      round(elapsed_time[[1]], 2),
      " seconds. ",
      length(file_names),
      " file(s) processed."
    )
  )
  message("----------------------------------------------")
  message(" ")
  
  
  return(tables_list)
  
}
