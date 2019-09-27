# TO RUN - copy/paste the following into the command line (without commenting):
# setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
# source("Hydroviz_DataProcessing.R")
# source("Hydroviz_PushToPSQL_v2.R")

ProcessHydrovizData <- function () {
  # NOTE - must set the working directory to the directory where this R file is run from! E.g.:
  # setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
  setwd("~/Box Sync/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")

  
  # # This will set the working path to the path of this file
  # this.dir <- dirname(parent.frame(2)$ofile)
  # setwd(this.dir)
  
  # CLEAR EVERYTHING AT THE START
  rm(list = ls())
  cat("\014")
  while (dev.cur() > 1)
    dev.off()
  
  # Source files / functions
  source("Hydroviz_PushToPSQL_v2.R")
  
  # Set flags
  save_processed_data = FALSE
  
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
  rawData <- data.frame()
  ALL_processed_data <- list()
  
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
      filters = dlg_filters["All", ],
      gui = .GUI
    )$res
  
  
  ## INSERT INTO DB DIALOGUE
  
  insert_into_DB <- ""
  DB_temp <- ""
  DB_confirm <- ""
  DB_selected <- ""
  cancel_DB <- ""
  select_DB_flag <- FALSE
  
  insert_into_DB <-
    dlg_message("Would you like to push this data to a DB?", "yesno")$res
  
  choices <- list("Production DB (AWS)", "Testing DB (AWS)", "Development DB (local)")
  
  DBList <-
    function() {
      dlg_list(
        choices,
        preselect = NULL,
        multiple = FALSE,
        title = NULL,
        gui = .GUI
      )$res
    }
  
  if (insert_into_DB == "yes") {
    select_DB_flag = TRUE
  }
  
  while (select_DB_flag) {
    DB_temp <- DBList()
    
    if (!is.na(DB_temp[1])) {
      # A DB was selected
      DB_confirm <-
        dlg_message(paste("You selected *", DB_temp, "* Is this correct?"),
                    "yesno")$res
      
      if (DB_confirm == "yes") {
        DB_selected <- DB_temp
        message("Will push to DB: ", DB_selected)
        select_DB_flag <- FALSE
      }
    } else {
      # 'CANCEL' was selected - confirm
      cancel_DB <-
        dlg_message(paste("Would you like to CANCEL pushing this data to a DB?"),
                    "yesno")$res
      if (cancel_DB == "yes") {
        insert_into_DB <- "no"
        select_DB_flag <- FALSE
      }
    }
  }

  
  ## START PROCESSING DATA
  
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
    
    message("Time to read the file: ", round(elapsed_time[[1]], 2), " seconds")
    
    
    ## -------------------------------------------------
    ## PROCESS DATA
    ## -------------------------------------------------
    
    # How many rows of metadata?
    # *** Make this more robust ***
    meta_rows = 7 # CHECK THE ASSUMPTION THAT THE DATA WILL BE IN THE SAME FORMAT EVERY TIME!
    
    # pb <-
    #   txtProgressBar(
    #     min = 1,
    #     max = length(names(rawData)),
    #     initial = 1,
    #     char = "=",
    #     width = NA,
    #     "title",
    #     "label",
    #     style = 3,
    #     file = ""
    #   )
    
    ## RESHAPE AND INSERT DATA
    for (j in 1:length(names(rawData)))
    {
      # setTxtProgressBar(pb, j)
      
      # Ignore the first two columns (j = 1, 2)
      if (j >= 3)
      {
        # RESHAPE THE DATA
        
        # Declare/clear variables
        df_column <- data.frame()
        df_final <- data.frame()
        
        # Get metadata
        # message(c("Processing Data Column ", j - 2, " of ", length(names(rawData)) -
        #             2))
        metadata <- rawData[1:meta_rows, j]
        
        # Get values
        tempValues <-
          data.frame(rawData[8:nrow(rawData), c(1, 2, j)])
        colnames(tempValues) <- c("id", "date", "value")
        
        # Transpose metadata
        meta_transposed <- data.frame(t(metadata))
        meta_transposed$source <-
          NA   # CAN I DROP STEPS LIKE THIS TO SAVE COMPUTATIONAL EFFORT??
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
        
        if (dataset_type == "RAS" ||
            dataset_type == "Ras" || dataset_type == "ras") {
          source = "RIVER"
          # message("* RAS Data *")
        } else if (dataset_type == "RES" |
                   dataset_type == "Res" | dataset_type == "res") {
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
        
        
        # REORGANIZE THE DATA
        
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
        
        if (typeof(df_reordered[1, "date"]) == "character") {
          # If date is type character, need to convert to numeric first
          df_reordered[, "date"] <-
            as.numeric(df_reordered[, "date"])
        }
        
        df_reordered[, "date"] <-
          as.Date(df_reordered[, "date"] , origin = "1899-12-30")
        
        ## REMOVE DATA FROM 1930
        # df_no1930 <-
        #   df_reordered[lubridate::year(df_reordered$date) != 1930,]
        
        ## REMOVE LEAP DAYS
        # df_final <-
        #   df_no1930[!(lubridate::month(df_no1930$date) == 2 &
        #                    lubridate::day(df_no1930$date) == 29),]
        
        df_final <-
          df_reordered[!(lubridate::month(df_reordered$date) == 2 &
                        lubridate::day(df_reordered$date) == 29),]
        
        df_final$location <- as.character(df_final$location)
        
        # Renumber ID column
        rownames(df_final) <- seq(length = nrow(df_final))
        df_final$id <- rownames(df_final)
        
        # Convert EMPTY value fields to NaN for Postgres
        
        df_final$value[is.na(df_final$value)] <- NaN
        
        
        # Add the processed column of data from rawData to processed_data_list
        if (save_processed_data) {
          ALL_processed_data <- c(ALL_processed_data, list(df_final))
        }
        
        # NOTE - this only packages the data for the columns in the current xlsx file
        # Doesn't package data for all xlsx files
        
        # # Bind it to df_ALL
        # df_ALL <- rbind(df_ALL, df_final)
        
        ## INSERT DATA IN DB
        if (insert_into_DB == "yes") {
          PushToPSQL(df_final, DB_selected)
          # message(c("Finished processing '", file_name, "'. " ,i, "/", length(file_names), " files processed"))
        }
        
      }
      # close(pb)
    }
    
    end_time <- Sys.time()
    elapsed_time <-
      difftime(end_time, processing_start, units = "secs")
    
    message(" ")
    message("-----------------------------")
    message(
      c(
        "Finished processing '",
        file_name,
        "'. " ,
        i,
        "/",
        length(file_names),
        " files processed. Elapsed time: ",
        round(elapsed_time[[1]], 2),
        " seconds."
      )
    )
    message("-----------------------------")
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
  
  # FOR DEBUGGING WHEN USING PushToPSQL - comment it out if not debugging
  df <- df_final
  
  return()
  
}
