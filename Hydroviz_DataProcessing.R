# TO RUN - copy/paste the following into the command line (without commenting):
# source("Hydroviz_DataProcessing.R")
# ProcessHydrovizData()

ProcessHydrovizData <- function () {

  setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")
  
  # # This will set the working path to the path of this file
  # this.dir <- dirname(parent.frame(2)$ofile)
  # setwd(this.dir)
  
  # CLEAR EVERYTHING AT THE START
  rm(list=ls())
  cat("\014")
  while (dev.cur()>1) dev.off()
  
  # Check for packages - install if necessary
  if(!"openxlsx" %in% rownames(installed.packages())) {
    install.packages("openxlsx")
  }
  
  if(!"svDialogs" %in% rownames(installed.packages())) {
    install.packages("svDialogs")
  }
  
  if(!"lubridate" %in% rownames(installed.packages())) {
    install.packages("lubridate")
  }
  
  if(!"gdata" %in% rownames(installed.packages())) {
    install.packages("gdata")
  }
  
  # Load libraries
  library("lubridate")
  library("openxlsx")
  library("dplyr")
  library("svDialogs")
  library("gdata")
  
  # Source files / functions
  source("Hydroviz_PushToPSQL.R")
  # source("Hydroviz_WriteToCSV.R")
  
  # Declare global variables
  tables_list <- list()
  rawData <- data.frame()
  
  
  # Choose file and get path
  process_all <- dlg_message("Would you like to process all of the files in this folder? Select YES to process ALL files in the folder. Select NO to process only the selected file.", "yesno")$res
  
  file_path <- dlgOpen(getwd(), "Please select a file", multiple = FALSE, filters = dlg_filters["All", ],
           gui = .GUI)$res
  
  insert_into_DB <- dlg_message("Would you like to push this data to the DB?", "yesno")$res
  
  save_tables <- dlg_message("Would you like to export the data tables in a list for debugging?", "yesno")$res
  
  processing_start <- Sys.time()
  
  split_string <- strsplit(toString(file_path), "/", fixed = TRUE)[[1]]
  file_name <- split_string[length(split_string)]
  file_path_no_filename <- paste(split_string[1:length(split_string)-1], collapse = '/')
  file_path_no_extension <-strsplit(toString(file_path), ".", fixed = TRUE)[[1]][1]
  
  # Get file extension
  file_extension <- strsplit(toString(file_path), ".", fixed = TRUE)[[1]][2]
   # NOTE: if there is a '.' in the username, then it will not grab the file extension properly!
  # Make this code more robust
  
  # Create list of file names to process
  if (process_all == "no") {
    file_names <- file_name
    sprintf("The selected FILE is: %s", file_names)
  } else if (process_all == "yes") {
    file_names <- file.names <- dir(file_path_no_filename, pattern =".xls")
    sprintf("The selected FOLDER is: %s", file_path_no_filename)
  }
  

  
  ## READ and PROCESS DATA for each file in file_names
  
  for (i in 1:length(file_names)) {
    # Declare/clear variables
    df_LIST <- list()
    df_ALL <- data.frame()
    df_column <- data.frame()
    
    
    file_name <- file_names[i]
    file_path <- paste(file_path_no_filename, file_name, sep='/')
    
    ## READ DATA
    message("Reading file: ", file_name, " (", i, " of ", length(file_names), ")")
    
    start_time <- Sys.time()
  
    if (file_extension == "xlsx") {
      rawData <- read.xlsx(file_path, sheet = 1, colNames  = FALSE)
    } else if (file_extension == "xls") {
      rawData <- read.xls(file_path, sheet=1, verbose=FALSE, na.strings=c("NA","#DIV/0!"))
    } else message ("FILE TYPE NOT SUPPORTED!")
  
    end_time <- Sys.time()
    
    elapsed_time <- difftime(end_time, start_time, units="secs")
  
    message("Time: ", round(elapsed_time[[1]],2), " seconds")
  
    ## PROCESS DATA

    
    # How many rows of metadata?
    # *** Make this more robust ***
    meta_rows = 7 # CHECK THE ASSUMPTION THAT THE DATA WILL BE IN THE SAME FORMAT EVERY TIME!
    # source = "RIVER" # Use RIVER for RAS data and RESERVOIR for RES data 
    
    ## RESHAPE DATA
    for (j in 1:length(names(rawData)))
    {
      # Ignore the first two columns (j = 1, 2)
      if(j >= 3)
      {
        # Get metadata
        message(c("Processing Data Column ", j-2, " of ", length(names(rawData))-2))
        metadata <- rawData[1:meta_rows,j]
        
        # Get values
        tempValues <- data.frame(rawData[8:nrow(rawData), c(1,2,j)])
        colnames(tempValues) <- c("id", "date", "value")
        
        # Transpose metadata
        meta_transposed <- data.frame(t(metadata))
        meta_transposed$source <- NA   # CAN I DROP STEPS LIKE THIS TO SAVE COMPUTATIONAL EFFORT??
        meta_names <- c("river", "location", "type", "EMPTY", "alternative", "units", "measure", "source")
        colnames(meta_transposed) <- meta_names
        
        # Find out if it is RES or RAS data
        # Get the first three letters of the alternative name. If RES then set source=RESERVOIR, if RAS then source=RIVER
        
        dataset_type <- substr(meta_transposed$alternative, 1, 3)
        
        if (dataset_type == "RAS") {
          source = "RIVER"
          message("* RAS Data *")
        } else if (dataset_type == "RES") {
          source = "RESERVOIR"
          message("* RES Data *")
        } else {
          source = "UNKNOWN"
          message("*** Unknown data source - not sure if it is RES or RAS ***")
        }
      
        # Add empty columns
        tempValues[,meta_names] <- NA
        
        # Add transposed metadata to each row of values
        tempValues[,4:length(tempValues)] <- meta_transposed
        tempValues[,"source"] <- source
        
        # Add alternative to the dataframe
        df_column <- rbind(df_column, tempValues)
      }
    }
    
    # Update the rownames - should be numbered from 1:nrow(df_column)
    rownames(df_column) <- seq(length=nrow(df_column)) 
    
    # Reorder columns
    df_reordered <-df_column[c("id","alternative","type","source","river","location","date","value","units","measure","EMPTY")]
    
    # Fix date format
    df_reordered[,"date"] <- as.Date(df_reordered[,"date"] , origin="1899-12-30") 
    
    ## REMOVE LEAP DAYS AND 1930 (INCOMPLETE YEAR)
    df_no1930 <- df_reordered[lubridate::year(df_reordered$date) != 1930,]
    df_final <- df_no1930[!(lubridate::month(df_no1930$date)==2 & lubridate::day(df_no1930$date)==29),]
    
    df_final$location <- as.character(df_final$location)
    
    # Renumber ID column
    rownames(df_final) <- seq(length=nrow(df_final)) 
    df_final$id <- rownames(df_final)
    
    df_ALL <- rbind(df_ALL, df_final)
    df_LIST[[i]] <- df_final  # For debugging purposes only - comment out or delete

    
    end_time <- Sys.time()
    elapsed_time <- difftime(end_time, processing_start, units="secs")
    
    # disp_message <- paste("Data processing complete. Elapsed time: ", round(elapsed_time[[1]],2), " seconds. ", length(file_names)," file(s) processed.")
    # dlg_message(disp_message, gui = .GUI)
    message(c("Finished processing ", i , " of ", length(file_names), " files. Elapsed time: ", round(elapsed_time[[1]],2), " seconds."))
    
    ## INSERT in DB
    if (insert_into_DB == "yes") {
      message("*** INSERTING INTO DB ***")
      tables_list <- PushToPSQL(df_ALL, save_tables)
      # NOTE: tables_list$alternatives will access the alternatives dataframe
    }
    
    # Add the data frames created in this R file to the tables_list for debugging purposes
    # If tables_list is empty (length=0) when returning from PushToPSQL, that means the user
    # indicated they did NOT want to export/save those tables. So don't add anything to the tables from here.
    
    if(length(tables_list) != 0) {
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
  elapsed_time <- difftime(end_time, processing_start, units="secs")
  
  # disp_message <- paste("Data processing complete. Elapsed time: ", round(elapsed_time[[1]],2), " seconds. ", length(file_names)," file(s) processed.")
  # dlg_message(disp_message, gui = .GUI)
  message(c("Data processing complete. Elapsed time: ", round(elapsed_time[[1]],2), " seconds. ", length(file_names)," file(s) processed."))
  message("FINISHED!")
  
  
  
  return(tables_list)
  
}


