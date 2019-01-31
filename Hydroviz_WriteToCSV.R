## WRITE CSVs TO DISK
WriteToCSV <- function() {
  res <- dlg_message("Would you like to save the key-value data file (.csv) to disk?", "yesno")$res
  
  if (res == "yes") {
    save_filename <- paste(file_path_no_extension, ".csv", sep="")
    
    msg_box(c("Writing to ", save_filename))
    write.csv(df_final, file=save_filename, row.names=FALSE)
    msg_box(c("Data file written to: ", save_filename))
  }
  
  res <- dlg_message("Would you like to save each table as a .csv?", "yesno")$res
  
  if (res == "yes") {
    
    save_filename <- paste(file_path_no_extension, "_ALTERNATIVES", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(alternatives, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_SOURCES", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(sources, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_TYPES", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(types, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_RIVERS", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(rivers, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_LOCATIONS", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(locations, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_DATA_BRIDGE_TABLE", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(data_bridge, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_MODEL_DATA_TABLE", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(model_data, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_YEAR_DATES", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(year_dates, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_MODEL_STATS", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(model_stats, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_MODEL_STATS_ALL", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(model_stats_all, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_MODELED_DATES", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(modeled_dates, file=save_filename, row.names=FALSE)
    
    save_filename <- paste(file_path_no_extension, "_MONTHS", ".csv", sep="")
    message(c("Writing to ", save_filename))
    write.csv(months, file=save_filename, row.names=FALSE)
  }
}