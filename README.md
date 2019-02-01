# hydroviz_R_code

To start:

In Hydroviz_DataProcessing.R: 
Set the working directory to the directory where the R scripts are located:
e.g., 

  setwd("~/Box/P722 - MRRIC AM Support/Working Docs/P722 - HydroViz/hydroviz_R_code")

Then in the R Studio prompt, write:

  source("Hydroviz_DataProcessing.R")
  ProcessHydrovizData()

The first line 'sources' or 'runs' the file, which declares the function ProcessHydrovizData in the R environment
The second line runs that function.

The script will ask you whether you want to process ALL of files in a directory, or just one.  Select 'YES' to process all of them, or 'NO' to process just one.

The script will then ask you to select a file.  

Next, you will be asked whether you want to push the data to the database - click YES.

The script will process each XLSX file one at a time. It will first read the file and build a data frame, then it will run a function from the file 'Hydroviz_PushToPSQL.R' that will process the tables and push them one by one into the database specified in the .env file.
-- The DATA and STATS table take a LONG time to process!

