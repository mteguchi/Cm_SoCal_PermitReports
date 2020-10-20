#SD_Bay_Report

# Creates a history of captured turtles

# 16 June 2017


#rm(list=ls())

extract_SDB_data <- function(run.date, save.files = T){
  library(RODBC)
  library(tidyverse)
  
  save.files <- save.files
  out.file.name <- paste0('data/SDB_report_', run.date, '.csv')

  if (!file.exists(out.file.name)){
    
    # load a couple databases through ODBC
    turtle <- odbcConnect(dsn = 'turtle', uid = '', pwd = '')
    LIMS <- odbcConnect(dsn = 'LIMS', uid = '', pwd = '')
    
    turtle.tbls <- sqlTables(turtle)
    LIMS.tbls <- sqlTables(LIMS)
    
    # TO FIND ALL TABLE NAMES:
    turtle.tbls.names <- turtle.tbls$TABLE_NAME[grep(pattern = 'tbl_',
                                                     turtle.tbls$TABLE_NAME)]
    
    turtle.vw.names <- turtle.tbls$TABLE_NAME[grep(pattern = 'vw_',
                                                   turtle.tbls$TABLE_NAME)]
    
    LIMS.tbls.names <- LIMS.tbls$TABLE_NAME[grep(pattern = 'tbl_',
                                                 LIMS.tbls$TABLE_NAME)]
    
    LIMS.vw.names <- LIMS.tbls$TABLE_NAME[grep(pattern = 'vw_',
                                               LIMS.tbls$TABLE_NAME)]
    
    # get SD Bay results:
    turtle.SDB <- sqlQuery(turtle, 'select * from tbl_SD_Bay') %>%
      select(Turtle_ID, Year_caught,
             Month_caught, Day_caught, Caught_Dead,
             PIT_Tag_LFF, PIT_Tag_RFF, Inconel_Tag_LFF,
             Inconel_Tag_RFF, Sex, Weight_kg,
             Str_Carapace_Length_cm, Str_Carapace_Width_cm,
             Cur_Carapace_Length_cm, Cur_Carapace_Width_cm,
             Tetracycline_ml, Blood_Samples, Skin_Samples) %>%
      mutate(Date = as.Date(paste0(Year_caught, "-", Month_caught, "-", Day_caught)))
    
    #turtle.SDB <- turtle.SDB[!is.na(turtle.SDB$NMFS_Tag),]
    
    # haplotypes - this table is not found ... 9/20/2018
    haplos.turtles <- sqlQuery(LIMS, 'select * from vw_Sequence_Latest_Run')
    #haplos.turtles <- sqlQuery(LIMS, 'select * from tbl_Haplotype')
    
    # turtle archive:
    turtle.archive.tbl <- sqlQuery(turtle, 'select * from tbl_Turtle_Archive')
    
    # hormone databases
    turtle.testosterone.tbl <- sqlQuery(turtle, "select * from tbl_Testosterone")
    turtle.HormoneExtSample.tbl <-sqlQuery(turtle,
                                           "select * from tbl_Hormone_Extraction_Sample")
    turtle.HormoneProject.tbl <-sqlQuery(turtle,
                                         "select * from tbl_Hormone_Project")
    
    odbcClose(turtle)
    odbcClose(LIMS)
    
    # merge by lab_id
    turtle.archive <- left_join(turtle.archive.tbl,
                                haplos.turtles, by = "Lab_ID") 
    
    turtle.archive %>%
      filter(Species_Code == 'CM') %>%
      select(Turtle_ID, Year_collected, Month_collected, Day_collected, Haplotype) %>%
      na.omit() %>%
      transmute(Turtle_ID = Turtle_ID,
                Date = as.Date(paste0(Year_collected, "-", Month_collected, "-", Day_collected)),
                Haplotype = Haplotype) -> turtle.cm.archive
    
    # merge with SD Bay data
    turtle.haplo.SDB <- left_join(turtle.SDB,
                                  turtle.cm.archive,
                                  by = c('Turtle_ID', 'Date'))
    
    turtle.haplo.SDB <- turtle.haplo.SDB[with(turtle.haplo.SDB,
                                              order(Turtle_ID, Date)),] %>%
      
      transmute(Turtle_ID = Turtle_ID,
                PIT_LFF = PIT_Tag_LFF,
                PIT_RFF = PIT_Tag_RFF,
                Tag_LFF = Inconel_Tag_LFF,
                Tag_RFF = Inconel_Tag_RFF,
                Date = Date,
                # Yr = Year_caught,
                # Mo = Month_caught,
                # Da = Day_caught,
                Dead = Caught_Dead,
                Sex = Sex,
                Weight = Weight_kg,
                SCL = Str_Carapace_Length_cm,
                SCW = Str_Carapace_Width_cm,
                CCL = Cur_Carapace_Length_cm,
                CCW = Cur_Carapace_Width_cm,
                Oxytet = Tetracycline_ml,
                Blood = Blood_Samples,
                Skin = Skin_Samples,
                Haplo = Haplotype) %>%
      arrange(PIT_LFF, PIT_RFF)
    
    turtle.haplo.SDB <- turtle.haplo.SDB[!is.na(turtle.haplo.SDB$Turtle_ID),]
    
    # find how many years each turtle was found:
    # define a date column:
    turtle.haplo.SDB %>%
      group_by(Turtle_ID) %>% 
      arrange(Date) %>%
      summarize(firstCapture = first(Date),
                lastCapture = last(Date),
                n = n()) %>%
      arrange(firstCapture) %>%
      mutate(timeBetween = lastCapture - firstCapture) -> captureStats
    
    #turtle.haplo.SDB %>% select(-Turtle_ID) -> turtle.report
    
    if (save.files){
      write.csv(turtle.haplo.SDB,
                file = out.file.name,
                quote = F, row.names = F)
      
    }
    
  } else {
    col_def_SDB <- cols(Turtle_ID = col_integer(),
                       PIT_LFF = col_factor(),
                       PIT_RFF = col_factor(),
                       Tag_LFF = col_factor(),
                       Tag_RFF = col_factor(),
                       Date = col_date(format = "%Y-%m-%d"),
                       Dead = col_integer(),
                       Sex = col_factor(levels = c("F", "M", "U")),
                       Weight = col_double(),
                       SCL = col_double(),
                       SCW = col_double(),
                       CCL = col_double(),
                       CCW = col_double(),
                       Oxytet = col_double(),
                       Blood = col_integer(),
                       Skin = col_integer(),
                       Haplo = col_factor())
    turtle.haplo.SDB <- read_csv(file = out.file.name,
                                 col_types = col_def_SDB)

  }
  
  return(turtle.haplo.SDB)
}



