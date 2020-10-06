#SealBeach_Report

# Creates a history of captured turtles at Seal Beach National Wildlife Refuge

# 7 October 2019 - modified from San Diego Bay report script
# 16 June 2017


rm(list=ls())
library(RODBC)
library(tidyverse)

save.files <- T
#source("C:/Users/Tomo.Eguchi/Documents/R/tools/TomosFunctions.R")

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
turtle.SB <- sqlQuery(turtle, 'select * from vw_San_Gabriel_Capture') %>%
  select(PIT_Max_Tag_RFF, PIT_Max_Tag_LFF, Flipper_Max_Tag_RFF,
         Flipper_Max_Tag_LFF, Capture_Year, Capture_Month, Capture_Day, 
         Capture_Latitude, Capture_Longitude, Sex, SCL_cm, SCW_cm, Body_Depth, 
         CCL_cm, CCW_cm, Weight_kg, Sat_Tags, Sat_Max_Tag, Skin_Samples,
         Blood_Samples, Tetracycline_ml, Turtle_ID) %>%
  mutate(Date = as.Date(paste0(Capture_Year, "-", Capture_Month, "-", Capture_Day)))

# haplotypes - this table is not found ... 9/20/2018
#haplos.turtles <- sqlQuery(LIMS, 'select * from tbl_Haplotype')
haplos.turtles <- sqlQuery(LIMS, 'select * from vw_Sequence_Latest_Run')

# turtle archive:
turtle.archive.tbl <- sqlQuery(turtle, 'select * from tbl_Turtle_Archive')

# hormone databases
# turtle.testosterone.tbl <- sqlQuery(turtle, "select * from tbl_Testosterone")
# turtle.HormoneExtSample.tbl <-sqlQuery(turtle,
#                                        "select * from tbl_Hormone_Extraction_Sample")
# turtle.HormoneProject.tbl <-sqlQuery(turtle,
#                                      "select * from tbl_Hormone_Project")

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
            # Year_caught = Year_collected,
            # Month_caught = Month_collected,
            # Day_caught = Day_collected,
            Haplotype = Haplotype) -> turtle.cm.archive

# merge with SD Bay data
turtle.haplo.SB <- left_join(turtle.SB,
                             turtle.cm.archive,
                             by = c('Turtle_ID', 'Date'))

turtle.haplo.SB <- turtle.haplo.SB[with(turtle.haplo.SB,
                                        order(Turtle_ID, Date)),] %>%
  
  transmute(Turtle_ID = Turtle_ID,
            Date = Date,
            PIT_LFF = PIT_Max_Tag_LFF,
            PIT_RFF = PIT_Max_Tag_RFF,
            Tag_LFF = Flipper_Max_Tag_LFF,
            Tag_RFF = Flipper_Max_Tag_RFF,
            Sex = Sex,
            SCL = SCL_cm,
            SCW = SCW_cm,
            Body_Depth = Body_Depth,
            CCL = CCL_cm,
            CCW = CCW_cm,
            Weight = Weight_kg,
            Oxytet = Tetracycline_ml,
            Blood = Blood_Samples,
            Skin = Skin_Samples,
            Sat_Tags,
            Sat_Max_Tag,
            Haplo = Haplotype) %>%
  arrange(PIT_LFF, PIT_RFF)

# find how many years each turtle was found:
# define a date column:
turtle.haplo.SB %>%
  group_by(Turtle_ID) %>% 
  arrange(Date) %>%
  summarize(ID = first(Turtle_ID),
            firstCapture = first(Date),
            lastCapture = last(Date),
            n = n()) %>%
  arrange(firstCapture) %>%
  mutate(timeBetween = lastCapture - firstCapture) -> captureStats

#turtle.haplo.SB %>% select(-Turtle_ID) -> turtle.report

if (save.files){
  write.csv(turtle.haplo.SB,
            file = paste0('data/SB_report_', Sys.Date(), '.csv'),
            quote = F, row.names = F)
  
}




