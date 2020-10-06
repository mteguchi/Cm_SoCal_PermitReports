#SD_Bay_Report

# Creates a history of captured turtles

# 16 June 2017


rm(list=ls())
library(RODBC)
library(dplyr)

save.files <- T
source("C:/Users/Tomo.Eguchi/Documents/R/tools/TomosFunctions.R")

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
            file = paste0('data/SDB_report_', Sys.Date(), '.csv'),
            quote = F, row.names = F)

}




