# Access database and extract data from the study period:

rm(list=ls())
library(RODBC)
library(tidyverse)
library(reshape2)

# load a couple databases through ODBC

turtle <- odbcConnect(dsn = 'Turtle', uid = '', pwd = '')

turtle.info <- odbcGetInfo(turtle)
turtle.tbls <- sqlTables(turtle)

# TO FIND ALL TABLE NAMES:
turtle.tbls.names <- turtle.tbls$TABLE_NAME[grep(pattern = 'tbl_',
                                                 turtle.tbls$TABLE_NAME)]

turtle.vw.names <- turtle.tbls$TABLE_NAME[grep(pattern = 'vw_',
                                               turtle.tbls$TABLE_NAME)]

turtle.SDB <- sqlQuery(turtle, 'select * from tbl_SD_Bay') %>%
  select('Turtle_ID', 'Year_caught',
         'Month_caught', 'Day_caught', 'Caught_Dead',
         'PIT_Tag_LFF', 'PIT_Tag_RFF', 'Inconel_Tag_LFF',
         'Inconel_Tag_RFF', 'Sex', 'Weight_kg',
         'Str_Carapace_Length_cm', 'Str_Carapace_Width_cm',
         'Cur_Carapace_Length_cm', 'Cur_Carapace_Width_cm',
         'Tetracycline_ml')


# turtle.SDB <- turtle.SDB[, c('NMFS_Tag', 'Turtle_ID', 'Year_caught',
#                              'Month_caught', 'Day_caught', 'Caught_Dead',
#                              'PIT_Tag_LFF', 'PIT_Tag_RFF', 'Inconel_Tag_LFF',
#                              'Inconel_Tag_RFF', 'Sex', 'Weight_kg',
#                              'Str_Carapace_Length_cm', 'Str_Carapace_Width_cm',
#                              'Cur_Carapace_Length_cm', 'Cur_Carapace_Width_cm',
#                              'Tetracycline_ml')]

#turtle.SDB <- turtle.SDB[!is.na(turtle.SDB$NMFS_Tag),]

#net.SDB <- sqlQuery(turtle, 'select * from tbl_Net_Info')

odbcClose(turtle)

# extract just for the CMR sampling:
turtle.SDB %>% filter(Year_caught > 2019) %>% 
  filter(Month_caught >= 5 & Month_caught < 7) %>%
  mutate(Date = paste0(Year_caught, "-", 
                       Month_caught, "-", 
                       Day_caught)) %>%
  mutate(Date = as.Date(Date)) %>% #-> turtle.SDB.CMR#
  mutate(PIT = ifelse(is.na(PIT_Tag_LFF), 
                      as.character(PIT_Tag_RFF), 
                      as.character(PIT_Tag_LFF))) %>%
  transmute(ID = PIT, 
            Date = Date, 
            CCL_cm = Cur_Carapace_Length_cm,
            detect = 1) -> turtle.SDB.CMR
  
#Convert them into 0-1
tmp <- melt(turtle.SDB.CMR,
            id.vars = c("ID", "Date"),
            measure.vars = "detect")

dat.01 <- dcast(tmp,
               formula = ID ~ Date,
               fun.aggregate = length)

turtle.SDB.CMR %>% group_by(ID) %>%
  summarise(CCL_cm = first(CCL_cm)) -> first.CCL.CMR

dat.01 %>% left_join(first.CCL.CMR, by = "ID") -> CMR_01_size

#turtle.SDB.CMR[is.na(turtle.SDB.CMR$Turtle_ID), 'Turtle_ID'] <- 999999
# write_csv in readr is a better choice than write.csv in R base.
write_csv(CMR_01_size,
          path = paste0('data/CMR_size_data_',
                        Sys.Date(), '.csv'))
