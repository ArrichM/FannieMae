library(tidyverse)
library(foreach)
library(parallel)
library(magrittr)
library(data.table)
library(zoo)


## ZIP CODES -------------------------------------------------------------------
ZipCodeSourceFile = "http://download.geonames.org/export/zip/US.zip"
temp <- tempfile()
download.file(ZipCodeSourceFile , temp)
ZipCodes <- read.table(unz(temp, "US.txt"), sep="\t")
unlink(temp)
names(ZipCodes) = c("CountryCode", "zip", "PlaceName", 
                    "AdminName1", "AdminCode1", "AdminName2", "AdminCode2", 
                    "AdminName3", "AdminCode3", "latitude", "longitude", "accuracy")

zip_codes <- ZipCodes %>% 
  as_tibble() %>% 
  filter(CountryCode == "US") %>%
  select(AdminCode1, AdminName1) %>% 
  mutate_all(as.character) %>% 
  rename(STATE = AdminCode1) %>% 
  filter(!STATE %in% c("PR", "MH", "VI", "")) %>% 
  bind_rows(tibble(STATE = c("PR", "MH", "VI"), 
                   AdminName1 = c("Puerto Rico", "Marshall Islands", "Virgin Islands"))) %>%
  distinct() %>% 
  as.data.table()




## MACRO DATA ------------------------------------------------------------------

library(fredr)
fredr_set_key("8a00d77e449540f7ec2310937f6d0eed")
sleep <- function(time, ...) Sys.sleep(time)
# get ids o fdesired data
macro_frame <- zip_codes %>% 
  select(AdminName1) %>% rename(US_STATE = AdminName1) %>% 
  mutate(gdp = paste("Total Gross Domestic Product for", US_STATE),
         hpi = paste("All-Transactions House Price Index for", US_STATE),
         unemp =paste("Unemployment Rate in", US_STATE)) %>%
  mutate(id_gdp = map(gdp, ~fredr_series_search_text(search_text = .x, order_by = "popularity", 
                                                    limit = 50, sort_order = "desc", filter_variable = c("frequency"), filter_value = "Quarterly")))
Sys.sleep(10)
macro_frame <- macro_frame %>% 
  mutate(id_hpi = map(hpi, ~fredr_series_search_text(search_text = .x, order_by = "popularity", 
                                                    limit = 50, sort_order = "desc", filter_variable = c("frequency"), filter_value = "Quarterly")))
Sys.sleep(60)
macro_frame <- macro_frame %>% 
  mutate(id_unemp = map(unemp, ~fredr_series_search_text(search_text = .x, order_by = "popularity", 
                                                       limit = 50, sort_order = "desc", filter_variable = c("frequency"), filter_value = "Monthly")))
# Wait to avoid API rate limit
# select rigth series
macro_frame$id_gdp <- map(macro_frame$id_gdp, ~filter(.x, seasonal_adjustment_short == "SAAR" & grepl("GSP", id))[1,])
macro_frame$id_hpi <- map(macro_frame$id_hpi, ~filter(.x, grepl("STHPI", id))[1,])
macro_frame$id_unemp <- map(macro_frame$id_unemp, ~filter(.x, seasonal_adjustment_short == "SA" & grepl("UR", id))[1,])
macro_frame <- macro_frame %>% drop_na()
# download and transform data
macro_frame$series_gdp <- map(macro_frame$id_gdp, ~fredr(series_id = .x$id) %>% 
                                mutate(GDP = c(rep(NA, 1), diff(value, 1)) / dplyr::lag(value, 1)))
Sys.sleep(60)
macro_frame$series_hpi <- map(macro_frame$id_hpi, ~fredr(series_id = .x$id) %>% 
                                mutate(HPI = c(rep(NA, 1), diff(value, 1)) / dplyr::lag(value, 1), observation_start = as.Date("2003-01-01")))
Sys.sleep(60)
macro_frame$series_unemp <- map(macro_frame$id_unemp, ~fredr(series_id = .x$id, observation_start = as.Date("2003-01-01")))
# merge to one large tibble
gdp_frame <-  macro_frame %>% select(US_STATE, series_gdp) %>% unnest(series_gdp)
hpi_frame <-  macro_frame %>% select(US_STATE, series_hpi) %>% unnest(series_hpi)
macro_frame_join <- macro_frame %>% select(US_STATE, series_unemp) %>% unnest(series_unemp) 
macro_frame_join <- macro_frame_join %>% 
  left_join(gdp_frame %>% select(date, US_STATE, GDP), by = c("date", "US_STATE")) %>% 
  left_join(hpi_frame %>% select(date, US_STATE, HPI), by = c("date", "US_STATE"))


# filter to final frame
macro_frame_final <- macro_frame_join %>% 
  filter(date < as.Date("2019-10-01")) %>% 
  group_by(US_STATE) %>% 
  mutate(GDP = zoo::na.locf(GDP, na.rm = FALSE),
         HPI = zoo::na.locf(HPI, na.rm = FALSE)) %>% 
  ungroup() %>% 
  rename(UNEMP = value, ORIG_DTE = date) %>%
  select(-series_id) %>%
  drop_na() %>% as.data.table()

rm("gdp_frame", "macro_frame", "ZipCodes", "macro_frame_join")






## PREPARE DATA ------------------------------------------------------------------


files <- list.files("/Users/Max/Desktop/FannieMae 1/Rda", pattern = "FNMA_Data_",full.names = T)


aquis_vars <- c("LOAN_ID", "ORIG_CHN", "Seller.Name", "ORIG_RT", "ORIG_AMT", "ORIG_TRM", "ORIG_DTE"
                ,"FRST_DTE", "OLTV", "OCLTV", "NUM_BO", "DTI", "CSCORE_B", "FTHB_FLG", "PURPOSE", "PROP_TYP"
                ,"NUM_UNIT", "OCC_STAT", "STATE", "ZIP_3", "MI_PCT", "Product.Type", "CSCORE_C", "MI_TYPE", "RELOCATION_FLG")

additional_vars <- c("MSA", "F180_DTE", "LAST_DTE", "NET_LOSS", "DISP_DT", "LAST_STAT")

keep_vars <- c(aquis_vars, additional_vars)

cl <- makePSOCKcluster(4, out.file = "")
doParallel::registerDoParallel(cl)


csvs <- foreach(i = 1:length(files), .combine = rbind, .packages = c("data.table", "zoo")) %dopar% {
  
  # Load Data
  print(i)
  load(files[[i]])
  
  # Format
  keep_vars <- keep_vars[keep_vars %in% colnames(Combined_Data)]
  Combined_Data <- Combined_Data[, ..keep_vars]
  Combined_Data[, LGD:= NET_LOSS / ORIG_AMT]
  Combined_Data[, c("ORIG_DTE", "FRST_DTE"):= lapply(.SD, function(x) zoo::as.Date(zoo::as.yearmon(x, "%m/%Y" ))), 
                .SDcols = c("ORIG_DTE", "FRST_DTE")]
  
  Combined_Data[, c("LAST_DTE", "F180_DTE", "DISP_DT"):= lapply(.SD, function(x) zoo::as.Date(x)), 
                .SDcols = c("LAST_DTE","F180_DTE", "DISP_DT")]
  # Default Indicators
  CreditEvents <- c("F", "S", "T", "N")
  # Default everything with event or unpaid bills
  Combined_Data[, DEF:= ifelse(!is.na(F180_DTE) | LAST_STAT %in% CreditEvents, 1, 0)]
  Combined_Data[zip_codes, on = 'STATE', US_STATE := i.AdminName1] 
  # Exclude non US States like militry zip codes
  Combined_Data <- Combined_Data[!is.na(US_STATE)  | US_STATE != "", ] 
  # When did the default occur?
  Combined_Data[, DEF_DTE := ifelse(DEF == 1 & !is.na(F180_DTE), as.character(F180_DTE), ifelse(DEF == 1, as.character(LAST_DTE), NA))]
  # Time to default
  Combined_Data[, DEF_TIME:= as.integer(difftime(DEF_DTE, ORIG_DTE))]
  # add macro variables
  Combined_Data[macro_frame_final, on = c("US_STATE", "ORIG_DTE"), GDP := GDP] 
  Combined_Data[macro_frame_final, on = c("US_STATE", "ORIG_DTE"), HPI := HPI] 
  Combined_Data[macro_frame_final, on = c("US_STATE", "ORIG_DTE"), UNEMP := UNEMP] 
  Combined_Data <- Combined_Data[, lapply(.SD, function(x) as.character(format(x, scientific = FALSE)))]
  # write to csv
  file_name <- paste0("/Users/Max/Desktop/FannieMae 1/csv/fnmae_", i, ".csv")
  write.csv(Combined_Data, file = file_name, row.names = FALSE)
  rm(Combined_Data)
  gc()
}


## FILTER LGD DATA ------------------------------------------------------------------


files_clean <- list.files("/Users/Max/Desktop/FannieMae 1/csv/train",full.names = T)


data_lgd <- foreach(i = 1:length(files_clean), .combine = rbind, .packages = c("data.table", "zoo")) %dopar% {
  # Load Data
  data <- fread(files_clean[[i]])
  data <- data[DEF == 1]
  gc()
  data
}



data_lgd %>% nrow()
# remove loan id
data_lgd[, LOAN_ID:=NULL]
# SJtrip white space
data_lgd[ , (names(data_lgd)) := lapply(.SD, str_trim), .SDcols = names(data_lgd)]
# check dtypes
lgd_readr <- readr::read_csv(files_clean[[30]])
data_types <- sapply(lgd_readr, class)
# save resutly to disk
write.csv(data_types, "/Users/max/Desktop/paraloq/sagemaker/data_vae/fnmae_dtypes.csv", row.names = FALSE)
fwrite(data_lgd, "/Users/max/Desktop/paraloq/sagemaker/data_vae/fnmae_defaults.csv", row.names = FALSE)

# some plotting
counts <- data_lgd[,.N, by = .(F180_DTE)][, F180_DTE:= as.Date(F180_DTE)][order(F180_DTE)] %>% as_tibble()
lgd_mean <- data_lgd[,mean(as.numeric(LGD)), by = .(F180_DTE)][, F180_DTE:= as.Date(F180_DTE)][order(F180_DTE)] %>% as_tibble()
toplot <- left_join(counts, lgd_mean) %>% pivot_longer(-F180_DTE)
counts %>% ggplot(aes(x=F180_DTE, y = N)) + geom_line(color = "red") + geom_line(data = lgd_mean, aes(y = V1*max(counts$N)), color = "blue")






