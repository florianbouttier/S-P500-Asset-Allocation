rm(list = ls())
library(lubridate)
library(data.table)
library(pacman)
library(tidyverse)
library(rvest)
library(yfR)

repo = "C:/Users/flori/OneDrive/Bureau/Finance/SP500/Data"
setwd(repo)
#--------------------------------------Functions----------------------------------------
Dowload_Historical_SP500 <- function(){
  Sys.setlocale("LC_TIME", "English")
  wikispx <- read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
  
  
  
  currentconstituents <- wikispx %>%
    html_node('#constituents') %>%
    html_table(header = TRUE)
  
  
  spxchanges <- wikispx %>%
    html_node('#changes') %>%
    html_table(header = FALSE, fill = TRUE) %>%
    filter(row_number() > 2) %>% # First two rows are headers
    `colnames<-`(c('Date','AddTicker','AddName','RemovedTicker','RemovedName','Reason')) %>%
    mutate(Date = as.Date(Date, format = "%B %d, %Y"),
           year = year(Date),
           month = month(Date))
  spxchanges$Date
  
  
  # Start at the current constituents...
  currentmonth <- as.Date(format(Sys.Date(), '%Y-%m-01'))
  monthseq <- seq.Date(as.Date('1990-01-01'), currentmonth, by = 'month') %>% rev()
  spxstocks <- currentconstituents %>% mutate(Date = currentmonth) %>% select(Date, Ticker = Symbol, Name = Security)
  lastrunstocks <- spxstocks
  # Iterate through months, working backwards
  for (i in 2:length(monthseq)) {
    d <- monthseq[i]
    y <- year(d)
    m <- month(d)
    changes <- spxchanges %>%
      filter(year == year(d), month == month(d))
    # Remove added tickers (we're working backwards in time, remember)
    tickerstokeep <- lastrunstocks %>%
      anti_join(changes, by = c('Ticker' = 'AddTicker')) %>%
      mutate(Date = d)
    
    # Add back the removed tickers...
    tickerstoadd <- changes %>%
      filter(!RemovedTicker == '') %>%
      transmute(Date = d,
                Ticker = RemovedTicker,
                Name = RemovedName)
    
    thismonth <- tickerstokeep %>% bind_rows(tickerstoadd)
    spxstocks <- spxstocks %>% bind_rows(thismonth)  
    
    lastrunstocks <- thismonth
  }
  
  spxstocks = spxstocks %>% 
    filter(Date == max(Date)) %>% 
    mutate(Date = as.Date(now())) %>%
    rbind(spxstocks)
  
  return(spxstocks)
  
  
}
Daily_return <- function(x){
  retour = x/lag(x)
  retour = replace(retour,is.na(retour),1)
  return(retour)
}
Cumprod_with_NA <- function(liste){
  NAN = TRUE
  i = 0
  while(NAN){
    i = i+1
    NAN = is.na(liste[i])
  }
  liste_cum = liste[1:i]
  cum = liste[i]
  for(e in (i+1):length(liste)){
    cum = cum*replace(liste[e],is.na(liste[e]),1)
    liste_cum = c(liste_cum,cum)
  }
  return(liste_cum)
}
#--------------------------------------Historical_Company_SP500-----------------------------------
Historical_Company_SP500 <- Dowload_Historical_SP500()
fwrite(Historical_Company_SP500,paste0(repo,"/Historical_Company_SP500.csv"))
#--------------------------------------Historical_Company_SP500-----------------------------------
Historical_Price = fread(paste0(repo,"/SP500_PriceVolume_Last.csv"))
max_date_Historical_Price = max(Historical_Price$Date)
memory.limit(4*memory.limit())

Ticker_To_Have <- Historical_Company_SP500 %>% 
  filter(Date >= floor_date(max_date_Historical_Price,unit = "1 month")) %>% 
  pull(Ticker) %>% unique() 
Old_Tickers <- Historical_Price %>% pull(ticker) %>% unique()

Ticker_To_Update <- Old_Tickers[Old_Tickers %in% Ticker_To_Have]
New_Tickers <- Ticker_To_Have[!(Ticker_To_Have %in% Ticker_To_Update)]
New_Tickers <- gsub("\\.","_",New_Tickers)

New_Data = Ticker_To_Update %>% 
  yf_get(first_date = max_date_Historical_Price,
         last_date = Sys.Date(),
         thresh_bad_data = 0) 

Data_New_Ticker = data.frame()
Data_New_Ticker =  New_Tickers %>% 
  yf_get(first_date = "2000-01-01",
         last_date = Sys.Date(),
         thresh_bad_data = 0) 


New_Data <- New_Data %>% 
  bind_rows(Data_New_Ticker) %>% 
  mutate(volume = volume*price_close) %>% 
  dplyr::select(ref_date,ticker,price_adjusted,volume) %>% 
  rename("Date" = "ref_date",
         "Adj_Close" = "price_adjusted",
         "Volume" = "volume") %>% 
  mutate(Date = as.character(Date)) %>% 
  group_by(ticker) %>% 
  arrange(Date) %>% 
  mutate(Daily_Return = Adj_Close/lag(Adj_Close)) %>% 
  drop_na(Daily_Return) %>% ungroup()


Joined_DB = Historical_Price  %>% mutate(Date = as.character(Date)) %>%  
  dplyr::bind_rows(New_Data %>% mutate(data = paste("YF",as.character(Sys.time())),
                                       data_map = 4) %>% 
                     mutate(Date = as.character(Date))) 

Joined_DB_ = Joined_DB %>% 
  group_by(Date,ticker) %>% 
  filter(data_map == max(data_map)) %>%
  slice(1) %>% 
  ungroup()


fwrite(Joined_DB_,paste0(repo,"/SP500_PriceVolume_Last.csv"))
#----------------------------------------Output Python-----------------------
Price = Joined_DB_ %>% 
  dplyr::select(Date,ticker,Daily_Return) %>% 
  pivot_wider(names_from = ticker,values_from = Daily_Return) %>% 
  mutate_at(vars(-Date),Cumprod_with_NA)

Volume = Joined_DB_ %>% 
  dplyr::select(Date,ticker,Volume) %>% 
  pivot_wider(names_from = ticker,values_from = Volume) 

Price_AgainstSP500 = Price %>% 
  left_join("^GSPC" %>% yf_get(first_date = "1960-01-01",
                               last_date = Sys.time()) %>% 
              rename("Date" = "ref_date",
                     "SP500" = "price_adjusted") %>%
              mutate(Date = as.character(Date)) %>% 
              dplyr::select(Date,SP500)) %>% 
  dplyr::mutate_at(vars(-Date,-SP500),funs(./SP500))

fwrite(Price,paste0(repo,"/Output_Python_Price.csv"))
fwrite(Volume,paste0(repo,"/Output_Python_Volume.csv"))
fwrite(Price_AgainstSP500,paste0(repo,"/Output_Python_Price_Against_SP500.csv"))
