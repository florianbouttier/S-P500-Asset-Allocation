rm(list = ls())
library(crypto2)
library(quantmod)
library(tidyverse)
library(TTR)
library(lubridate)
library(data.table)
library(readxl)
library(yfR)
Repo_Output = "C:/Users/flori/OneDrive/Bureau/Finance/SP500/Models/Xavier Delmas"
Repo_Input = "C:/Users/flori/OneDrive/Bureau/Finance/SP500"
#--------------------------------------Data-------------------------------------------------------------------------------------
Historical_Company <- fread(paste0(Repo_Input,"/Data/Historical_Company_SP500.csv")) 
Data = fread(paste0(Repo_Input,"/Data/SP500_PriceVolume_Last.csv")) %>% 
  group_by(ticker) %>% 
  arrange(Date) %>% 
  mutate(Adj_Close = cumprod(Daily_Return)) %>% ungroup() %>% 
  dplyr::select(Date,ticker,Adj_Close,Volume)

max(Data$Date)
SP500 <- yfR::yf_get("^GSPC",
                first_date = "1960-01-01",
                last_date = Sys.Date(),
                thresh_bad_data = 0) %>% 
  rename("Date" = "ref_date",
         "Open" = "price_open",
         "High" = "price_high",
         "Low" = "price_low",
         "Close" = "price_close",
         "Adj_Close" = "price_adjusted",
         "Volume" = "volume") %>% 
  dplyr::select(colnames(Data)) %>% 
  group_by(Date) %>% 
  dplyr::slice(1) %>% 
  ungroup() %>% 
  group_by(ticker) %>% 
  arrange(Date) %>%  
  mutate(DR_SP500 = Adj_Close/lag(Adj_Close)) %>% ungroup()

Data_VS_SP500 = Data %>% 
  mutate(Date = as.Date(Date)) %>% 
  left_join(SP500 %>% 
              dplyr::select(Date,Adj_Close) %>% 
              rename("Close_SP500" = "Adj_Close"),by = "Date") %>% 
  mutate(Close_VS_SP500 = Adj_Close/Close_SP500) %>% 
  group_by(ticker) %>% 
  arrange(Date) %>% 
  mutate(DR = Adj_Close/lag(Adj_Close),
         DR_VS_SP500 = Close_VS_SP500/lag(Close_VS_SP500),
         Number = n(),
         Close = Adj_Close) %>%
  ungroup() %>% 
  drop_na(DR) %>% filter(Number > 300) %>% 
  dplyr::select(Date,ticker,Close_VS_SP500,Close,DR_VS_SP500,DR) %>% 
  drop_na() 

Historical_Company <- Historical_Company %>% 
  mutate(Date = lubridate::floor_date(Date,"1 month")) %>% 
  distinct()  %>% 
  group_by(Date,Ticker) %>% dplyr::slice(1)

#--------------------------------------LearningPerMonth V2------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Learning_Monthly <- function(N_Long,N_Short,func_MovingAverage,temporality,N_Asset){
  df = Data_VS_SP500 %>% 
    drop_na() %>% 
    mutate(temporality = floor_date(Date,unit = temporality)) %>% 
    mutate(M = floor_date(Date,unit = "1 month")) %>% 
    group_by(ticker) %>%
    arrange(Date) %>%
    mutate(Short = func_MovingAverage(Close,n = N_Short),
           Long = func_MovingAverage(Close,n = N_Long),
           Short_VS_SP500 = func_MovingAverage(Close_VS_SP500,n = N_Short),
           Long_VS_SP500 = func_MovingAverage(Close_VS_SP500,n = N_Long)) %>% ungroup() %>% 
    drop_na() %>% 
    mutate(MTR_Close = Short/Long,
           MTR_Close_VSSP500 = Short_VS_SP500/Long_VS_SP500) %>% 
    pivot_longer(c(MTR_Close,MTR_Close_VSSP500),names_to = "KPI",values_to = "MTR") %>% 
    inner_join(Historical_Company,
               by =c("M" = "Date","ticker" = "Ticker")) %>% 
    group_by(temporality,KPI) %>% 
    filter(Date == max(Date)) %>% 
    slice_max(MTR,n = N_Asset) %>% 
    mutate(N_Long = N_Long,
           N_Short = N_Short,
           N_Asset = N_Asset) %>%
    dplyr::select(temporality,ticker,KPI,N_Long,N_Short,N_Asset,MTR)
  
  Final_Return <- Data_VS_SP500 %>% 
    mutate(month = lubridate::floor_date(Date,temporality)) %>% 
    group_by(month,ticker) %>% 
    summarise(DR = prod(DR)) %>% ungroup() %>% 
    group_by(ticker) %>% 
    arrange(month) %>% 
    mutate(application_month = lag(month)) %>% ungroup() %>% 
    inner_join(df,
               by = c("application_month" = "temporality","ticker"))
  
  return(Final_Return)
}
Determination_Position <- function(N_Long,N_Short,func_MovingAverage,temporality,N_Asset){
  df = Data_VS_SP500 %>% 
    drop_na() %>% 
    mutate(M = floor_date(Date,unit = "1 month")) %>% 
    group_by(ticker) %>%
    arrange(Date) %>%
    mutate(Short = func_MovingAverage(Close,n = N_Short),
           Long = func_MovingAverage(Close,n = N_Long),
           Short_VS_SP500 = func_MovingAverage(Close_VS_SP500,n = N_Short),
           Long_VS_SP500 = func_MovingAverage(Close_VS_SP500,n = N_Long)) %>% ungroup() %>% 
    drop_na() %>% 
    mutate(MTR_Close = Short/Long,
           MTR_Close_VSSP500 = Short_VS_SP500/Long_VS_SP500) %>% 
    pivot_longer(c(MTR_Close,MTR_Close_VSSP500),names_to = "KPI",values_to = "MTR") %>% 
    inner_join(Historical_Company,
               by =c("M" = "Date","ticker" = "Ticker")) %>% 
    group_by(KPI) %>% 
    filter(Date == max(Date)) %>% 
    slice_max(MTR,n = N_Asset) %>%
    mutate(MTR = MTR-1,
           MTR = if_else(MTR <= 0,0,MTR),
           MTR = MTR/sum(MTR)) %>% ungroup() %>%  
    mutate(N_Long = N_Long,
           N_Short = N_Short,
           N_Asset = N_Asset) %>%
    dplyr::select(Date,ticker,KPI,N_Long,N_Short,N_Asset,MTR)
  return(df)
}

A <- Determination_Position(N_Long = 115,
                       N_Short = 10,
                       func_MovingAverage = TTR::EMA,
                       temporality = "1 month",
                       N_Asset = 10)

Parameters = expand.grid(N_Long = c(100,125,150,175,200,250,300),N_Short = c(10,15,20,25,30,40,50),N_Asset = c(10)) %>% 
  filter(N_Long > N_Short)

All_Return_Monthly <- map_df(1:nrow(Parameters),
                    function(i){
                      return(Learning_Monthly(N_Long = Parameters$N_Long[i],
                                              N_Short = Parameters$N_Short[i],
                                              func_MovingAverage = TTR::EMA,
                                              N_Asset =  Parameters$N_Asset[i],
                                              temporality = "1 month"))
                    })

All_Return_Monthly_Total <- All_Return_Monthly %>%
  mutate(MTR = MTR-1,
         MTR = if_else(MTR <= 0,0,MTR)) %>% 
  group_by(month,KPI,N_Long,N_Short,N_Asset) %>% 
  mutate(MTR = MTR/sum(MTR),
         DR = DR*MTR) %>% 
  summarise(DR = sum(DR),
            N = n(),
            Min_MTR = min(MTR)) %>% 
  ungroup()

Model <- All_Return_Monthly_Total %>% 
  dplyr::select(KPI,N_Long,N_Short,N_Asset) %>% 
  distinct() %>% 
  mutate(id = 1,id = cumsum(id))

All_Return_Monthly_Total %>% 
  inner_join(Model) %>% 
  mutate(Month = month) %>% 
  dplyr::select(Month,id,DR) %>% 
  mutate(Model = "XavierDelmas") %>% 
  arrow::write_parquet(paste(Repo_Output,"Output_XavierDelmas.parquet",sep = "/"))

Model %>% fwrite(paste(Repo_Output,"Output_ListModel_XavierDelmas.csv",sep = "/"))

Debut = as.Date("2005-01-01")
Fin = as.Date("2023-11-01")
All_Return_Monthly_Total %>%
  filter(month >= Debut,
         month <= Fin) %>% 
  group_by(KPI,N_Long,N_Short,N_Asset) %>% 
  summarise(Return = prod(DR),
            Number = n()) %>% ungroup() %>% 
  mutate(Annualised_Return = Return^(12/Number)-1) %>% 
  mutate(Label = scales::percent(Annualised_Return,2)) %>% 
  ggplot(aes(x = as.character(N_Short),y = N_Long,label = Label,fill = Annualised_Return))+
  geom_tile()+scale_fill_viridis_c()+geom_text()+
  facet_wrap(KPI~N_Asset)

All_Return_Monthly %>% 
  filter(N_Short == 20,N_Long == 200,N_Asset %in% c(10),KPI == "MTR_Close_VSSP500",month == "2022-06-12")%>%
  filter(month >= Debut,
         month <= Fin) %>% View()

All_Return_Monthly_Total %>% 
  filter(N_Short == 20,N_Long == 200,N_Asset %in% c(10),KPI == "MTR_Close_VSSP500")%>%
  filter(month >= Debut,
         month <= Fin) %>% 
  group_by(KPI,N_Long,N_Short,N_Asset) %>% 
  arrange(month) %>% 
  mutate(Return = cumprod(DR)) %>% ungroup() %>% 
  ggplot(aes(x = month,y = Return,color = as.character(N_Asset)))+geom_point()+geom_line()



All_Return_Monthly_Total <- All_Return_Monthly %>%
  mutate(MTR = MTR-1,
         MTR = if_else(MTR <= 0,0,MTR)) %>% 
  group_by(month,KPI,N_Long,N_Short,N_Asset) %>% 
  mutate(MTR = MTR/sum(MTR),
         DR_Special = DR*MTR) %>% 
  summarise(DR = sum(DR_Special),
            N = n(),
            Min_MTR = min(MTR)) %>% 
  ungroup()
