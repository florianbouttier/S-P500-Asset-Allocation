rm(list =ls())
library(tidyverse)
library(data.table)
library(quantmod)
library(TTR)
library(yfR)
Repo = "C:/Users/flori/OneDrive/Bureau/Finance/SP500/Models"
Modelisation = "Median"

#ParamLvl1
#params_temp = 12*(1:5)
#params_alpha = c(1,1.5,2)
params_temp = 12*c(2,4,6,8,10,12)
params_alpha = c(1)

Temp_Lvl2 = 8*12
Alpha_Lvl2 = 1
#--------------------------------------Function------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
ema_historical_return <- function(returns, span = 252) {
  # Convertir les rendements en pourcentages
  alpha = 2 / (1 + span)
  returns_loop <- (returns-1) * 100
  
  # Calculer les rendements exponentiels pondérés
  ema_returns <- TTR::EMA(returns_loop, n = span, wilder = TRUE)
  
  # Supprimer les valeurs manquantes
  ema_returns <- ema_returns[!is.na(ema_returns)]
  
  plot(ema_returns)
  # Calculer la moyenne des rendements exponentiels pondérés
  ema_return <- mean(ema_returns)
  
  return(ema_return)
}
Rendement_Annuel <- function(l){
  N = length(l)
  Nombre_Jour_Par_Annee = 252
  Nombre_Annee = N/Nombre_Jour_Par_Annee
  l = replace(l,is.na(l),1)
  Rendement = prod(l)**(1/Nombre_Annee)-1
  return(Rendement)
  
}
max_drawdown <- function(prices) {
  n <- length(prices)
  drawdown <- numeric(n)
  
  for (i in 2:n) {
    drawdown[i] <- (prices[i] - max(prices[1:i])) / max(prices[1:i])
  }
  
  max_drawdown_value <- min(drawdown)
  return(max_drawdown_value)
}
Vol_Annuel <- function(l){
  Nombre_Jour_Par_Annee = 252
  l = replace(l,is.na(l),1)
  Volatilite = sd(l-1)*sqrt(Nombre_Jour_Par_Annee)
}  
Score <- function(x,alpha){
  x0 = x-1
  sco = log(1+alpha*x0) 
  sco = replace(sco,is.nan(sco),-Inf)
  return(sco)
} 
Decreasing_Sum <- function(liste,half_period){
  as = log(2)/half_period
  Weight = exp(-as*(0:(length(liste)-1)))
  Weight = Weight/sum(Weight)
  KPI = sum(liste * Weight)
  return(KPI)
}
Determination_Futur_Parameters_1d_DT <- function(Data_,date_end_training,alpha,alpha_temp){
  
  p <- setDT(Data_)[as.Date(End_Test) < as.Date(date_end_training) & !is.nan(Rendement_VS_SP500)][order(-End_Test)][
    , .(Rendement_VS_SP500 = Decreasing_Sum(Rendement_VS_SP500, half_period =  alpha_temp)),
    by = .(idi, NBVol)]
  p_ <- p[, .SD[Rendement_VS_SP500 == max(Rendement_VS_SP500)]]
  p_ <- p_[, .SD[idi == max(idi)]]
  p_$alpha = alpha
  p_$alpha_temp = alpha_temp
  p_$temporality = as.character(date_end_training)
  return(p_)
}
Final_Param <- function(DB = Full_Backtest,a,a_t){
  All_Return_By_Temporality_Loop <- DB %>%
    mutate(Surperf = Score(Rendement_VS_SP500,a)) %>% 
    filter(!is.na(End_Test))
  
  Final_Parameters = map_df(All_Return_By_Temporality_Loop %>% 
                              filter(End_Test != min(End_Test)) %>% 
                              pull(End_Test) %>% 
                              unique(),
                            Determination_Futur_Parameters_1d_DT,
                            Data_ = All_Return_By_Temporality_Loop,
                            alpha = a,
                            alpha_temp = a_t)
  
  #print(paste(alpha_positive,alpha,sep = "-")
  return(Final_Parameters)
}

Best_Model <- function(Model = Data,param_temp,param_alpha,Modelisation){
  Model <- Model %>% 
    mutate(Score_Model = Score(Return_VS_SP500,alpha= param_alpha))
  Choice_Best_Model_Date_DecreasingSum <- function(date){
    BM_Loop <- Model %>% 
      filter(Month < date) %>%
      group_by(Model,id) %>% 
      arrange(desc(Month)) %>% 
      summarise(Score_Model = Decreasing_Sum(Score_Model,half_period = param_temp)) %>% 
      group_by(Model) %>% 
      filter(Score_Model == max(Score_Model)) %>%
      dplyr::slice(1) %>%  
      mutate(Month = date,
             param_alpha = param_alpha,
             param_temp = param_temp)
    return(BM_Loop)
  }
  Choice_Best_Model_Date_Median <- function(date){
    BM_Loop <- Model %>%
      filter(Month < date,
             Month >= date %m-% months(param_temp)) %>%
      group_by(Model,id) %>%
      summarise(Score_Model = median(Score_Model)) %>%
      group_by(Model) %>%
      filter(Score_Model == max(Score_Model)) %>%
      dplyr::slice(1) %>% 
      mutate(Month = date,
             param_alpha = param_alpha,
             param_temp = param_temp)
    return(BM_Loop)
  }
  if(Modelisation == "Median"){
    All_BM <- map_df(Model %>% pull(Month) %>% unique(),Choice_Best_Model_Date_Median)
  }
  if(Modelisation != "Median"){
    All_BM <- map_df(Model %>% pull(Month) %>% unique(),Choice_Best_Model_Date_DecreasingSum)
  }
  return(All_BM)
}
#--------------------------------------Aggregation And preparation Data------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Data = arrow::read_parquet(paste0(Repo,"/Python Model/Output_Python.parquet")) %>% 
  dplyr::bind_rows(arrow::read_parquet(paste0(Repo,"/R Model/Output_R.parquet"))) %>% 
  dplyr::bind_rows(arrow::read_parquet(paste0(Repo,"/Xavier Delmas/Output_XavierDelmas.parquet"))) %>% 
  drop_na(Month)


Model_Python = fread(paste0(Repo,"/Python Model/Output_ModelPython.csv")) %>% mutate(Model = "Python") 
Model_R =   data.table::fread(paste0(Repo,"/R Model/Output_List_Model.csv"))  
Model_XavierDelmas = data.table::fread(paste0(Repo,"/Xavier Delmas/Output_ListModel_XavierDelmas.csv"))

SP500 <- yfR::yf_get("^GSPC",first_date = "1990-01-01",last_date = as.Date(now())) %>% 
  mutate(Month = floor_date(ref_date,unit = "month")) %>% 
  drop_na(ret_adjusted_prices) %>% 
  group_by(Month) %>% summarise(DR_SP500 = prod(1+ret_adjusted_prices)) 

Data <- Data %>% 
  left_join(SP500,by = "Month") %>% 
  mutate(Return_VS_SP500 = DR/DR_SP500)
#--------------------------------------DataQuality------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Data %>%  
  group_by(Month,id,Model) %>% count() %>% 
  summarise(Maxi = max(n)) %>% pull(Maxi)

Data %>% 
  group_by(Model,Month) %>% count() %>% 
  ggplot(aes(x = Month,y = n,color = Model))+geom_line()

#--------------------------------------BacktestLvl1------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
All_Best_Model_Lvl1 <- map_df(params_alpha,
                         function(alpha){
                           return(
                             map_df(params_temp,
                                    Best_Model,
                                    param_alpha = alpha,
                                    Model = Data,
                                    Modelisation = Modelisation
                                    )
                           )})

All_Return_Lvl1 <- All_Best_Model_Lvl1 %>%
  inner_join(Data,by = c("Month","id","Model"))

#--------------------------------------Backtest Lvl2------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
All_Best_Model_Lvl2 <- Best_Model(Model = All_Return_Lvl1 %>% 
                                         mutate(id = paste(param_alpha,param_temp,sep = "-")) %>% 
                                         dplyr::select(Month,Model,id,Return_VS_SP500),
                                       param_temp = Temp_Lvl2,
                                       param_alpha = Alpha_Lvl2,
                                  Modelisation = "Median")

All_Return_Lvl2 <- All_Best_Model_Lvl2 %>%
  inner_join(All_Return_Lvl1 %>% 
               mutate(id = paste(param_alpha,param_temp,sep = "-"))%>% 
               dplyr::select(Month,Model,id,DR),
             by = c("Month","id","Model"))
#----------------------------------------Analysing the Result---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------v
Date_Debut = "2005-01-01"
Date_Fin = "2024-01-01"

All_All_Return <- All_Return_Lvl2 %>% dplyr::select(Month,Model,DR) %>% 
  dplyr::bind_rows(SP500 %>% 
                     mutate(DR = DR_SP500,Model = "SP500") %>% 
                     dplyr::select(Month,Model,DR)) %>% 
  dplyr::bind_rows(All_Return_Lvl2 %>%
                     filter(Model != "R") %>% 
                     dplyr::select(Month,Model,DR) %>% 
                     group_by(Month) %>% 
                     summarise(DR = mean(DR))%>% 
                     ungroup() %>% 
                     mutate(Model = "Aggregated"))  %>% 
  filter(year(Month) < 2024)


All_All_Return %>% 
  filter(Month >= as.Date(Date_Debut),
         Month <= as.Date(Date_Fin)) %>% 
  group_by(Model) %>% 
  arrange(Month) %>% 
  mutate(Price = cumprod(DR)) %>% 
  ggplot(aes(x = Month,y = Price,color = Model))+geom_line()+geom_point()


All_Return_Since_x <- map_df(2005:2023,
       function(annee){
         All_All_Return %>% 
           filter(year(Month)>= annee) %>% 
           group_by(Model) %>% 
           arrange(Month) %>% 
           summarise(Return = prod(DR),
                     N = n(),
                     Maxi_DD = max_drawdown(cumprod(DR))) %>% ungroup() %>% 
           mutate(Annualised_Return = Return^(12/N)-1,
                  Year = annee)
       }
       )

All_Return_Since_x %>%
  mutate(Label = scales::percent(round(Annualised_Return,2))) %>% 
  ggplot(aes(x = Year,y = Model,fill = Annualised_Return,label = Label))+
  geom_tile()+geom_text()+scale_fill_viridis_c()


All_All_Return %>% 
  filter(Month >= as.Date(Date_Debut),
         Month <= as.Date(Date_Fin)) %>% 
  group_by(Model) %>% 
  arrange(Month) %>% 
  mutate(Price = cumprod(DR)) %>% 
  summarise(MaxDD = max_drawdown(Price))

All_All_Return %>% 
  mutate(Year = year(Month)) %>% 
  group_by(Year,Model) %>% 
  summarise(Return = prod(DR)) %>% ungroup() %>% 
  pivot_wider(names_from = "Model",values_from = "Return") %>% 
  mutate(Python_VS_SP500 = as.numeric(Python > SP500),
         XavierDelmas_VS_SP500 = as.numeric(XavierDelmas > SP500),
         Aggregated_VS_SP500 = as.numeric(Aggregated > SP500)) %>% 
  drop_na() %>% 
  pivot_longer(contains("_VS_SP500"),names_to = "Beating",values_to = "Beat") %>%
  group_by(Beating) %>% 
  summarise(Beat = mean(Beat))
  

All_All_Return %>% 
  mutate(Year = year(Month)) %>% 
  group_by(Year,Model) %>% 
  summarise(Return = prod(DR)) %>% ungroup() %>% 
  mutate(Label = paste0(100*round(Return-1,2),"%")) %>% 
  ggplot(aes(x = Year,y = Model,fill = Return,label = Label))+geom_tile()+geom_text()+
  scale_fill_viridis_c()

#----------------------------------------Historical Model---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Historical_Model <- All_Best_Model_Lvl2 %>% 
  dplyr::select(Month,Model,param_alpha,param_temp) %>% 
  inner_join(All_Return_Lvl1 %>% 
               dplyr::select(Month,Model,param_alpha,param_temp,id),
             by = c("Month","Model","param_alpha","param_temp")) 


Historical_ModelR <- Historical_Model %>% 
  filter(Model == "XavierDelmas") %>% 
  left_join(Model_XavierDelmas,by = "id")

Historical_ModelPython <- Historical_Model %>% 
  filter(Model == "Python") %>% 
  left_join(Model_Python,by = "id")



