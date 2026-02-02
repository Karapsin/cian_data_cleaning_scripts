rm(list = ls())
gc()

library(dplyr)
library(purrr)

dir <- "~/Downloads/data_115/"

get_df <- function(file){
  print(file)
  
  
  res <- 
    file%>%
      paste0(dir, .)%>%
      data.table::fread()%>%
      filter(region == "г. Москва")%>%
      mutate(source = file)
  
  res%>%
    nrow()%>%
    paste0(., " lines")%>%
    print()
  
  res
}

df_list <- 
  list.files(dir)%>%
    .[stringr::str_detect(string=., pattern="csv")]%>%
    map(~get_df(.x))



t <- data.table::fread("~/Downloads/data_115/structure.csv")

map(df_list, ~.x%>%colnames())%>%
  reduce(intersect)


df_list[[3]]%>%
  select(region, mun_level, munr, municipality, oktmo, mun_type)%>%
  distinct()%>%
  as_tibble()%>%
  print(n = 1000)

readxl::read_excel("~/Downloads/data-62963-2025-11-30.xlsx")%>%
  select(ContractNumber)%>%
  distinct()%>%
  print(n = 1000)
