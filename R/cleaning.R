library(dplyr)

df <- 
  data.table::fread("daily_prices.csv")%>%
    inner_join(data.table::fread("cleaned_data.csv")%>%
                  select(property_id, ad_deal_type),
               join_by(property_id)
    )


library(ggplot2)
options(scipen = 9999)
df%>%
  group_by(ad_deal_type, date)%>%
  summarise(median_price = median(price))%>%
    ggplot(aes(x = date, y = median_price))+
      facet_wrap(~ad_deal_type, scales = "free")+
      geom_line()
  


