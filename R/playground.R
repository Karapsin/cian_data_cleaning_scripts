library(dplyr)

df <- 
  data.table::fread("daily_prices.csv")%>%
    inner_join(data.table::fread("cleaned_data.csv")%>%
                  select(property_id, ad_deal_type),
               join_by(property_id)
    )


df%>%
  filter(ad_deal_type == "short_rent")%>%
  filter(property_id == "b09993d14dd71e8017f177027d3829f678ba44301fdcc53cb6d2b1282a9bdb47")

library(ggplot2)
options(scipen = 9999)
df%>%
  filter(as.Date(date) >= as.Date("2025-04-20") & as.Date(date) <= as.Date("2025-05-31"))%>%
  group_by(ad_deal_type, date)%>%
  summarise(median_price = median(price))%>%
    ggplot(aes(x = date, y = median_price))+
      facet_wrap(~ad_deal_type, scales = "free")+
      geom_line()
  


df <- data.table::fread("cleaned_data.csv")

df%>%
  group_by(ad_deal_type)%>%
  summarise(total = length(url),
            closed = sum(ifelse(ad_is_closed, 1, 0))          
  )%>%
  mutate(close_share = closed/total)

df$ad_is_closed%>%sum()
