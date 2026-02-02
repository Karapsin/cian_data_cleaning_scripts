library(dplyr)

df <- data.table::fread("offers_with_key.csv")%>%
        mutate(dt = as.Date(creationDate))%>%
        group_by(property_id)%>%
        mutate(dt = min(dt))%>%
        ungroup()



df%>%
  filter(!ad_is_closed)%>%
  select(ad_deal_type, dt, property_id)%>%
  distinct()%>%
  filter(ad_deal_type %in% c("sale_secondary", "long_rent", "sale_primary"))%>%
  mutate(is_old_ad = ifelse(dt < as.Date("2025-04-01"), 1, 0))%>%
  group_by(ad_deal_type, property_id)%>%
  summarise(is_old_ad = max(is_old_ad))%>%
  group_by(ad_deal_type)%>%
  summarise(old_ads = sum(is_old_ad),
            total_ads = length(property_id)
  )%>%
  ungroup()%>%
  mutate(old_share = old_ads/total_ads)


df%>%
  #filter(!ad_is_closed)%>%
  select(ad_deal_type, dt, property_id)%>%
  distinct()%>%
  filter(ad_deal_type %in% c("sale_secondary", "long_rent"))%>%
  mutate(is_old_ad = ifelse(dt < as.Date("2025-04-12"), 1, 0))%>%
  group_by(ad_deal_type, property_id)%>%
  summarise(is_old_ad = max(is_old_ad))%>%
  group_by(ad_deal_type)%>%
  summarise(old_ads = sum(is_old_ad),
            total_ads = length(property_id)
  )%>%
  ungroup()%>%
  mutate(old_share = old_ads/total_ads)


old_ids <- 
df%>%
  #filter(!ad_is_closed)%>%
  select(ad_deal_type, dt, property_id)%>%
  distinct()%>%
  filter(ad_deal_type %in% c("sale_secondary", "long_rent"))%>%
  mutate(is_old_ad = ifelse(dt < as.Date("2025-04-12"), 1, 0))%>%
  group_by(ad_deal_type, property_id)%>%
  summarise(is_old_ad = max(is_old_ad))%>%
  filter(is_old_ad == 1)%>%
  pull(property_id)%>%
  unique()

url_to_exclude <- 
df%>%
  filter(property_id %in% old_ids)%>%
  pull(url)%>%
  unique()

clean_df <- 
df%>%
  filter(!(url %in% url_to_exclude))

url_to_exclude

clean_df$url%>%unique()%>%
  unique()

df$cian_price_range%>%
  unique()

t <- 
  df%>%
  ungroup()%>%
  filter(as.Date(creationDate) == last_dt & ad_is_closed & as.Date(creationDate) <= as.Date("2025-05-25"))%>%
  filter(ad_deal_type %in% c("sale_secondary", "long_rent"))%>%
  mutate(last_dt = as.Date(offer_page_load_dttm))%>%
  mutate(days = as.numeric(last_dt) - as.numeric(dt))%>%
  select(days, ad_deal_type, property_id)%>%
  distinct()%>%
  select(ad_deal_type, days)

t%>%
  filter(ad_deal_type != "long_rent")%>%
  pull(days)%>%
  quantile(0.7)

df%>%
  ungroup()%>%
  filter(as.Date(creationDate) == last_dt & ad_is_closed & as.Date(creationDate) <= as.Date("2025-05-25"))%>%
  filter(ad_deal_type %in% c("sale_secondary", "long_rent"))%>%
  mutate(last_dt = as.Date(offer_page_load_dttm))%>%
  mutate(days = as.numeric(last_dt) - as.numeric(dt))%>%
  select(ad_deal_type, days)%>%
  ungroup()%>%
  ggplot(aes(x=days, color = ad_deal_type))+
    geom_density()

library(ggplot2)
old_plot <- 
df%>%
  filter(dt >= as.Date("2025-04-12"))%>%
  filter(!ad_is_closed)%>%
  select(ad_deal_type, property_id, dt)%>%
  distinct()%>%
  #mutate(dt = lubridate::floor_date(dt, unit = "day", week_start = 1))%>%
  group_by(ad_deal_type, dt)%>%
  count()
  

clean_df%>%
  filter(between(dt, as.Date("2025-04-12"), as.Date("2025-04-19")))%>%
  group_by(property_id)%>%
  count()%>%
  arrange(desc(n))

clean_df%>%
  filter(between(dt, as.Date("2025-04-12"), as.Date("2025-04-19")) & ad_is_closed)
  

clean_df
  


old_plot%>%
#  filter(dt <= as.Date("2025-05-20"))%>%
  ggplot(aes(x=dt, y = (n)))+
    facet_wrap(~ad_deal_type, scales = "free")+
    geom_line()
key_df <- 
df%>%
  select(ad_deal_type, url, property_id)%>%
  group_by(property_id)%>%
  mutate(url_count = length(unique(url)))
  
key_df%>%
  inner_join(df%>%
              select(url, priceTotal)%>%
              group_by(url)%>%
              summarise(price = mean(priceTotal)), 
             join_by(url)
  )%>%
  group_by(ad_deal_type, url_count)%>%
  summarise(n = length(url),
            avg_price = mean(price)
  )%>%
  group_by(ad_deal_type)%>%
  mutate(share = round((n/sum(n))*100, 2))%>%
  print(n = 100)


df$creationDate%>%as.Date()

key_df%>%
  filter(url_count == 2 & ad_deal_type == "sale_primary")%>%
  inner_join(df%>%
               select(property_id, url, creationDate)%>%
               mutate(creationDate = as.Date(creationDate))%>%
               group_by(property_id)%>%
               mutate(dt = min(creationDate))%>%
               ungroup()%>%
               select(url, dt)%>%
               distinct(),
             join_by(url)
  )%>%
  arrange(desc(dt))%>%
  filter(property_id == "8c6854e4c725367782895f363d583f400c5ca28930c7809a6d48ae598d7efec1")%>%
  pull(url)%>%
  unique()

df$priceTotal



data.table::fread("~/Downloads/proxys_1100733.txt", header = FALSE)%>%
  pull(V1)%>%
  paste(collapse=",")
