library(dplyr)
library(stringr)

format_phone_number <- function(phone_number) {
  formatted_number <- str_replace(phone_number, 
                                  "^(7)(\\d{3})(\\d{3})(\\d{2})(\\d{2})$", 
                                  "+\\1 \\2 \\3 \\4 \\5")
  return(formatted_number)
}

df <- 
  data.table::fread("phones.csv")%>%
    filter(creationDate >= as.Date("2025-04-12"))%>%
    select(property_id, url, parsed_address, roomsCount, priceTotal, phone)%>%
    mutate(parsed_address = stringr::str_remove(parsed_address, "Россия, Москва, "),
           phone =phone%>%as.character()%>%format_phone_number()      
    )%>%
    inner_join(data.table::fread("search_clean_with_id.csv")%>%
                 select(property_id, last_seen_dttm)%>%
                 group_by(property_id)%>%
                 summarise(last_seen_dt = last_seen_dttm%>%as.Date()%>%max()),
               join_by(property_id)
    )%>%
    select(-property_id)%>%
    filter(last_seen_dt <= as.Date("2025-09-09") & last_seen_dt >= as.Date("2025-09-07"))

df%>%
  writexl::write_xlsx("phones_sample.xlsx")


t <- readxl::read_excel("~/Downloads/consolidated.xlsx")
tt <- data.table::fread("offers_raw.csv")


t%>%
  inner_join(tt%>%select(url, creationDate)%>%distinct(), join_by(url))%>%
  mutate(days = as.Date(last_seen_dt) - as.Date(creationDate))%>%
  writexl::write_xlsx("rented_days.xlsx")

data.table::fread("cleaned_data.csv")%>%
  filter(url == "https://www.cian.ru/rent/flat/318518826/")%>%
  pull(description)%>%
  print()


"36fb3d4a1e8105126e063d048c8700578da3d757d03f7441ba2a27e925e3a213"

"https://www.cian.ru/rent/flat/317065806/"

data.table::fread("search_clean_with_id.csv")%>%
     filter(url == "https://www.cian.ru/rent/flat/318964729/")%>%
     select(lat, lng, floorNumber, roomsCount, ad_deal_type, totalArea)


data.table::fread("cleaned_data.csv")%>%
  filter(url == "https://www.cian.ru/rent/flat/317065806/")%>%
  select(lat, lng, floorNumber, roomsCount, ad_deal_type, totalArea)
