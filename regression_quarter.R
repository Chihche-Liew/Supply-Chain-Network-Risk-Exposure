require(tidyverse)
require(RPostgres)
require(data.table)
require(sqldf)
require(zoo)
require(bizdays)
require(lfe)
require(stargazer)

setwd("D:/SupplyChainRiskExposure")

n_pull <- -1

wrds_user <- "lzy2lzz"
wrds_password <- "playerpassword"

# load data
wrds <- dbConnect(Postgres(),
                  host     = "wrds-pgdata.wharton.upenn.edu",
                  port     = 9737,
                  user     = wrds_user,
                  password = wrds_password,
                  dbname   = "wrds",
                  sslmode  = "require")

env_score <- select(fread('./factset_revere_companies/Environmental_Dimension/quarter/1-s-firm.csv'), 
                    firm_gvkey, year, quarter) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/quarter/2-s-firm.csv'), 
                   firm_gvkey, year, quarter)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/quarter/3-s-firm.csv'), 
                   firm_gvkey, year, quarter)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/quarter/1-c-firm.csv'), 
                   firm_gvkey, year, quarter)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/quarter/2-c-firm.csv'), 
                   firm_gvkey, year, quarter)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/quarter/3-c-firm.csv'), 
                   firm_gvkey, year, quarter)) %>% 
  distinct()

env_score <- env_score %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/1-s-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>%
  mutate(first_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/2-s-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>% 
  mutate(second_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/3-s-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>% 
  mutate(third_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/1-c-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>% 
  mutate(first_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/2-c-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>% 
  mutate(second_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/quarter/3-c-firm.csv'),
            by = c('firm_gvkey', 'year', 'quarter')) %>% 
  mutate(third_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  mutate(across(everything(), ~replace_na(.x, 0)))

# link gvkey to permno
env_score <- env_score %>% 
  mutate(month = case_when(quarter == 1 ~ 3,
                           quarter == 2 ~ 6,
                           quarter == 3 ~ 9,
                           quarter == 4 ~ 12)) %>% 
  left_join(select(comp_fundq, gvkey, datadate, year, month),
            by = c('firm_gvkey' = 'gvkey', 'year', 'month')) %>% 
  drop_na()

env_score <- sqldf("SELECT a.*, b.*
                    FROM env_score AS a
                    LEFT JOIN ccmxpf AS b
                    ON (a.firm_gvkey = b.gvkey) 
                    AND (a.datadate >= b.linkdt OR b.linkdt IS NULL) 
                    AND (a.datadate <= b.linkenddt OR b.linkenddt IS NULL)") %>% 
  transmute(firm_gvkey = firm_gvkey,
            firm_permno = lpermno,
            year = year,
            quarter = quarter,
            datadate = datadate,
            first_degree_supplier_env_decline = first_degree_supplier_env_decline,
            second_degree_supplier_env_decline = second_degree_supplier_env_decline,
            third_degree_supplier_env_decline = third_degree_supplier_env_decline,
            first_degree_customer_env_decline = first_degree_customer_env_decline,
            second_degree_customer_env_decline = second_degree_customer_env_decline,
            third_degree_customer_env_decline = third_degree_customer_env_decline
  ) %>% 
  distinct()

## price, return, volume, shrout
res <- dbSendQuery(wrds, "select date, permno, prc, ret, vol, shrout
                          from crsp.msf 
                          where date between '01/31/2010' and '12/31/2022'")

crsp_msf <- dbFetch(res, n = n_pull); dbClearResult(res)

## book_value, capx, roa, tant
res <- dbSendQuery(wrds, "select gvkey, datadate, ceqq, capxy, saleq, atq, niq, cheq, rectq, invtq, ppentq
                          from comp.fundq
                          where datadate between '01/31/2010' and '12/31/2022'")

comp_fundq <- dbFetch(res, n = n_pull); dbClearResult(res)

comp_fundq <- comp_fundq %>% 
  mutate(gvkey = as.numeric(gvkey)) %>% 
  mutate(year = year(datadate),
         month = month(datadate))

## adv exp
res <- dbSendQuery(wrds, "select gvkey, datadate, xad
                          from comp.funda
                          where datadate between '01/31/2010' and '12/31/2022'")

comp_funda <- dbFetch(res, n = n_pull); dbClearResult(res)

## linktable
res <- dbSendQuery(wrds, "select *
                          from crsp.ccmxpf_linktable
                          where LINKTYPE in ('LU', 'LC', 'LD', 'LF', 'LN', 'LO', 'LS', 'LX')")

ccmxpf <- dbFetch(res, n = n_pull); dbClearResult(res)

ccmxpf <- ccmxpf %>% 
  mutate(gvkey = as.numeric(gvkey))

## ff_port_return
port_ret <- fread('ff_port_ret.csv')

# control vars
## size
size <- crsp_msf %>% 
  mutate(size = abs(prc)*shrout) %>% 
  select(permno, date, size) %>% 
  mutate(year = year(date),
         quarter = quarter(date)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(size = dplyr::last(size, na_rm = T)) %>% 
  drop_na()

## bm
book_value <- env_score %>% 
  select(firm_gvkey, firm_permno, datadate) %>% 
  distinct() %>% 
  left_join(select(comp_fundq, gvkey, datadate, ceqq) %>% 
              fill(ceqq, .direction = 'down'),
            by=c('firm_gvkey' = 'gvkey', 'datadate')) %>%
  mutate(last_year = year(datadate) - 1)

market_value <- size %>% 
  mutate(last_year = year - 1) %>% 
  distinct() %>% 
  group_by(permno, last_year) %>% 
  summarise(market_value = last(size))

book_to_market <- market_value %>% 
  left_join(book_value, by=c('permno' = 'firm_permno', 'last_year')) %>% 
  mutate(bm = ceqq / market_value,
         year = year(datadate)) %>% 
  drop_na() %>% 
  group_by(firm_gvkey, year) %>% 
  mutate(quarter = quarter(datadate)) %>% 
  distinct() %>% 
  select(firm_gvkey, year, quarter, bm)

## return
return <- crsp_msf %>% 
  select(permno, date, ret) %>% 
  mutate(year = year(date),
         quarter = quarter(date),
         ret = ret + 1) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret, na.rm = T)) %>% 
  ungroup() %>% 
  mutate(return = rollapply(ret, 4L, prod, na.rm = T, na.pad = T, align = 'right')) %>% 
  drop_na() %>% 
  select(permno, year, quarter, return)
  

## prc
cal <- create.calendar('cal', weekdays = c("saturday", "sunday"), financial = T)

prc <- crsp_msf %>% 
  select(permno, date) %>% 
  mutate(date_2_q_ago = date %m-% months(6),
         date_2_q_ago = ceiling_date(date_2_q_ago, unit = 'quarter') - days(1),
         date_2_q_ago = preceding(date_2_q_ago, cal)) %>% 
  left_join(crsp_msf %>% 
              select(permno, date, prc),
            by=c('permno' = 'permno', 'date_2_q_ago' = 'date')) %>% 
  mutate(year = year(date),
         quarter = quarter(date)) %>% 
  group_by(permno, year, quarter) %>% 
  mutate(prc = abs(last(prc, order_by = date))) %>% 
  select(permno, year, quarter, prc) %>% 
  distinct() %>% 
  drop_na()

## vol
vol <- crsp_msf %>% 
  select(date, permno, vol) %>% 
  mutate(quarter = quarter(date),
         year = year(date)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(vol = sum(vol, na.rm = T)) %>% 
  ungroup()

vol <- vol %>% 
  select(permno, year, quarter) %>% 
  mutate(year_2_q_ago = case_when(quarter %in% c(1, 2) ~ year - 1,
                                  quarter %in% c(3, 4) ~ year),
         quarter_2_q_ago = case_when(quarter == 1 ~ 3,
                                     quarter == 2 ~ 4,
                                     quarter %in% c(3, 4) ~ quarter - 2)) %>% 
  left_join(vol, by=c('permno', 'year_2_q_ago' = 'year', 'quarter_2_q_ago' = 'quarter')) %>% 
  select(permno, year, quarter, vol)

## capx
capx <- comp_fundq %>% 
  mutate(capx = capxy / saleq,
         year = year(datadate),
         quarter = quarter(datadate)) %>% 
  select(gvkey, year, quarter, capx)

## tant
tant <- comp_fundq %>% 
  mutate(tant = (cheq + rectq + invtq + ppentq) / atq,
         year = year(datadate),
         quarter = quarter(datadate)) %>% 
  select(gvkey, year, quarter, tant)

## roa
roa <- comp_fundq %>% 
  mutate(roa = niq / atq,
         year = year(datadate),
         quarter = quarter(datadate)) %>% 
  select(gvkey, year, quarter, roa)

## adv exp
adv_exp <- comp_fundq %>% 
  select(gvkey, datadate) %>% 
  mutate(year = year(datadate),
         quarter = quarter(datadate)) %>% 
  left_join(comp_funda %>% 
              mutate(gvkey = as.numeric(gvkey)) %>% 
              mutate(year = year(datadate),
                     quarter = quarter(datadate)) %>% 
              select(-datadate) %>% 
              drop_na(), 
            by=c('gvkey', 'year', 'quarter')) %>%
  group_by(gvkey) %>% 
  fill(xad, .direction = 'down') %>% 
  drop_na() %>% 
  transmute(gvkey = gvkey,
            year = year,
            quarter = quarter,
            adv_exp = xad)

# dep var
lag_period <- 1L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, quarter, excess_return) %>% 
  drop_na() %>% 
  distinct()

# excess_return <- port_ret %>% 
#   mutate(ret = ret + 1, 
#          port_ret = port_ret/100 + 1) %>% 
#   mutate(year = year(rdate),
#          month = month(rdate),
#          quarter = case_when(month %in% c(1, 2, 3) ~ 1,
#                              month %in% c(4, 5, 6) ~ 2,
#                              month %in% c(7, 8, 9) ~ 3,
#                              month %in% c(10, 11, 12) ~ 4)) %>% 
#   group_by(permno, year, quarter) %>% 
#   summarise(ret = prod(ret),
#             port_ret = prod(port_ret)) %>% 
#   ungroup() %>% 
#   group_by(permno) %>% 
#   mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
#          port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>% 
#   mutate(excess_return = ret_cumulative - port_ret_cumulative) %>% 
#   select(permno, year, quarter, excess_return) %>% 
#   drop_na() %>% 
#   distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression
reg <- list()

reg[[1]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[2]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))


lag_period <- 2L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, quarter, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

reg[[3]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[4]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))


lag_period <- 4L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, quarter, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

reg[[5]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[6]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))


lag_period <- 6L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, quarter, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

reg[[7]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[8]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))


lag_period <- 8L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>% 
  group_by(permno, year, quarter) %>% 
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, quarter, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

reg[[9]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[10]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

stargazer(reg, type = 'text')

save(adv_exp, book_to_market, capx, ccmxpf, comp_funda, crsp_msf, excess_return, port_ret, prc, reg, return, roa, size, tant, vol, file = 'quarter_data.RData')




# cum ret

lag_period <- 1L

excess_return <- port_ret %>%
  mutate(ret = ret + 1,
         port_ret = port_ret/100 + 1) %>%
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>%
  group_by(permno, year, quarter) %>%
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>%
  ungroup() %>%
  group_by(permno) %>%
  mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
         port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>%
  mutate(excess_return = ret_cumulative - port_ret_cumulative) %>%
  select(permno, year, quarter, excess_return) %>%
  drop_na() %>%
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression
reg <- list()

reg[[1]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[2]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))



lag_period <- 2L

excess_return <- port_ret %>%
  mutate(ret = ret + 1,
         port_ret = port_ret/100 + 1) %>%
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>%
  group_by(permno, year, quarter) %>%
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>%
  ungroup() %>%
  group_by(permno) %>%
  mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
         port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>%
  mutate(excess_return = ret_cumulative - port_ret_cumulative) %>%
  select(permno, year, quarter, excess_return) %>%
  drop_na() %>%
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression

reg[[3]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[4]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))



lag_period <- 4L

excess_return <- port_ret %>%
  mutate(ret = ret + 1,
         port_ret = port_ret/100 + 1) %>%
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>%
  group_by(permno, year, quarter) %>%
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>%
  ungroup() %>%
  group_by(permno) %>%
  mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
         port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>%
  mutate(excess_return = ret_cumulative - port_ret_cumulative) %>%
  select(permno, year, quarter, excess_return) %>%
  drop_na() %>%
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression

reg[[5]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[6]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))



lag_period <- 6L

excess_return <- port_ret %>%
  mutate(ret = ret + 1,
         port_ret = port_ret/100 + 1) %>%
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>%
  group_by(permno, year, quarter) %>%
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>%
  ungroup() %>%
  group_by(permno) %>%
  mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
         port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>%
  mutate(excess_return = ret_cumulative - port_ret_cumulative) %>%
  select(permno, year, quarter, excess_return) %>%
  drop_na() %>%
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression

reg[[7]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[8]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))


lag_period <- 8L

excess_return <- port_ret %>%
  mutate(ret = ret + 1,
         port_ret = port_ret/100 + 1) %>%
  mutate(year = year(rdate),
         month = month(rdate),
         quarter = case_when(month %in% c(1, 2, 3) ~ 1,
                             month %in% c(4, 5, 6) ~ 2,
                             month %in% c(7, 8, 9) ~ 3,
                             month %in% c(10, 11, 12) ~ 4)) %>%
  group_by(permno, year, quarter) %>%
  summarise(ret = prod(ret),
            port_ret = prod(port_ret)) %>%
  ungroup() %>%
  group_by(permno) %>%
  mutate(ret_cumulative = rollapply(ret, lag_period, prod, na.rm = T, align = 'left', fill = NA),
         port_ret_cumulative = rollapply(port_ret, lag_period, prod, na.rm = T, align = 'left', fill = NA)) %>%
  mutate(excess_return = ret_cumulative - port_ret_cumulative) %>%
  select(permno, year, quarter, excess_return) %>%
  drop_na() %>%
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>% 
  distinct() %>% 
  drop_na() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year', 'quarter')) %>%
  distinct() %>%
  mutate(across(everything(), ~replace_na(.x, 0)))

## fixed effect - time
data <- data %>% 
  mutate(year_quarter = str_c(year, 'Q', quarter))

# regression
reg[[9]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year_quarter | 0 | firm_gvkey + year_quarter,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

reg[[10]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(return) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol != 0 & !is.infinite(capx) & bm > 0  & size != 0 & bm != 0 & return != 0))

stargazer(reg, type = 'text')