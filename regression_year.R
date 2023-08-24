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

## price, return, volume, shrout
res <- dbSendQuery(wrds, "select date, permno, prc, ret, vol, shrout
                          from crsp.msf 
                          where date between '01/31/2010' and '06/30/2023'")

crsp_msf <- dbFetch(res, n = n_pull); dbClearResult(res)

## book_value, capx, roa, tant, adv_exp
res <- dbSendQuery(wrds, "select gvkey, datadate, fyear, ceq, capx, sale, at, ni, che, rect, invt, ppent, xad
                          from comp.funda
                          where datadate between '01/31/2010' and '06/30/2023'")

comp_funda <- dbFetch(res, n = n_pull); dbClearResult(res)

comp_funda <- comp_funda %>% 
  mutate(gvkey = as.numeric(gvkey),
         xad = replace_na(xad, 0)) %>% 
  drop_na()

## linktable
res <- dbSendQuery(wrds, "select *
                          from crsp.ccmxpf_linktable
                          where LINKTYPE in ('LU', 'LC', 'LD', 'LF', 'LN', 'LO', 'LS', 'LX')")

ccmxpf <- dbFetch(res, n = n_pull); dbClearResult(res)

ccmxpf <- ccmxpf %>% 
  mutate(gvkey = as.numeric(gvkey),
         linkyr = year(linkdt),
         linkendyr = year(linkenddt))

## ff_port_return
port_ret <- fread('ff_port_ret.csv')


env_score <- select(fread('./factset_revere_companies/Environmental_Dimension/year/1-s-firm.csv'), 
                    firm_gvkey, year) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/year/2-s-firm.csv'), 
                   firm_gvkey, year)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/year/3-s-firm.csv'), 
                   firm_gvkey, year)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/year/1-c-firm.csv'), 
                   firm_gvkey, year)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/year/2-c-firm.csv'), 
                   firm_gvkey, year)) %>% 
  bind_rows(select(fread('./factset_revere_companies/Environmental_Dimension/year/3-c-firm.csv'), 
                   firm_gvkey, year)) %>% 
  distinct()

env_score <- env_score %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/1-s-firm.csv'),
            by = c('firm_gvkey', 'year')) %>%
  mutate(first_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/2-s-firm.csv'),
            by = c('firm_gvkey', 'year')) %>% 
  mutate(second_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/3-s-firm.csv'),
            by = c('firm_gvkey', 'year')) %>% 
  mutate(third_degree_supplier_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/1-c-firm.csv'),
            by = c('firm_gvkey', 'year')) %>% 
  mutate(first_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/2-c-firm.csv'),
            by = c('firm_gvkey', 'year')) %>% 
  mutate(second_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  left_join(fread('./factset_revere_companies/Environmental_Dimension/year/3-c-firm.csv'),
            by = c('firm_gvkey', 'year')) %>% 
  mutate(third_degree_customer_env_decline = environmental_score_decline_rate) %>% 
  select(-environmental_score_decline_rate) %>% 
  mutate(across(everything(), ~replace_na(.x, 0)))

env_score <- sqldf("SELECT a.*, b.*
                    FROM env_score AS a
                    LEFT JOIN ccmxpf AS b
                    ON (a.firm_gvkey = b.gvkey) 
                    AND (a.year >= b.linkyr OR b.linkyr IS NULL) 
                    AND (a.year <= b.linkendyr OR b.linkendyr IS NULL)") %>% 
  transmute(firm_gvkey = firm_gvkey,
            firm_permno = lpermno,
            year = year,
            first_degree_supplier_env_decline = first_degree_supplier_env_decline,
            second_degree_supplier_env_decline = second_degree_supplier_env_decline,
            third_degree_supplier_env_decline = third_degree_supplier_env_decline,
            first_degree_customer_env_decline = first_degree_customer_env_decline,
            second_degree_customer_env_decline = second_degree_customer_env_decline,
            third_degree_customer_env_decline = third_degree_customer_env_decline
  ) %>% 
  distinct()

# control vars
## size
size <- crsp_msf %>% 
  mutate(size = abs(prc)*shrout) %>% 
  select(permno, date, size) %>% 
  mutate(year = year(date)) %>% 
  group_by(permno, year) %>% 
  summarise(size = dplyr::last(size, na_rm = T)) %>% 
  drop_na()

## bm
book_value <- env_score %>% 
  select(firm_gvkey, firm_permno, year) %>% 
  distinct() %>% 
  left_join(select(comp_funda, gvkey, fyear, ceq),
            by=c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  mutate(last_year = year - 1) %>% 
  select(firm_gvkey, firm_permno, ceq, last_year)

market_value <- size %>% 
  mutate(last_year = year - 1)

book_to_market <- market_value %>% 
  left_join(book_value, by=c('permno' = 'firm_permno', 'last_year')) %>% 
  mutate(bm = ceq / size) %>% 
  drop_na() %>% 
  ungroup() %>% 
  select(firm_gvkey, year, bm)

## return
return <- crsp_msf %>% 
  select(permno, date, ret) %>% 
  mutate(year = year(date),
         ret = ret + 1) %>% 
  group_by(permno, year) %>% 
  summarise(ret = prod(ret, na.rm = T)) %>% 
  mutate(ret = lag(ret, 1L))

## prc
prc <- crsp_msf %>% 
  select(permno, date, prc) %>% 
  mutate(year = year(date)) %>% 
  group_by(permno, year) %>% 
  summarise(prc = abs(dplyr::last(prc, na_rm = T))) %>% 
  mutate(prc = lag(prc, 2L)) %>% 
  drop_na()

## vol
vol <- crsp_msf %>% 
  select(date, permno, vol) %>% 
  mutate(year = year(date)) %>% 
  group_by(permno, year) %>% 
  summarise(vol = sum(vol, na.rm = T)) %>% 
  mutate(vol = lag(vol, 2L)) %>% 
  drop_na()

## capx
capx <- comp_funda %>% 
  mutate(capx = capx / sale) %>% 
  select(gvkey, fyear, capx)

## tant
tant <- comp_funda %>% 
  mutate(tant = (che + rect + invt + ppent) / at) %>% 
  select(gvkey, fyear, tant)

## roa
roa <- comp_funda %>% 
  mutate(roa = ni / at) %>% 
  select(gvkey, fyear, roa)

## adv_exp
adv_exp <- comp_funda %>% 
  mutate(adv_exp = xad) %>% 
  select(gvkey, fyear, adv_exp)

# dep var
lag_period <- 1L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate)) %>% 
  group_by(permno, year) %>% 
  summarise(ret = prod(ret) - 1,
            port_ret = prod(port_ret) - 1) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>%
  drop_na()


# regression
reg <- list()

reg[[1]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))

reg[[2]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))


summary(reg[[1]])

summary(reg[[2]])

# dep var
lag_period <- 2L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate)) %>% 
  group_by(permno, year) %>% 
  summarise(ret = prod(ret) - 1,
            port_ret = prod(port_ret) - 1) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>%
  drop_na()


# regression
reg[[3]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))

reg[[4]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))


summary(reg[[3]])

summary(reg[[4]])


# dep var
lag_period <- 3L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate)) %>% 
  group_by(permno, year) %>% 
  summarise(ret = prod(ret) - 1,
            port_ret = prod(port_ret) - 1) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>%
  drop_na()


# regression

reg[[5]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))

reg[[6]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))


summary(reg[[5]])

summary(reg[[6]])

# dep var
lag_period <- 4L

excess_return <- port_ret %>% 
  mutate(ret = ret + 1, 
         port_ret = port_ret/100 + 1) %>% 
  mutate(year = year(rdate)) %>% 
  group_by(permno, year) %>% 
  summarise(ret = prod(ret) - 1,
            port_ret = prod(port_ret) - 1) %>% 
  ungroup() %>% 
  group_by(permno) %>% 
  mutate(excess_return = ret - port_ret) %>% 
  mutate(excess_return = lead(excess_return, n = lag_period)) %>% 
  select(permno, year, excess_return) %>% 
  drop_na() %>% 
  distinct()

data <- env_score %>% 
  left_join(excess_return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(size, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(book_to_market, by = c('firm_gvkey', 'year')) %>% 
  distinct() %>% 
  left_join(return, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(vol, by = c('firm_permno' = 'permno', 'year')) %>% 
  distinct() %>% 
  left_join(capx, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>% 
  left_join(tant, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(roa, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>% 
  distinct() %>% 
  left_join(adv_exp, by = c('firm_gvkey' = 'gvkey', 'year' = 'fyear')) %>%
  distinct() %>%
  drop_na()


# regression
reg[[7]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + second_degree_supplier_env_decline + third_degree_supplier_env_decline + first_degree_customer_env_decline + second_degree_customer_env_decline + third_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))

reg[[8]] <- felm(
  excess_return ~
    + first_degree_supplier_env_decline + first_degree_customer_env_decline + log(size) + log(bm) + log(ret) + log(vol) + capx + tant + roa + adv_exp
  | firm_gvkey + year | 0 | firm_gvkey + year,
  data = filter(data, vol > 0 & !is.infinite(capx) & bm > 0 & ret > 0))


summary(reg[[7]])

summary(reg[[8]])

stargazer(reg, type = 'text')

save(adv_exp, book_to_market, capx, ccmxpf, comp_funda, crsp_msf, excess_return, port_ret, prc, reg, return, roa, size, tant, vol, file = 'year_data.RData')
