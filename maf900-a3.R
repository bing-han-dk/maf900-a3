library(RPostgres)
library(dplyr)
library(lubridate)
library(tidyverse)
library(broom)


# generate wrds connection 
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='bing_han')


# crsp monthly retrun data 
res_ret <- dbSendQuery(wrds, "
                  with SHRCD as 
                  (select distinct permno,permco 
                  from crsp_a_stock.mse 
                  where date < '1968-07-01'
                  and exchcd = 1
                  and SHRCD in (10,11)
                  )
                  select t2.*, t1.date, t1.RET as ret
                  from crsp_a_stock.msf t1 
                  inner join SHRCD t2 
                  on t1.PERMNO = t1.PERMNO 
                  and t1.date < '1968-07-01'
                  and hexcd = 1
                  and t1.retx is not null;
      ")
data_ret <- dbFetch(res_ret, n = -1)
dbClearResult(res_ret)
head(data_ret)

# backup data 
data_ret_bk<-data_ret


# crsp delist data 
res_delist <- dbSendQuery(wrds, "
                  select permno,permco,dlstdt 
                  from crsp_a_stock.msedelist 
                  where dlstdt < '1968-07-01';
      ")
data_delist <- dbFetch(res_delist, n = -1)
dbClearResult(res_delist)
head(data_delist)


## Fama-French factor data 
#res_factor <- dbSendQuery(wrds, "
#                  select dateff, mktrf, rf
#                  from ff_all.factors_monthly 
#                  where date between < '1968-07-01';
#      ")
#data_factor <- dbFetch(res_factor, n = -1)
#dbClearResult(res_factor)
#head(data_factor)


#data_ret <- data_ret %>%
#  mutate(year = year(date), month = month(date)) %>%
#  left_join(
#    data_factor %>%
#      mutate(year = year(dateff), month = month(dateff)) %>%
#      select(year, month, rf, mktrf),
#    by = c("year", "month")
#  ) %>%
#  # mutate(exret = ret - rf) %>%
#  mutate(mkt = mktrf + rf) %>%
#  filter(!is.na(rf))


data_ret <- data_ret %>%
  mutate(year = year(date), month = month(date)) %>%
  #left_join(
  #  data_factor %>%
  #    mutate(year = year(dateff), month = month(dateff)) %>%
  #    select(year, month, rf),
  #  by = c("year", "month")
  #) %>%
  group_by(year, month) %>%
  mutate(
    mkt = mean(ret, na.rm = TRUE)
  ) %>%
  ungroup()%>%
  # mutate(rf = ifelse(year == 1926 & month < 7 & is.na(rf), 0.0022, rf))%>%
  filter(!is.na(ret))


# define periods
periods <- list(
  list(name = "P1",
    fstart = 1926, fend = 1929,
    estart = 1930, eend = 1934,
    tstart = 1935, tend = 1938
  ),
  list(name = "P2",
    fstart = 1927, fend = 1933,
    estart = 1934, eend = 1938,
    tstart = 1939, tend = 1942
  ),
  list(name = "P3",
    fstart = 1931, fend = 1937,
    estart = 1938, eend = 1942,
    tstart = 1943, tend = 1946
  ),
  list(name = "P4",
    fstart = 1935, fend = 1941,
    estart = 1942, eend = 1946,
    tstart = 1947, tend = 1950
  ),
  list(name = "P5",
    fstart = 1939, fend = 1945,
    estart = 1946, eend = 1950,
    tstart = 1951, tend = 1954
  ),
  list(name = "P6",
    fstart = 1943, fend = 1949,
    estart = 1950, eend = 1954,
    tstart = 1955, tend = 1958
  ),
  list(name = "P7",
    fstart = 1947, fend = 1953,
    estart = 1954, eend = 1958,
    tstart = 1959, tend = 1962
  ),
  list(name = "P8",
    fstart = 1951, fend = 1957,
    estart = 1958, eend = 1962,
    tstart = 1963, tend = 1966
  ),
  list(name = "P9",
    fstart = 1955, fend = 1961,
    estart = 1962, eend = 1966,
    tstart = 1967, tend = 1968
  )
)


# beta regression
estimate_beta <- function(data, min_obs = 1) {
  if (nrow(data) < min_obs) {
    return(data.frame(beta=NA,se_beta=NA,R2=NA
                      ,sd_ret=NA,sd_resid=NA)) 
  } else {
    fit <- lm(ret ~ mkt, data = data)
    beta<- as.numeric(coefficients(fit)[2])
    se_beta <- summary(fit)$coefficients["mkt", "Std. Error"]
    R2  <- summary(fit)$r.squared
    sd_ret <- sd(data$ret, na.rm = TRUE)
    sd_resid <- sd(resid(fit)) 
    return(data.frame(beta=beta,se_beta=se_beta,R2=R2
                      ,sd_ret=sd_ret,sd_resid=sd_resid))
  }
}

# portfolio formation function
assign_portfolios <- function(beta_df, n_port = 20) {
  N <- nrow(beta_df)
  n_each <- floor(N / n_port)
  remainder <- N - n_each * n_port
  extra_first <- floor(remainder / 2)
  extra_last  <- ceiling(remainder / 2)
  beta_df <- beta_df %>% arrange(beta)
  if (n_port > 2) {
    mid_portfolios <- rep(2:(n_port-1), each = n_each)
  } else {
    mid_portfolios <- c()
  }
  portfolio_vec <- c(
    rep(1, n_each + extra_first),
    mid_portfolios,
    rep(n_port, n_each + extra_last)
  )
  if (length(portfolio_vec) != N) {
    stop("Length mismatch: check input data")
  }
  beta_df$portfolio <- portfolio_vec
  return(beta_df)
}


# --- FAMA AND MACBETH (1973) 


p <- 1
message("Processing period ", periods[[p]]$name, " ...")
fstart <- periods[[p]]$fstart 
fend   <- periods[[p]]$fend 
estart <- periods[[p]]$estart 
eend   <- periods[[p]]$eend 
tstart <- periods[[p]]$tstart 
tend   <- periods[[p]]$tend 


#--- formation stage ---


beta_f <- data_ret %>% 
  filter(year >= fstart & year <= fend) %>%
  group_by(permno) %>%
  do(estimate_beta(data = ., min_obs = 48)) %>% 
  ungroup()%>%
  filter(!is.na(beta))


# print No. of securities available (for table 1)


# assign portfolios
beta_f<-assign_portfolios(beta_f)

# check assigned result
beta_f %>% count(portfolio) 



#--- estimation stage ---


# portfolio beta of entire period 
beta_p_all <- purrr::map_dfr(tstart:tend, function(i) {
  # Using purrr::map_dfr & anonymous function can effectively 
  # avoid using for loop to generate too many intermediate variables,
  # and avoid passing too many parameters when defining normal.
  
  message("Processing year ", i, " ...")
  
  n <- i - tstart

  # re-cumpute individual beta & s(e)
  beta_e <- data_ret %>% 
    filter(year >= estart & year <= (eend+n)) %>%
    group_by(permno) %>%
    do(estimate_beta(data = ., min_obs = 60)) %>%
    ungroup() %>%
    filter(!is.na(beta))
  
  
  # print No. of securities meeting data requirements (for table 1)
  
  
  # portfolio beta of year i (all months)
  beta_p_year <- purrr::map_dfr(1:12, function(m) {
    
    # monthly first day
    cutoff_date <- as.Date(sprintf("%d-%02d-01", i, m))
    
    # get all delisted stocks 
    delist_set <- data_delist %>%
      filter(!is.na(dlstdt), dlstdt <= cutoff_date) %>%
      pull(permno)
    
    # merge beta_f, beta_e and data_ret, excluding delisted
    df_m <- beta_f %>%
      inner_join(beta_e, by = "permno") %>%
      filter(!(permno %in% delist_set)) %>%
      inner_join(
        data_ret %>%
          filter(year == i, month == m) %>%
          select(permno, ret),
        by = "permno"
      ) %>%
      #only keep beta_e's beta and sd_resid
      select(permno, portfolio, beta = beta.y, sd_resid = sd_resid.y, ret)
    
    # calculate portfolio beta, se and return 
    df_m %>%
      group_by(portfolio) %>%
      summarise(
        beta = mean(beta, na.rm = TRUE),
        sd_resid = mean(sd_resid,   na.rm = TRUE),
        ret  = mean(ret,  na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(year = i, month = m) %>%
      select(year, month, portfolio, beta, sd_resid, ret)
  })
  
  beta_p_year
  
})


# portfolio level estimation (Table 2)
beta_p_stat <- beta_f %>%
  inner_join(
    data_ret %>%
      filter(year >= estart & year <= eend) %>%
      select(permno, ret, mkt, year, month),
    by = "permno"
  ) %>%
  group_by(year, month, portfolio) %>%
  summarise(
    ret = mean(ret, na.rm = TRUE), 
    mkt = mean(mkt, na.rm = TRUE), 
    .groups = "drop"
  )%>%
  group_by(portfolio) %>%
  do(estimate_beta(data = ., min_obs = 60)) %>%
  ungroup()



# --- table 2 --- 

stat_t2 <- beta_p_stat %>%
  left_join(
    beta_p_all %>%
      filter(year == tstart, month == 1) %>% 
      select(portfolio, sd_resid_i = sd_resid),
    by = "portfolio"
  ) %>%
  mutate(sd_resid_over = sd_resid / sd_resid_i)


# --- 




