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
                  where date < '1969-01-01' 
                  and exchcd = 1
                  and SHRCD in (10,11) 
                  )
                  select t2.*, t1.date, t1.RET*100 as ret
                  from crsp_a_stock.msf t1 
                  inner join SHRCD t2 
                  on t1.PERMNO = t2.PERMNO 
                  and t1.date < '1969-01-01'
                  and hexcd = 1
                  and t1.retx is not null;
      ")
data_ret <- dbFetch(res_ret, n = -1)
dbClearResult(res_ret)
head(data_ret)

data_ret_bk<-data_ret


# crsp delist data 
res_delist <- dbSendQuery(wrds, "
                  select permno,permco,dlstdt 
                  from crsp_a_stock.msedelist 
                  where dlstdt < '1969-01-01';
      ")
data_delist <- dbFetch(res_delist, n = -1)
dbClearResult(res_delist)
head(data_delist)


# Fama-French factor data 
res_factor <- dbSendQuery(wrds, "
                  select dateff, mktrf*100 as mktrf, rf*100 as rf
                  from ff_all.factors_monthly 
                  where date < '1969-01-01';
      ")
data_factor <- dbFetch(res_factor, n = -1)
dbClearResult(res_factor)
head(data_factor)
       

data_ret <- data_ret %>%
  mutate(year = year(date), month = month(date))

data_factor <- data_factor %>%
  mutate(year = year(dateff), month = month(dateff))

data_ret <- data_ret %>%
  left_join(data_factor %>% select(year, month, rf, mktrf), by = c("year","month")) %>%
  mutate(exret = ret - rf)

data_ret <- data_ret %>%
  filter(!is.na(rf) & !is.na(exret))


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


# get stock i's beta  
estimate_capm <- function(data, min_obs = 1) {
  if (nrow(data) < min_obs) {
    return(data.frame(beta = NA, se = NA)) 
  } else {
    fit <- lm(exret ~ mktrf, data = data)
    beta <- as.numeric(coefficients(fit)[2])
    se   <- sd(resid(fit)) 
    return(data.frame(beta = beta, se = se))
  }
}

# get stock i's betas (rolling version) 
roll_capm_estimation <- function(data, months, min_obs) {
  data <- data |> arrange(month)
  betas <- slide_period_vec(
    .x = data,
    .i = data$month,# index for rolling window (first-day format)
    .period = "month",
    .f = ~ estimate_capm(., min_obs),# function or formula
    .before = months - 1,# use current and past months - 1 periods
    .complete = FALSE )
  return(tibble(
    month = unique(data$month),
    beta = betas ))}



#--- formation stage ---


formation_list <- lapply(periods, function(p) {
  data_ret %>%
    filter(year >= p$fstart & year <= p$fend)
})

formation_data <- formation_list[[1]]
n_distinct(formation_data$permno) #729
n_distinct(formation_data$permno[formation_data$date == as.Date("1926-07-31")]) #483
# maybe we can check the permco or the SHRCD 11


beta_f <- formation_data %>%
  group_by(permno) %>%
  do(data.frame(beta = estimate_capm(data = ., min_obs = 48-6))) %>%
  ungroup()%>%
  filter(!is.na(beta))

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

beta_f<-assign_portfolios(beta_f) 
beta_f %>% count(portfolio) # check retult


#--- estimation stage ---

# -----------for version 

estart <- 1930
eend   <- 1934
tstart <- 1935
tend   <- 1938

n <- 0

# portfolio beta of entire period 
beta_p_all <- list() 

for (i in tstart:tend) {
  message("Processing year ", i, " ...")
  
  # re-cumpute individual beta & s(e)
  estimation_data <- data_ret %>% filter(year >= estart & year <= eend+n)
  beta_e <- estimation_data %>%
    group_by(permno) %>%
    do(estimate_capm(data = ., min_obs = 60)) %>%
    ungroup() %>%
    filter(!is.na(beta))
  
  # portfolio beta of year i 
  beta_p_year <- list()
  
  for (m in 1:12) {
    
    # month first day
    cutoff_date <- as.Date(sprintf("%d-%02d-01", i, m))
    
    # get all delisted stocks 
    delist_set <- data_delist %>%
      filter(!is.na(dlstdt), dlstdt <= cutoff_date) %>%
      pull(permno)
    
    # merge beta_f and beta_e 
    df_m <- beta_f %>%
      inner_join(beta_e, by = "permno") %>%
      filter(!(permno %in% delist_set))
    
    # calculate portfolio beta 
    beta_p_m <- df_m %>%
      group_by(portfolio) %>%
      summarise(beta = mean(beta.y, na.rm = TRUE), .groups = "drop") %>%
      mutate(year = i, month = m) %>%
      select(year, month, portfolio, beta)
    
    beta_p_year[[as.character(m)]] <- beta_p_m
  }
  
  beta_p_all[[as.character(i)]] <- bind_rows(beta_p_year)
  n <- n + 1
}

beta_p <- bind_rows(beta_p_all)



# ---------function version 

estart <- 1930
eend   <- 1934
tstart <- 1935
tend   <- 1938

n <- 0

# portfolio beta of entire period 
beta_p_all <- purrr::map_dfr(tstart:tend, function(i) {
  message("Processing year ", i, " ...")
  
  # re-cumpute individual beta & s(e)
  estimation_data <- data_ret %>% filter(year >= estart & year <= eend+n)
  beta_e <- estimation_data %>%
    group_by(permno) %>%
    do(estimate_capm(data = ., min_obs = 60)) %>%
    ungroup() %>%
    filter(!is.na(beta))
  
  
  # portfolio beta of year i (all months)
  beta_p_year <- purrr::map_dfr(1:12, function(m) {
    
    # month first day
    cutoff_date <- as.Date(sprintf("%d-%02d-01", i, m))
    
    # get all delisted stocks 
    delist_set <- data_delist %>%
      filter(!is.na(dlstdt), dlstdt <= cutoff_date) %>%
      pull(permno)
    
    # merge beta_f and beta_e, exclude delisted
    df_m <- beta_f %>%
      inner_join(beta_e, by = "permno") %>%
      filter(!(permno %in% delist_set))
    
    # calculate portfolio beta 
    df_m %>%
      group_by(portfolio) %>%
      summarise(beta = mean(beta.y, na.rm = TRUE), .groups = "drop") %>%
      mutate(year = i, month = m) %>%
      select(year, month, portfolio, beta)
  })
  
  n <<- n + 1
  beta_p_year
})

