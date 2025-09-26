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
                  on t1.PERMNO = t2.PERMNO 
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
                  select permno,permco,dlstdt,dlret
                  from crsp_a_stock.msedelist 
                  where dlstdt < '1968-07-01';
      ")
data_delist <- dbFetch(res_delist, n = -1)
dbClearResult(res_delist)
head(data_delist)


# rf data 
res_factor <- dbSendQuery(wrds, "
                  select dateff, rf
                  from ff_all.factors_monthly 
                  where date < '1968-07-01';
      ")
data_factor <- dbFetch(res_factor, n = -1)
dbClearResult(res_factor)
head(data_factor)


data_factor<-data_factor %>% mutate(year = year(dateff), month = month(dateff))


data_ret <- data_ret %>%
  mutate(year = year(date), month = month(date)) %>%
  group_by(year, month) %>%
  mutate(
    mkt = mean(ret, na.rm = TRUE)
  ) %>%
  ungroup()%>%
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


p <- 2
message("Processing period ", periods[[p]]$name, " ...")
fstart <- periods[[p]]$fstart 
fend   <- periods[[p]]$fend 
estart <- periods[[p]]$estart 
eend   <- periods[[p]]$eend 
tstart <- periods[[p]]$tstart 
tend   <- periods[[p]]$tend 


#--- formation stage ---

formation_data<-data_ret %>% filter(year >= fstart & year <= fend)

message(length(unique(formation_data$permno)), " stocks in formation_data")

beta_f <- formation_data %>% 
  group_by(permno) %>%
  do(estimate_beta(data = ., min_obs = 48)) %>% 
  ungroup()%>%
  filter(!is.na(beta))

message(length(unique(beta_f$permno)), " stocks in beta_f")


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
  estimatation_data<-data_ret %>% filter(year >= estart & year <= (eend+n))
  
  message(length(unique(estimatation_data$permno)), " stocks in estimatation_data")
  
  beta_e <- estimatation_data %>% 
    group_by(permno) %>%
    do(estimate_beta(data = ., min_obs = 60)) %>%
    ungroup() %>%
    filter(!is.na(beta))
  
  message(length(unique(beta_e$permno)), " stocks in beta_e")
  
  # portfolio beta of year i (all months)
  beta_p_year <- purrr::map_dfr(1:12, function(m) {
    
    # merge month i's delisting return
    data_ret_m <- data_ret %>%
      filter(year == i, month == m) %>%
      left_join(
        data_delist %>% select(permno, dlstdt, dlret)
        ,by = "permno"
        ) %>%
      mutate(
        ret = ifelse(
          !is.na(dlstdt)&year(dlstdt)==i&month(dlstdt)==m,
          (1+ret)*(1+coalesce(dlret, 0))-1, ret))
    
    # make delisting set
    first_date  <- as.Date(sprintf("%d-%02d-01", i, m))
    delist_set <- data_delist %>%
      filter(!is.na(dlstdt), dlstdt < first_date) %>%
      pull(permno)
    
    # # merge beta_f, beta_e and data_ret, excluding delisted 
    df_m <- beta_f %>%
      inner_join(beta_e, by = "permno") %>%
      filter(!(permno %in% delist_set)) %>%
      inner_join(
        data_ret_m %>% select(permno, ret),
        by = "permno"
      ) %>%
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
beta_p_est <- beta_f %>%
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

stat_t2 <- beta_p_est %>%
  left_join(
    beta_p_all %>%
      filter(year == tstart, month == 1) %>% 
      select(portfolio, sd_resid_i = sd_resid),
    by = "portfolio"
  ) %>%
  mutate(sd_resid_over = sd_resid / sd_resid_i)


# --- two parameter regression --- 

beta_p_all <- beta_p_all %>%
  mutate(beta2 = beta^2)


run_fmb <- function(data, formula_str) {
  data %>%
    group_by(year, month) %>%
    group_modify(~ {
      model <- lm(as.formula(formula_str), data = .x)
      tidy_res <- broom::tidy(model)
      tidy_res$r_squared <- summary(model)$r.squared
      tidy_res
    }) %>%
    ungroup()
}


fmb_model1 <- run_fmb(beta_p_all, "ret ~ beta")
fmb_model2 <- run_fmb(beta_p_all, "ret ~ beta + beta2")
fmb_model3 <- run_fmb(beta_p_all, "ret ~ beta + sd_resid")
fmb_model4 <- run_fmb(beta_p_all, "ret ~ beta + beta2 + sd_resid")


fmb_coef_stats <- function(fmb_model, rf_data) {
  fmb_model %>%
    left_join(rf_data %>% select(year, month, rf), by = c("year", "month")) %>%
    group_by(term) %>%
    summarise(
      mean_gamma = mean(estimate, na.rm = TRUE),
      sd_gamma = sd(estimate, na.rm = TRUE),
      t_stat = mean_gamma / (sd_gamma / sqrt(n())),
      
      acf1 = acf(estimate, plot = FALSE)$acf[2],  # ρ₀(γ)
      
      mean_gamma_rf = mean(estimate - rf, na.rm = TRUE),
      t_gamma_rf = mean_gamma_rf / (sd_gamma / sqrt(n())),
      acf1_gamma_rf = acf(estimate - rf, plot = FALSE)$acf[2],
      
      .groups = "drop"
    )
}

#--- table 3 
fmb_coef1 <-fmb_coef_stats(fmb_model1, data_factor)
fmb_coef2 <-fmb_coef_stats(fmb_model2, data_factor)
fmb_coef3 <-fmb_coef_stats(fmb_model3, data_factor)
fmb_coef4 <-fmb_coef_stats(fmb_model4, data_factor)



# print No. of securities available (for table 1)
# print No. of securities meeting data requirements (for table 1)

