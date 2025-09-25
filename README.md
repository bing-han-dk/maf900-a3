# maf900-a3

**Logic of estimation stage**

  if we have estart=1930, eend = 1934, tstart=1935, tend = 1938
  
  n = 0
  for i in range(tstart, tend) # e.g. i = 1935,1936,1937,1938
    step1:
      generate individual stock beta use data from estart to eend+n (year) and get the dataframe beta_e 
    step2:
      generate 20 portfolio beta for every monthy of year i
      every monthy use portflolio group info (1~20) from beta_f and beta info from beta_e
      if any stock is delisted in current month,exclude the stock (by merge the data_delist)
      calculate portfolio beta by average the stock beta
      save portfolio beta (20 rows for each month) into dataframe beta_p 
    step3:
      n+1
    (to next i)
    
    *Note:
      beta_f with columns - permno,beta,portfolio(1~20)
      beta_e with columns - permno,beta
      data_delist with columns - permno, permco, dlstdt(date type)
      beta_p with columns - year,month,portfolio,beta
  
    *the logic of step1 is given: 
      estimation_data <-data_ret %>% filter(year >= estart & year <= eend+n)
      beta_e <- estimation_data %>%
        group_by(permno) %>%
        do(data.frame(beta = estimate_capm(data = ., min_obs = 60))) %>%
        ungroup()%>%
        filter(!is.na(beta))