# Fama and MacBeth (1973)

### Data 

A. CRSP Data

Monthly total stock returns (including dividends and capital gains, adjusted for splits and stock dividends) (to compute individual stock beta and portfolio returns)

Stock identifiers and delisting dates (to determine whether a stock is available in a given portfolio formation, beta estimation, or testing period)

1926.1 - 1968.6

New York Stock Exchange

All common stocks

B. Based on Gonedes (1973),

A stock must be available in the first month of the testing period to be included in a portfolio.

The stock must have complete data for the preceding 5-year beta estimation period.

The stock must have at least 4 years of data in the portfolio formation period.

#### Methodology

Each complete process consists of three stages.

Portfolio formation stage (seven years except for the first):

- Individual stock betas are estimated based on their monthly returns, and stocks are divided into twenty equally weighted portfolios based on their beta values.

Beta estimation stage (five years):

- Individual stock betas are reestimated using a rolling window, and the portfolio's average beta, squared term, and non-beta risk (measured by the standard deviation of the market model residuals) are calculated. Individual stock betas are updated annually, while portfolio betas are adjusted monthly based on changes in the constituent stocks.

Testing stage (four years except for the last):

- The returns of the twenty portfolios are calculated monthly, and cross-sectional regressions are performed to test the return-risk relationships. The monthly regression coefficients are then averaged over time to form the final test results.

The entire process continues rolling forward according to the framework of "seven years of formation + five years of estimation + four years of testing ," resulting in a total of nine complete testing cycles.

PS (in-class insight): The essential purpose of "using estimation state“ is to use the new error term distribution to hedge the problem of overvaluation (undervaluation) of the high (low) beta portfolios in formation stage.

#### Ambiguity

Summary of ambiguities and my replication decisions

Individual beta estimation: rolling vs. one-time; we use one-time regressions.

Portfolio returns: Dollar weighting or share weighting? not fully defined in the paper; we take simple averages of stock returns.

For the Rp~Rm regression in estimation stage, the authors should give more information. Because it relates to the Table 2 results. e.g. We need to calculate the portfolio-level return first. During this step, data sample requirement (5 years) can avoid the delisting issues.

### Key Logic of estimation stage

```
if we have estart=1930, eend = 1934, tstart=1935, tend = 1938

set a cursor n = 0

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
```