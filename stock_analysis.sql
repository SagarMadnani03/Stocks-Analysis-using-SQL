create database stock_analysis;

use stock_analysis;

select count(*) from bharti_airtel;
select count(*) from maruti_suzuki;
select count(*) from ultra_cemco;

alter table bharti_airtel
add column new_date date;

alter table maruti_suzuki
add column new_date date;

alter table ultra_cemco
add column new_date date;

desc bharti_airtel;
desc maruti_suzuki;
desc ultra_cemco;

# Stock Analysis 

#Firstly we combine the data and create the view 
Drop view A1;
create view A1 as (Select symbol, high, low from bharti_airtel
union 
Select symbol, high, low from maruti_suzuki
union
Select symbol, high, low from ultra_cemco);

Select * from A1;

create table ultra_cemco_bkp as select * from ultra_cemco;


#update bharti_airtel
#set new_date='''




#1. Calculating Volatility of all 3 stocks.
#Formula : Avg(High - Low)

Select symbol, round(avg(high - low),2) as avg_volatility,
dense_rank() over(order by avg(high - low))
as ranking from A1
where symbol <>'BHARTI'
group by symbol;


#2. Calculating the drawdown/ fall in stock price

/* A. Calculating for Bharti_Airtel Stock Price */
set @pre_covid_bha_artl_price:=(Select close from bharti_airtel where date='2020-03-20'); 

set @post_covid_bha_artl_price:=(Select close from bharti_airtel where date='2020-03-23');

Select @pre_covid_bha_artl_price;

Select @post_covid_bha_artl_price;

Select round(((-@pre_covid_bha_artl_price+@post_covid_bha_artl_price)/@pre_covid_bha_artl_price),5)* 100 as Bharti_Airtel_DrawDown;

/* B. Calculating for Maruti_Suzuki Stock Price */

set @pre_covid_mar_suz_price:=(Select close from maruti_suzuki where date='2020-03-20'); 

set @post_covid_mar_suz_price:=(Select close from maruti_suzuki where date='2020-03-23');

Select @pre_covid_mar_suz_price;

Select @post_covid_mar_suz_price;

Select round(((-@pre_covid_mar_suz_price+@post_covid_mar_suz_price)/@pre_covid_mar_suz_price),5)* 100 as Maruti_Suzuki_DrawDown;

/* C. Calculating for Ultra Cement Stock Price */

set @pre_covid_ult_cem_price:=(Select close from ultra_cemco where date='2020-03-20'); 

set @post_covid_ult_cem_price:=(Select close from ultra_cemco where date='2020-03-23');

Select @pre_covid_ult_cem_price;

Select @post_covid_ult_cem_price;

Select round(((-@pre_covid_ult_cem_price+@post_covid_ult_cem_price)/@pre_covid_ult_cem_price),5)* 100 as Ultra_Cement_DrawDown;

Select round(((-@pre_covid_bha_artl_price+@post_covid_bha_artl_price)/@pre_covid_bha_artl_price),5)* 100 as DrawDown_Percentage
union
Select round(((-@pre_covid_mar_suz_price+@post_covid_mar_suz_price)/@pre_covid_mar_suz_price),5)* 100 as Maruti_Suzuki_DrawDown
union
Select round(((-@pre_covid_ult_cem_price+@post_covid_ult_cem_price)/@pre_covid_ult_cem_price),5)* 100 as Ultra_Cement_DrawDown;

#3. Finding Recovery days

set @date_close_more_than_precovid_maruti:=
(Select date from
(Select date, close, row_number() over(order by date) as rank_based_on_date from maruti_suzuki
where date between '2020-03-23' and '2021-04-30' and close>=@pre_covid_mar_suz_price) as A3
where rank_based_on_date =1);

select @date_close_more_than_precovid_maruti;

select timestampdiff(day,"2020-03-23", @date_close_more_than_precovid_maruti)
as num_of_days_taken;

#4. Number of days when stock price closed above its previous day closed price

Drop view A2;
create view A2 as (Select symbol, date, close from bharti_airtel
union 
Select symbol, date, close from maruti_suzuki
union
Select symbol, date, close from ultra_cemco);

Select * from A2;

Select symbol, sum(if((close>prev_day_CC),1,0)) as num_of_days_above_prev_close,
dense_rank() over(order by sum(if((close>prev_day_CC),1,0)) desc) as rank_prev_close from 
(select symbol, date, close, lag(close) over(partition by symbol order by date) as prev_day_cc from A2) as xyz group by symbol;

#5. Calculating CAGR( Compound Annual Growth Rate of all 3 stocks)

#A. Bharti Airtel
set @begin_price_bha_artl:=(select close from bharti_airtel where date='2010-01-04');

set @end_price_bha_artl:=(select close from bharti_airtel where date='2021-04-30');

Select @begin_price_bha_artl;

Select @end_price_bha_artl;

set @number_of_years:= (select round(timestampdiff(day,'2010-01-04', '2021-04-30')/365,3));

select round((power((@end_price_bha_artl/@begin_price_bha_artl),(1/@number_of_years) )-1)*100,4) as BHA_ARTL_CAGR;



#A. Maruti Suzuki
set @begin_price_mar_suz:=(select close from maruti_suzuki where date='2010-01-04');

set @end_price_mar_suz:=(select close from maruti_suzuki where date='2021-04-30');

Select @begin_price_mar_suz;

Select @end_price_mar_suz;

set @number_of_years2:= (select round(timestampdiff(day,'2010-01-04', '2021-04-30')/365,3));

select round((power((@end_price_mar_suz/@begin_price_mar_suz),(1/@number_of_years2) )-1)*100,4) as MARUTI_SUZUKI_CAGR;


#C. Ultra Tech Cement
set @begin_price_ult_cem:=(select close from ultra_cemco where date='2010-01-04');

set @end_price_ult_cem:=(select close from ultra_cemco where date='2021-04-30');

Select @begin_price_ult_cem;

Select @end_price_ult_cem;

set @number_of_years3:= (select round(timestampdiff(day,'2010-01-04', '2021-04-30')/365,3));

select round((power((@end_price_ult_cem/@begin_price_ult_cem),(1/@number_of_years3) )-1)*100,4) as ULT_CEM_CAGR;

select round((power((@end_price_bha_artl/@begin_price_bha_artl),(1/@number_of_years) )-1)*100,4) as CAGR_ALL_3
union
select round((power((@end_price_mar_suz/@begin_price_mar_suz),(1/@number_of_years2) )-1)*100,4) 
union
select round((power((@end_price_ult_cem/@begin_price_ult_cem),(1/@number_of_years3) )-1)*100,4);

#6. Finding Max Volume month of all 3 stocks

Select symbol,year(date) as year, month(date) as month, max(volume) as Max_Vol
from bharti_airtel
group by year(date), month(date), symbol
order by max(volume) desc
limit 1;

Select symbol,year(date) as year, month(date) as month, max(volume) as Max_Vol
from ultra_cemco
group by year(date), month(date), symbol
order by max(volume) desc
limit 1;

Select symbol,year(date) as year, month(date) as month, max(volume) as Max_Vol
from maruti_suzuki
group by year(date), month(date), symbol
order by max(volume) desc
limit 1;