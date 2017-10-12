CREATE TABLE transactions_2016 (
	parcelid bigint, 
	logerror double precision,
	transactiondate varchar
);

COPY transactions_2016
FROM '/Users/fangjie/Downloads/train_2016_v2.csv' DELIMITER ',' CSV HEADER;


/*
1. for each transaction (both in 2016 and 2017)
	1. month of year
	2. week of month
	3. day of week  WEEK
	
	4. average logerror in the same day of week, but only prior to today
	
	
	5. average logerror in last month, if applicable (windowing function)
	6. standard deviation of log error in last month, if applicable
	
	7. average logerror in last 2 months, if applicable 
	8. standard deviation of log error in last 2 months, if applicable
	
	9. average logerror in last 3 months, if applicable 
	10. standard deviation of log error in last 3 months, if applicable
	
	11. average logerror in last 6 months, if applicable 
	12. standard deviation of log error in last 6 months, if applicable

Use this syntax:
create table new_table as

select 
	* 
from  a
join b
\CREATE TABLE new_table_name AS
    SELECT column1, column2,...
    FROM existing_table_name
    WHERE ....;

*/

CREATE TABLE newtable AS
SELECT 
    t.parcelid as id,
    t.logerror as err,
    t.transactiondate as ndate,
    date_part('year', date (t.transactiondate)) AS Year,
    DATE_PART('month',date (t.transactiondate)) AS Month,
    DATE_PART('dow',date (t.transactiondate)) AS DayWeek
FROM transactions_2016 as t

/* extract year, month, date*/
SELECT 
    nt.date,nt.err,
	DATE_PART('year',timestamp nt.date) AS Year,
    DATE_PART('month',timestamp nt.date) AS Month,
    DATE_PART('dow',timestamp nt.date) AS DayWeek
FROM newtable as nt;
        
        
SELECT 
    nt.date,nt.err,
	EXTRACT(YEAR from nt.date) AS Year,
    EXTRACT(MONTH from nt.date) AS Month,
    EXTRACT(DOW from nt.date) as date) AS DayWeek
FROM newtable as nt;      
        


/* average log error */
SELECT 
   	   AVG(nt.err) OVER (PARTITION BY nt.DayWeek ORDER BY nt.ndate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS avg_logerr_dweek,
       AVG(nt.err)
            OVER(ORDER BY nt.month ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS avg_logerr_1month,
       stddev_samp(nt.err)
            OVER(ORDER BY nt.month ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS STD_logerr_1month,     
       AVG(nt.err)
            OVER(ORDER BY nt.month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_logerr_2month,
       stddev_samp(nt.err)
       OVER(ORDER BY nt.month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS STD_logerr_2month,   
       AVG(nt.err)
            OVER(ORDER BY nt.month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_logerr_quarter,
       stddev_samp(nt.err)
       OVER(ORDER BY nt.month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS STD_logerr_quarter, 
        AVG(nt.err)
            OVER(ORDER BY nt.month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_logerr_semiannual,
       stddev_samp(nt.err)
       OVER(ORDER BY nt.month ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS STD_logerr_semiannual
FROM newtable nt


