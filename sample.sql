SELECT count(*), max(rptdt) FROM world.hkex_futures_contract;	
select * from hkex_futures_contract where rptdt = '2019-10-31';    

-- drop table trades202008;
select count(*) from trades;

select count(*) from trades202008;
select * from trades202008
where underly='HSI' and strike = 0
and month(contractmonth) = month(dt)
and date(dt) = '2020-08-06'
order by dt;
 show global variables like '%timeout';
create table trades202008 as
SELECT 
	
    FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(dt))/60+1)*60) as dt,   -- not sure about that
    contractmonth,
    underly,
    strike,
    cp,
    p,
    count(*) as tradesnum,
    sum(v*p) as turnover,
    sum(v) as vol
FROM trades
-- WHERE 
-- underly='HSI' 
-- and date(dt)='2020-08-06' 
-- and month(dt) = month(contractmonth)
-- and  strike=0
GROUP BY
FROM_UNIXTIME(floor((UNIX_TIMESTAMP(dt))/60+1)*60),
contractmonth,
underly,
strike,
cp,
p;
show variables like "%_buffer%";
SELECT dt from trades where underly='HSI' and date(dt)='2020-08-06' and contractmonth='2020-08-01' ;
and FROM_UNIXTIME(floor((UNIX_TIMESTAMP(dt))/60 + 1)*60) = '2020-08-06 16:00:00';



select dt, UNIX_TIMESTAMP(dt) from  trades;
WHERE underly='HSI' and date(dt)='2020-08-06' amd hour(dt)=15 and minute(dt) = 19 and contractmonth='2020-08-01'
2020-08-06 15:19:01


select * from trades;
-- truncate table trades;

SELECT A.contractmonth, rptdt as ld2 from hkex_futures_contract A, 
(SELECT contractmonth, max(rptdt) as ld FROM world.hkex_futures_contract group by contractmonth) B
where A.contractmonth=B.contractmonth and A.rptdt < B.ld 
and month(A.rptdt) = month(B.contractmonth);

SELECT contractmonth, max(rptdt) as ld FROM world.hkex_futures_contract group by contractmonth
having day(ld) > 20;

select * from hkex_futures_contract  where year(rptdt)=year(contractmonth) and month(rptdt)=month(contractmonth); 




SELECT rptdt, count(*), min(contractmonth), max(contractmonth), min(strike),  max(strike)  FROM world.hkex_options_contract group by  rptdt;
select * from hkex_options_contract where rptdt = '2021-05-03';

SELECT count(*), max(rptdt) FROM world.hkex_futures_contract;
SELECT rptdt, count(*), min(contractmonth), max(contractmonth) FROM world.hkex_futures_contract group by  rptdt;


SELECT *FROM world.hkex_options_contract where rptdt = '2019-06-13' order by rptdt, contractmonth, cp, strike;
describe hkex_options_contract;

select group_concat(column_name order by ordinal_position)
from information_schema.columns
where table_schema = 'world' and table_name = 'hkex_options_contract';
select group_concat(column_name order by ordinal_position)
from information_schema.columns
where table_schema = 'world' and table_name = 'trades';
