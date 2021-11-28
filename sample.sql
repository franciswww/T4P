SELECT count(*), max(rptdt) FROM world.hkex_futures_contract;	
select * from hkex_futures_contract where contractmonth = '2021-11-01';    

select* from trades202008
                                        where underly='HSI' and strike = 0
                                    and month(contractmonth) = month(dt)
                                    and date(dt) = '2021-06-18'
                                    order by dt;
                                    
select * from hkex_options_contract where rptdt = '2021-11-26' and contractmonth = '2021-11-01' and cp='C'; 

select rptdt, no, nh, nl, nc, nv,  '|', o, h, l, v from hkex_futures_contract where rptdt >='2021-05-21'  and contractmonth = '2021-06-01'; 

create table tmp  select distinct a.contractmonth, a.rptdt, a.strike from optionsrawview a;
select a.contractmonth, a.rptdt, a.strike, b.coi, b.coic, b.civ, b.cc, c.poi, c.coic, c.piv, c.pc, d.c as hsifc  
from tmp a
left join hkex_futures_contract d on (a.contractmonth=d.contractmonth and a.rptdt=d.rptdt)
left join optionsrawview b on (a.contractmonth=b.contractmonth and a.rptdt=b.rptdt and a.strike = b.strike and b.cp ='C')
left join optionsrawview c on (a.contractmonth=c.contractmonth and a.rptdt=c.rptdt and a.strike = c.strike and c.cp ='P')
;
select * from hkex_futures_contract;


create view optionsrawview as
select  
contractmonth, rptdt, strike, cp,
case when cp='C' then ifnull(sum(oi),0) else 0 end as coi,
case when cp='C' then ifnull(sum(oic),0) else 0 end as coic,
case when cp='C' then ifnull(sum(iv),0) else 0 end as civ,
case when cp='C' then ifnull(sum(c),0) else 0 end as cc,
case when cp='P' then ifnull(sum(oi),0) else 0 end as poi,
case when cp='P' then ifnull(sum(oic),0) else 0 end as poic,
case when cp='P' then ifnull(sum(iv),0) else 0 end as piv,
case when cp='P' then ifnull(sum(c),0) else 0 end as pc
from hkex_options_contract
where 
underly='HSI' and
datediff(contractmonth , rptdt) < 4
group by contractmonth, rptdt,strike,cp
;

create view optionsrawview as
select  
contractmonth, rptdt, strike, cp,
ifnull(sum(oi),0) as oi,
ifnull(sum(oic),0) as oic,
ifnull(sum(iv),0) as iv,
ifnull(sum(c),0) as c
from hkex_options_contract
where 
underly='HSI' and
datediff(contractmonth , rptdt) < 4
group by contractmonth, rptdt,strike,cp
;

select  
strike, concat(CAST(rptdt AS CHAR),cp) as dcp, concat(cast(sum(oi) as CHAR), ' ', cast(sum(oic) as CHAR) , char(13), cast(sum(iv) as CHAR), ' ',cast(sum(c) as CHAR)  ) as sample  from hkex_options_contract
where 
contractmonth = '2021-04-01' and
strike>=27600 and strike<31000 and
underly='HSI' and
datediff(contractmonth , rptdt) < 8
group by strike, concat(CAST(rptdt AS CHAR),cp)
;


select
strike, 
case when rptdt = '2021-04-01' and cp = 'C' then
concat(CAST(rptdt AS CHAR),cp) as dcp, concat(cast(sum(oi) as CHAR), ' ', cast(sum(oic) as CHAR) , char(13), cast(sum(iv) as CHAR), ' ',cast(sum(c) as CHAR)) else '' end as sample
from hkex_options_contract
where 
contractmonth = '2021-04-01' and
strike>=27600 and strike<31000 and
underly='HSI' and
datediff(contractmonth , rptdt) < 8
group by strike
;



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
