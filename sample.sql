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


SELECT distinct(rptdt) from hkex_options_contract where rptdt >= '2021-01-03';

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


select dt, 
max(if(bullbear='Bull' and listing_date<dt and mce<>'Y', call_lvl, 0)), 
max(if(bullbear='Bull' and listing_date<dt and mce='Y', call_lvl, null)), 
min(if(bullbear='Bull' and listing_date<dt and mce='Y', call_lvl, null)), 
sum(if(bullbear='Bull' and listing_date<dt and mce='Y', os*h, null)), 

max(if(bullbear='Bull' and listing_date=dt and mce='Y', call_lvl, null)), 
min(if(bullbear='Bull' and listing_date=dt and mce='Y', call_lvl, null)), 
sum(if(bullbear='Bull' and listing_date=dt and mce='Y', os*h, null)), 

min(if(bullbear='Bear' and listing_date<dt, call_lvl, 99999)),
min(if(bullbear='Bear' and listing_date=dt, call_lvl, 99999))

from hkex_cbbc where underly='HSI' and dt='2022-01-26'
group by dt;


select dt, 
max(if(bullbear='Bull' and listing_date<dt , call_lvl, 0)) as H, 
min(if(bullbear='Bear' and listing_date<dt , call_lvl, 99999)) as L
from hkex_cbbc where underly='HSI'  
group by dt;

select dt, 
max(if(bullbear='Bull' and listing_date<dt , call_lvl, 0)) as H, 
min(if(bullbear='Bear' and listing_date<dt , call_lvl, 99999)) as L
from hkex_cbbc where underly='HSI' 
group by dt
having abs(max(if(bullbear='Bull' and listing_date<dt , call_lvl, 0)) - min(if(bullbear='Bear' and listing_date<dt , call_lvl, 99999))) < 250;

select listing_date as dt, 
max(if(bullbear='Bull' and listing_date=dt , call_lvl, 0)) as H, 
min(if(bullbear='Bear' and listing_date=dt , call_lvl, 99999)) as L
from hkex_cbbc where underly='HSI' 
group by listing_date;

select dt,  call_lvl,round(os/(ent_ratio * 50),0) as no_fut_contracts, round(os * c) as money,
	 dense_RANK() OVER (PARTITION BY
                     dt 
				order by 
					call_lvl desc ) lvl_rank
from hkex_cbbc 
where  underly='HSI'  and bullbear = 'Bull' and listing_date < dt and dt = '2022-01-31';

select dt,  call_lvl, round(os/(ent_ratio * 50),0) as no_fut_contracts, round(os * c) as money,
	 DENSE_RANK() OVER (PARTITION BY
                     dt 
				order by 
					call_lvl ) lvl_rank
from hkex_cbbc 
where  underly='HSI'  and bullbear = 'Bear' and listing_date < dt and dt = '2022-01-31';



CREATE TABLE `hkex_cbbc` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cbbc_code` int(11) NOT NULL COMMENT 'CBBC Code',
  `cbbc_name` char(30) NOT NULL COMMENT 'CBBC Name',
  `dt` datetime NOT NULL COMMENT 'Trade Date',
  `bv` float NOT NULL COMMENT 'No. of CBBC Bought',
  `bavg` float NOT NULL COMMENT 'Average Price per CBBC Bought',
  `sv` float NOT NULL COMMENT 'No. of CBBC Sold',
  `savg` float NOT NULL COMMENT 'Average Price per CBBC Sold',
  `os` float NOT NULL COMMENT 'No. of CBBC still out in market',
  `pctos` float NOT NULL COMMENT '% of issue still out in market',
  `ttl_iss_size` float NOT NULL COMMENT 'Total Issue Size',
  `ccy` char(5) NOT NULL COMMENT 'Trading Currency',
  `h` float NOT NULL COMMENT 'Day High',
  `l` float NOT NULL COMMENT 'Day Low',
  `c` float NOT NULL COMMENT 'Closing Price',
  `v` float NOT NULL COMMENT 'Volume',
  `turnover` float NOT NULL COMMENT 'turnover',
  `issuer` char(3) NOT NULL COMMENT 'Issuer',
  `underly` char(5) NOT NULL COMMENT 'Underlying',
  `bullbear` char(5) NOT NULL COMMENT 'Bull/Bear',
  `type` char(10) NOT NULL COMMENT 'CBBC Type',
  `category` char(5) NOT NULL COMMENT 'CBBC Category',
  `listing_date` datetime NOT NULL COMMENT 'Trade Date',
  `last_trading_date` datetime DEFAULT NULL COMMENT 'Last Trading Date',
  `maturity_date` datetime NOT NULL COMMENT 'Maturity Date',
  `mce` char(1) NOT NULL COMMENT 'MCE',
  `strike_ccy` char(5) NOT NULL COMMENT 'Strike/Call Currency',
  `strike_lvl` float NOT NULL COMMENT 'Strike Level',
  `call_lvl` float NOT NULL COMMENT 'Call Level',
  `ent_ratio` float NOT NULL COMMENT 'Ent. Ratio',
  `delisting_date` datetime DEFAULT NULL COMMENT 'Delisting Date',
  `create_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `mod_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `reportdate` (`dt`,`cbbc_code`)
) ENGINE=MyISAM AUTO_INCREMENT=4978059 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from hkex_cbbc where call_lvl = 24500 and dt = '2022-01-31';
select * from hkex_cbbc where cbbc_code = 54573 and call_lvl = 24500 order by dt;


select * from hkex_cbbc where underly='HSI' and listing_date < dt  and dt =  '2021-11-09' and bullbear ='Bull';
select * from hkex_cbbc where underly='HSI' and listing_date < dt  and dt =  '2021-11-09' and bullbear ='Bear';
select * from hkex_cbbc where cbbc_code = 68341;

select distinct listing_date from hkex_cbbc;

select  * from hkex_cbbc where underly = 'HSI' and dt = '2022-01-31' and bullbear = 'Bull';

