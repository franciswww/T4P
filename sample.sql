SELECT count(*), max(rptdt) FROM world.hkex_futures_contract;	
select * from hkex_futures_contract where rptdt = '2021-05-03';    


SELECT rptdt, count(*), min(contractmonth), max(contractmonth), min(strike),  max(strike)  FROM world.hkex_options_contract group by  rptdt;
select * from hkex_options_contract where rptdt = '2021-05-03';

SELECT count(*), max(rptdt) FROM world.hkex_futures_contract;
SELECT rptdt, count(*), min(contractmonth), max(contractmonth) FROM world.hkex_futures_contract group by  rptdt;


SELECT *FROM world.hkex_options_contract where rptdt = '2019-06-13' order by rptdt, contractmonth, cp, strike;
describe hkex_options_contract;

select group_concat(column_name order by ordinal_position)
from information_schema.columns
where table_schema = 'world' and table_name = 'hkex_options_contract'
