with typed_data as (

 select distinct on (trade_date) run_id, 
   		to_date(trade_date, 'DD.MM.YYYY') as trade_date, 
	   	regexp_replace(keyrate, ',', '.')::numeric(10, 2) as keyrate
   from {{ source('stg', 'keyrate_history') }}
  order by trade_date, run_id desc),

calendar as (

 select generate_series(
			(select min(trade_date) from typed_data), 
			(select max(trade_date) from typed_data), 
			'1 day'::interval)::date as _date),

joined as ( 

 select c._date, 
		td.*
   from calendar c left join typed_data td on c._date = td.trade_date),

filled as (

 select *, count(keyrate) over (order by _date) value_group from joined)

select _date as trade_date,
 	   max(keyrate) over (partition by value_group) as keyrate,
	   max(run_id) over (partition by value_group) as source_run_id
  from filled

