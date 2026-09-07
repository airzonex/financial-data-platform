with source_data as (

 select trade_date::date as trade_date,
	    close::numeric(10, 2) as rgbi_close,
		currency_id,
		run_id
   from (select distinct on (trade_date)
				trade_date,
				close,
				currency_id,
				run_id
		   from {{ source('stg', 'rgbi_history') }}
		  order by trade_date desc, run_id desc)
),
 
 calendar as (
 
 select generate_series((select min(trade_date) from source_data),
 						(select max(trade_date) from source_data),
						interval '1 day')::date as trade_date
),
 
 joined as (
 
 select c.trade_date, 
		s.rgbi_close, 
		s.currency_id, 
		s.run_id
   from calendar c left join source_data s on c.trade_date = s.trade_date
),

 filled as (

 select trade_date, 
		rgbi_close, 
		currency_id, 
		run_id,
		count(rgbi_close) over (order by trade_date) value_group 
   from joined
)
 
 select trade_date,
 		max(rgbi_close) over (partition by value_group) as rgbi_close,
		max(currency_id) over (partition by value_group) as currency_id,
		max(run_id) over (partition by value_group) as source_run_id
   from filled

