select r.trade_date, 
	   r.rgbi_close, 
	   k.keyrate
 from {{ ref('rgbi_history') }} r
 join {{ ref('keyrate_history') }} k on r.trade_date = k.trade_date
