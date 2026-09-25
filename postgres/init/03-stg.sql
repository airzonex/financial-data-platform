create table if not exists stg.rgbi_history(
	run_id bigint not null,
	trade_date text,
	close text,
	currency_id text
);

create table if not exists stg.keyrate_history(
	run_id bigint not null,
	trade_date text,
	keyrate text
);
