console:
	rebar3 compile
	erl +W w -boot start_sasl -kernel error_logger '{file, "error.log"}' -pa _build/default/lib/pooler/ebin -pa _build/default/lib/envy/ebin -pa _build/default/lib/sqeache_client/ebin -config config/test.config -eval "application:ensure_all_started(sqeache_client)"

