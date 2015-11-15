-module('sqeache_client_sup').

% Supervisor
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    PoolerSup = {pooler_sup, {pooler_sup, start_link, []}, permanent, infinity, supervisor, [pooler_sup]},
    {ok, {{one_for_one, 5, 10}, [PoolerSup]}}.

