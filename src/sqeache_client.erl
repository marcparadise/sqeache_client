-module(sqeache_client).
-behaviour(gen_server).

% The timeout for opening a socket and sending data
% is kept very low so we can quickly fail over if we
% don't get a good response.
-define(COMM_TIMEOUT, 250).
-define(RECV_TIMEOUT, 10000).
-define(SERVER, ?MODULE).

-record(state, { reserved = 0, socket = undefined }).

% API
-export([statement/2, statement/3, statement/4, statement/5,
         select/2, select/3, select/4, select/5,
         execute/2, execute/3,
         start_link/0,
         ping/1
        ]).

% gen_server

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).


ping(Who) ->
    gen_server:call(Who, ping).

select(DbId, Statement) ->
    prep_and_send(DbId, select, Statement, [], identity, []).

select(DbId, Statement, Args) ->
    prep_and_send(DbId, select, Statement, Args, identity, []).

% TODO - why isn't this auto-resolved like it is in a direct sqerl call?
select(DbId, Statement, Args, {XFormName, XFormArgs}) ->
    prep_and_send(DbId, select, Statement, Args, XFormName, XFormArgs);
select(DbId, Statement, Args, XFormName) ->
    prep_and_send(DbId, select, Statement, Args, XFormName, []).

select(DbId, Statement, Args, XFormName, XFormArgs) ->
    prep_and_send(DbId, select, Statement, Args, XFormName, XFormArgs).

statement(DbId, Statement) ->
    prep_and_send(DbId, statement, Statement, [], identity, []).

statement(DbId, Statement, Args) ->
    prep_and_send(DbId, statement, Statement, Args, identity, []).

statement(DbId, Statement, Args, XFormName) ->
    prep_and_send(DbId, statement, Statement, Args, XFormName, []).

statement(DbId, Statement, Args, XFormName, XFormArgs) ->
    prep_and_send(DbId, statement, Statement, Args, XFormName, XFormArgs).

execute(DbId, Statement) ->
    prep_and_send(DbId, execute, Statement, [], none, none).

execute(DbId, Statement, Args) ->
    prep_and_send(DbId, execute, Statement, Args, none, none).

%% Internal
prep_and_send(DbId, Type, Statement, Args, XFormName, XFormArgs) ->
    Bin = to_bin(DbId, Type, Statement, Args, XFormName, XFormArgs),
    case pooler:take_member(sqp) of
        error_no_members ->
            error_logger:error_report("No members available in sqp pool."),
            {error, {pool, sqp, pool_overload_no_workers}};
        Member when is_pid(Member) ->
            {State, Result} = parse_response(gen_server:call(Member, {send, Bin}, infinity)),
            pooler:return_member(sqp, Member, State),
            Result

    end.
parse_response({error, {tcp, _, _} = Response}) ->
    error_logger:error_report(Response),
   {fail, Response};
parse_response(Other) ->
    % While there may be other errors, they will originate from
    % the remote server and don't affect the validity of our pool
    {ok, Other}.



to_bin(DbId, execute, Statement, Args, _, _) ->
    term_to_binary({DbId, execute, Statement, Args});
to_bin(DbId, Type, Statement, Args, XFormName, XFormArgs) ->
    term_to_binary({DbId, Type, Statement, Args, XFormName, XFormArgs}).

% gen-server

init([]) ->
    % { Addr, Port }  = escon:get_with_watch(data_service_host),
    Addr = envy_parse:host_to_ip(sqeache_client, sqeache_vip, "127.0.0.1"),
    Port = envy:get(sqeache_client, sqeache_port, 6543, integer),
    {ok, Socket} = gen_tcp:connect(Addr, Port, [binary, {active,false},
                                                {packet, raw},
                                                {keepalive, true},
                                                {send_timeout, ?COMM_TIMEOUT}],
                                   ?COMM_TIMEOUT),
    {ok, #state{socket = Socket}}.
handle_call({send, Bin}, _From, #state{socket = Socket} = State) ->
    send_it(Socket, Bin, State);
handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Sock}) ->
    gen_tcp:close(Sock),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_it(Sock, Bin, State) ->
    Len = byte_size(Bin),
    Message = <<Len:32/integer,Bin/binary>>,
    case gen_tcp:send(Sock, Message) of
        {error, closed} ->
            {stop, error, {error, {tcp, send, socket_closed}}, State};
        {error, Other} ->
            {stop, error, {error, {tcp, send, Other}}, State};

        _ ->
            recv(Sock, State)
    end.

recv(Sock, State) ->
   case gen_tcp:recv(Sock, 0) of
       {ok, <<Length:32/integer,Data/binary>>} ->
           recv(Sock, Length - byte_size(Data), Data, State);
       {error, Reason} ->
           {stop, error, {error, {tcp, recv_header, Reason}}, State}
   end.

recv(_Sock, 0, Acc, State) ->
    {reply, binary_to_term(Acc), State};
recv(Sock, Length, Acc, State) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            recv(Sock, Length - byte_size(Data), <<Acc/binary,Data/binary>>, State);
        {error, Reason} ->
           {stop, error, {error, {tcp, recv, Reason}}, State}
    end.

