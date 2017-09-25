-module(marina_sup).
-include("marina_internal.hrl").

-export([
    start_link/0
]).

-behaviour(supervisor).
-export([
    init/1
]).

%% public
-spec start_link() -> {ok, pid()}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor callbacks
-spec init([]) -> {ok, {{one_for_one, 5, 10}, []}}.

init([]) ->
    BacklogSize = ?GET_ENV(backlog_size, ?DEFAULT_BACKLOG_SIZE),
    Ip = ?GET_ENV(ip, ?DEFAULT_IP),
    Ip2 = ?GET_ENV(ip2, ?DEFAULT_IP),
    PoolSize = ?GET_ENV(pool_size, ?DEFAULT_POOL_SIZE),
    PoolStrategy = ?GET_ENV(pool_strategy, ?DEFAULT_POOL_STRATEGY),
    Port = ?GET_ENV(port, ?DEFAULT_PORT),
    Port2 = ?GET_ENV(port2, ?DEFAULT_PORT),
    Reconnect = ?GET_ENV(reconnect, ?DEFAULT_RECONNECT),
    ReconnectTimeMax = ?GET_ENV(reconnect_time_max,
        ?DEFAULT_RECONNECT_MAX),
    ReconnectTimeMin = ?GET_ENV(reconnect_time_min,
        ?DEFAULT_RECONNECT_MIN),
    SocketOptions = ?GET_ENV(socket_options, ?DEFAULT_SOCKET_OPTIONS),

    ok = shackle_pool:start(marina_1, ?CLIENT, [
        {ip, Ip},
        {port, Port},
        {reconnect, Reconnect},
        {reconnect_time_max, ReconnectTimeMax},
        {reconnect_time_min, ReconnectTimeMin},
        {socket_options, SocketOptions}
    ], [
        {backlog_size, BacklogSize},
        {pool_size, PoolSize},
        {pool_strategy, PoolStrategy}
    ]),

    case ?GET_ENV(ip2, undefined) of
        undefined ->
            ok;
        Ip2 ->
            ok = shackle_pool:start(marina_2, ?CLIENT, [
                {ip, Ip2},
                {port, Port2},
                {reconnect, Reconnect},
                {reconnect_time_max, ReconnectTimeMax},
                {reconnect_time_min, ReconnectTimeMin},
                {socket_options, SocketOptions}
            ], [
                {backlog_size, BacklogSize},
                {pool_size, PoolSize},
                {pool_strategy, PoolStrategy}
            ])
    end,

    marina_cache:init(),

    {ok, {{one_for_one, 5, 10}, []}}.
