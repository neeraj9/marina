-module(marina).
-include("marina_internal.hrl").

-export([
    async_execute/5,
    async_execute/6,
    async_execute_2/6,
    async_prepare/2,
    async_prepare/3,
    async_query/5,
    async_query/6,
    async_reusable_query/5,
    async_reusable_query/6,
    async_reusable_query_2/6,
    execute/5,
    execute_2/5,
    prepare/2,
    query/5,
    receive_response/1,
    response/1,
    reusable_query/5,
    reusable_query_2/5
]).

%% public
-spec async_execute(statement_id(), [value()], consistency(), [flag()],
    pid()) -> {ok, reference()} | error().

async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid) ->
    async_execute(StatementId, Values, ConsistencyLevel, Flags,
        Pid, ?DEFAULT_TIMEOUT).

-spec async_execute(statement_id(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | error().

async_execute(StatementId, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    async_call({execute, StatementId, Values, ConsistencyLevel, Flags},
        Pid, Timeout).

-spec async_execute_2(statement_id(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | error().

async_execute_2(StatementId, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    async_call_2({execute, StatementId, Values, ConsistencyLevel, Flags},
        Pid, Timeout).

-spec async_prepare(query(), pid()) ->
    {ok, reference()} | error().

async_prepare(Query, Pid) ->
    async_prepare(Query, Pid, ?DEFAULT_TIMEOUT).

-spec async_prepare(query(), pid(), timeout()) ->
    {ok, reference()} | error().

async_prepare(Query, Pid, Timeout) ->
    async_call({prepare, Query}, Pid, Timeout).

-spec async_query(query(), [value()], consistency(), [flag()], pid()) ->
    {ok, reference()} | error().

async_query(Query, Values, ConsistencyLevel, Flags, Pid) ->
    async_query(Query, Values, ConsistencyLevel, Flags,
        Pid, ?DEFAULT_TIMEOUT).

-spec async_query(query(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | error().

async_query(Query, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    async_call({query, Query, Values, ConsistencyLevel, Flags}, Pid, Timeout).

-spec async_reusable_query(query(), [value()], consistency(), [flag()],
    pid()) -> {ok, reference()} | error().

async_reusable_query(Query, Values, ConsistencyLevel, Flags, Pid) ->
    async_reusable_query(Query, Values, ConsistencyLevel, Flags,
        Pid, ?DEFAULT_TIMEOUT).

-spec async_reusable_query(query(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | error().

async_reusable_query(Query, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    case marina_cache:get({marina_1, Query}) of
        {ok, StatementId} ->
            async_execute(StatementId, Values, ConsistencyLevel, Flags,
                Pid, Timeout);
        {error, not_found} ->
            case prepare(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put({marina_1, Query}, StatementId),
                    async_execute(StatementId, Values, ConsistencyLevel, Flags,
                        Pid, Timeout);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec async_reusable_query_2(query(), [value()], consistency(), [flag()],
    pid(), timeout()) -> {ok, reference()} | error().

async_reusable_query_2(Query, Values, ConsistencyLevel, Flags, Pid, Timeout) ->
    case marina_cache:get({marina_2, Query}) of
        {ok, StatementId} ->
            async_execute_2(StatementId, Values, ConsistencyLevel, Flags,
                Pid, Timeout);
        {error, not_found} ->
            case prepare_2(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put({marina_2, Query}, StatementId),
                    async_execute_2(StatementId, Values, ConsistencyLevel,
                        Flags, Pid, Timeout);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec execute(statement_id(), [value()], consistency(), [flag()],
    timeout()) -> {ok, term()} | error().

execute(StatementId, Values, ConsistencyLevel, Flags, Timeout) ->
    call({execute, StatementId, Values, ConsistencyLevel, Flags}, Timeout).

-spec execute_2(statement_id(), [value()], consistency(), [flag()],
    timeout()) -> {ok, term()} | error().

execute_2(StatementId, Values, ConsistencyLevel, Flags, Timeout) ->
    call_2({execute, StatementId, Values, ConsistencyLevel, Flags}, Timeout).

-spec prepare(query(), timeout()) ->
    {ok, term()} | error().

prepare(Query, Timeout) ->
    call({prepare, Query}, Timeout).

-spec prepare_2(query(), timeout()) ->
    {ok, term()} | error().

prepare_2(Query, Timeout) ->
    call_2({prepare, Query}, Timeout).

-spec query(query(), [value()], consistency(), [flag()], timeout()) ->
    {ok, term()} | error().

query(Query, Values, ConsistencyLevel, Flags, Timeout) ->
    call({query, Query, Values, ConsistencyLevel, Flags}, Timeout).

-spec receive_response(term()) ->
    {ok, term()} | error().

receive_response(RequestId) ->
    response(shackle:receive_response(RequestId)).

-spec response({ok, term()} | error()) ->
    {ok, term()} | error().

response({ok, Frame}) ->
    marina_body:decode(Frame);
response({error, Reason}) ->
    {error, Reason}.

-spec reusable_query(query(), [value()], consistency(), [flag()], timeout()) ->
    {ok, term()} | error().

reusable_query(Query, Values, ConsistencyLevel, Flags, Timeout) ->
    Timestamp = os:timestamp(),
    case marina_cache:get({marina_1, Query}) of
        {ok, StatementId} ->
            Execute = execute(StatementId, Values, ConsistencyLevel, Flags,
                Timeout),
            case Execute of
                {error, {9472, _}} ->
                    marina_cache:erase({marina_1, Query}),
                    Timeout3 = marina_utils:timeout(Timeout, Timestamp),
                    reusable_query(Query, Values, ConsistencyLevel, Flags,
                        Timeout3);
                Response ->
                    Response
            end;
        {error, not_found} ->
            case prepare(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put({marina_1, Query}, StatementId),
                    Timeout2 = marina_utils:timeout(Timeout, Timestamp),
                    execute(StatementId, Values, ConsistencyLevel, Flags,
                        Timeout2);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec reusable_query_2(query(), [value()], consistency(), [flag()],
    timeout()) -> {ok, term()} | error().

reusable_query_2(Query, Values, ConsistencyLevel, Flags, Timeout) ->
    Timestamp = os:timestamp(),
    case marina_cache:get({marina_2, Query}) of
        {ok, StatementId} ->
            Execute = execute_2(StatementId, Values, ConsistencyLevel, Flags,
                Timeout),
            case Execute of
                {error, {9472, _}} ->
                    marina_cache:erase({marina_2, Query}),
                    Timeout3 = marina_utils:timeout(Timeout, Timestamp),
                    reusable_query_2(Query, Values, ConsistencyLevel, Flags,
                        Timeout3);
                Response ->
                    Response
            end;
        {error, not_found} ->
            case prepare(Query, Timeout) of
                {ok, StatementId} ->
                    marina_cache:put({marina_2, Query}, StatementId),
                    Timeout2 = marina_utils:timeout(Timeout, Timestamp),
                    execute_2(StatementId, Values, ConsistencyLevel, Flags,
                        Timeout2);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% private
async_call(Msg, Pid, Timeout) ->
    shackle:cast(?APP, Msg, Pid, Timeout).

call(Msg, Timeout) ->
    response(shackle:call(?APP, Msg, Timeout)).

async_call_2(Msg, Pid, Timeout) ->
    shackle:cast(marina_2, Msg, Pid, Timeout).

call_2(Msg, Timeout) ->
    response(shackle:call(marina_2, Msg, Timeout)).
