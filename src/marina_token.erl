-module(marina_token).
-include("marina_internal.hrl").

% -include_lib("eunit/include/eunit.hrl").

-export([
    m3p/1
]).

-define(INT64_MAX, 9223372036854775807).
-define(INT64_MIN, -9223372036854775808).
-define(INT64_OVF_DIV, 18446744073709551616).
-define(INT64_OVF_OFFSET, 9223372036854775808).

%% public
-spec m3p(binary()) ->
    integer().

m3p(Key) ->
    <<Hash64:8/binary, _/binary>> = murmerl:murmur3_128(Key, 0, x64),
    trunc_int64(binary:decode_unsigned(Hash64, little)).

%% private
trunc_int64(Int) when Int =< ?INT64_MIN; Int >= ?INT64_MAX ->
    (Int + ?INT64_OVF_OFFSET) rem ?INT64_OVF_DIV - ?INT64_OVF_OFFSET;
trunc_int64(Int) ->
    Int.

%% tests
% token_test() ->
%     assert_token(-9223372035861886709, <<9,131,77,226,102,103,17,231,140,250,183,95,66,0,24,117>>),
%     assert_token(-9223369077907327739, <<160,39,52,130,163,205,17,230,164,217,118,211,94,3,67,199>>),
%     assert_token(-5980844116634242428, <<233,31,36,72,86,15,17,231,191,184,237,33,29,0,18,21>>),
%     assert_token(2867464136704746769, <<166,28,198,70,141,130,17,231,158,157,233,18,139,1,113,126>>).
%
% assert_token(Expected, Key) ->
%     ?assertEqual(Expected, token(Key)).
