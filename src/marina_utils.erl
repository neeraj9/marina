-module(marina_utils).
-include("marina_internal.hrl").

-export([
    connect/2,
    ip_to_bin/1,
    pack/1,
    string_to_uuid/1,
    sync_msg/2,
    timeout/2,
    unpack/1,
    uuid_to_string/1
]).

%% public
-spec connect(inet:socket_address() | inet:hostname(), inet:port_number()) ->
    {ok, inet:socket()} | {error, atom()}.

connect(Ip, Port) ->
    SocketOpts = ?DEFAULT_SOCKET_OPTIONS ++ [{active, false}],
    case gen_tcp:connect(Ip, Port, SocketOpts) of
        {ok, Socket} ->
            {ok, Socket};
        {error, Reason} ->
            error_logger:error_msg("failed to connect: ~p~n", [Reason]),
            {error, Reason}
    end.

-spec ip_to_bin(string()) ->
    binary().

ip_to_bin(Ip) ->
    {ok, {A, B, C, D}} = inet:parse_ipv4_address(Ip),
    <<A/integer, B/integer, C/integer, D/integer>>.

-spec pack(binary() | iolist()) ->
    {ok, binary()} | {error, term()}.

pack(Iolist) when is_list(Iolist) ->
    pack(iolist_to_binary(Iolist));
pack(Binary) ->
    case lz4:compress(Binary, []) of
        {ok, Compressed} ->
            {ok, <<(size(Binary)):32/unsigned-integer, Compressed/binary>>};
        {error, Reason} ->
            {error, Reason}
    end.

string_to_uuid([N01, N02, N03, N04, N05, N06, N07, N08, $-, N09, N10, N11, N12,
    $-, N13, N14, N15, N16, $-, N17, N18, N19, N20, $-, N21, N22, N23, N24, N25,
    N26, N27, N28, N29, N30, N31, N32]) ->

    B01 = hex_to_int(N01, N02),
    B02 = hex_to_int(N03, N04),
    B03 = hex_to_int(N05, N06),
    B04 = hex_to_int(N07, N08),
    B05 = hex_to_int(N09, N10),
    B06 = hex_to_int(N11, N12),
    B07 = hex_to_int(N13, N14),
    B08 = hex_to_int(N15, N16),
    B09 = hex_to_int(N17, N18),
    B10 = hex_to_int(N19, N20),
    B11 = hex_to_int(N21, N22),
    B12 = hex_to_int(N23, N24),
    B13 = hex_to_int(N25, N26),
    B14 = hex_to_int(N27, N28),
    B15 = hex_to_int(N29, N30),
    B16 = hex_to_int(N31, N32),

    <<B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B11, B12,
        B13, B14, B15, B16>>.

hex_to_int(C1, C2) ->
    hex_to_int(C1) * 16 + hex_to_int(C2).

hex_to_int(C) when $0 =< C, C =< $9 ->
    C - $0;
hex_to_int(C) when $A =< C, C =< $F ->
    C - $A + 10;
hex_to_int(C) when $a =< C, C =< $f ->
    C - $a + 10.

-spec sync_msg(inet:socket(), iodata()) ->
    {ok, term()} | {error, term()}.

sync_msg(Socket, Msg) ->
    case gen_tcp:send(Socket, Msg) of
        ok ->
            rcv_buf(Socket, <<>>);
        {error, Reason} ->
            {error, Reason}
    end.

-spec unpack(binary()) ->
    {ok, binary()} | {error, term()}.

unpack(<<Size:32/unsigned-integer, Binary/binary>>) ->
    lz4:uncompress(Binary, Size).

-spec timeout(pos_integer(), erlang:timestamp()) ->
    integer().

timeout(Timeout, Timestamp) ->
    Diff = timer:now_diff(os:timestamp(), Timestamp) div 1000,
    Timeout - Diff.

-spec uuid_to_string(<<_:128>>) ->
    list().

uuid_to_string(<<Value:128>>) ->
    [N01, N02, N03, N04, N05, N06, N07, N08, N09, N10, N11, N12, N13, N14, N15,
     N16, N17, N18, N19, N20, N21, N22, N23, N24, N25, N26, N27, N28, N29, N30,
     N31, N32] = int_to_hex_list(Value, 32),

    [N01, N02, N03, N04, N05, N06, N07, N08, $-, N09, N10, N11, N12, $-, N13,
     N14, N15, N16, $-, N17, N18, N19, N20, $-, N21, N22, N23, N24, N25, N26,
     N27, N28, N29, N30, N31, N32].

%% private
int_to_hex(I) when 0 =< I, I =< 9 ->
    I + $0;
int_to_hex(I) when 10 =< I, I =< 15 ->
    (I - 10) + $a.

int_to_hex_list(I, N) when is_integer(I), I >= 0 ->
    int_to_hex_list([], I, 1, N).

int_to_hex_list(L, I, Count, N) when I < 16 ->
    int_to_hex_list_pad([int_to_hex(I) | L], N - Count);
int_to_hex_list(L, I, Count, N) ->
    int_to_hex_list([int_to_hex(I rem 16) | L], I div 16, Count + 1, N).

int_to_hex_list_pad(L, 0) ->
    L;
int_to_hex_list_pad(L, Count) ->
    int_to_hex_list_pad([$0 | L], Count - 1).

rcv_buf(Socket, Buffer) ->
    case gen_tcp:recv(Socket, 0, ?DEFAULT_RECV_TIMEOUT) of
        {ok, Msg} ->
            Buffer2 = <<Buffer/binary, Msg/binary>>,
            case marina_frame:decode(Buffer2) of
                {_Rest, []} ->
                    rcv_buf(Socket, Buffer2);
                {_Rest, [Frame | _]} ->
                    marina_body:decode(Frame)
            end;
        {error, Reason} ->
            {error, Reason}
    end.
