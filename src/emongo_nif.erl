-module(emongo_nif).
-export([init/0, connect/2, connect/1,
  find_all/2,
  find_all/4,
  find_by_id/3,
  find/3,
  find/5,
  exists/3,
  count/4,
  insert/4,
  update_by_id/5,
  update_by_query/6,
  delete_by_id/3,
  delete_by_query/4,
  list_databases/1,
  list_collections/2,
  collection_exists/3]).


init() ->
  case code:which(emongo) of
    Filename when is_list(Filename) ->
      erlang:load_nif(filename:join([filename:dirname(Filename),
        "..", "priv",
        "emongo"]), 0);
    Reason when is_atom(Reason) ->
      {error, Reason}
  end.


-spec connect(string()) -> {ok, binary()}.
connect(Uri) ->
  connect("default", Uri).

-spec connect(string(), string()) -> {ok, binary()}.
connect(_ConnectionId, _Uri) ->
  erlang:nif_error(nif_library_not_loaded).


-spec find_all(string(), string()) -> {ok, binary()}.
find_all(_Conn, _Collection) ->
  erlang:nif_error(nif_library_not_loaded).

-spec find_all(string(), string(), string(), string()) -> {ok, binary()}.
find_all(_Conn, _Collection, _SortKey, _Order) ->
  erlang:nif_error(nif_library_not_loaded).

-spec find_by_id(string(), string(), string()) -> {ok, binary()}.
find_by_id(_Conn, _Collection, _Filter) ->
  erlang:nif_error(nif_library_not_loaded).

-spec find(string(), string(), string()) -> {ok, binary()}.
find(_Conn, _Collection, _Filter) ->
  erlang:nif_error(nif_library_not_loaded).

-spec find(string(), string(), string(), string(), string()) -> {ok, binary()}.
find(_Conn, _Collection, _Filter, _SortKey, _Order) ->
  erlang:nif_error(nif_library_not_loaded).

-spec exists(string(), string(), string()) -> {ok, binary()}.
exists(_Conn, _Collection, _Query) ->
  erlang:nif_error(nif_library_not_loaded).

-spec count(string(), string(), string(), string()) -> {ok, binary()}.
count(_Conn, _DbName, _Collection, _Query) ->
  erlang:nif_error(nif_library_not_loaded).

-spec insert(string(), string(), integer(), string()) -> {ok, binary()}.
insert(_Conn, _Collection, _LenOfJsonData, _JsonData) ->
  erlang:nif_error(nif_library_not_loaded).

-spec update_by_id(string(), string(), string(), integer(), string()) -> {ok, binary()}.
update_by_id(_Conn, _Collection, _Id, _LenOfJsonData, _JsonData) ->
  erlang:nif_error(nif_library_not_loaded).

-spec update_by_query(string(), string(), integer(), string(), integer(), string()) -> {ok, binary()}.
update_by_query(_Conn, _Collection, _LenOfJsonQuery, _JsonQuery, _LenOfJsonData, _JsonData) ->
  erlang:nif_error(nif_library_not_loaded).

-spec delete_by_id(string(), string(), string()) -> {ok, binary()}.
delete_by_id(_Conn, _Collection, _Id) ->
  erlang:nif_error(nif_library_not_loaded).

-spec delete_by_query(string(), string(), integer(), string()) -> {ok, binary()}.
delete_by_query(_Conn, _Collection, _LenOfJsonQuery, _JsonQuery) ->
  erlang:nif_error(nif_library_not_loaded).

-spec list_databases(string()) -> {ok, binary()}.
list_databases(_Conn) ->
  erlang:nif_error(nif_library_not_loaded).

-spec list_collections(string(), string()) -> {ok, binary()}.
list_collections(_Conn, _DbName) ->
  erlang:nif_error(nif_library_not_loaded).

-spec collection_exists(string(), string(), string()) -> {ok, binary()}.
collection_exists(_Conn, _DbName, _Collection) ->
  erlang:nif_error(nif_library_not_loaded).