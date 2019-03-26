%%%----------------------------------------------------------------------
%%% File    : emongo_app.erl
%%% @author : Jeet Parmar <jeet@glabbr.com>
%%% Purpose : Emongo APIs
%%% Created : 15 Dec 2018 by Jeet Parmar <jeet@glabbr.com>
%%%
%%% Copyright (C) 2002-2019 Glabbr India Pvt. Ltd. All Rights Reserved.
%%%
%%% Licensed under the GNU GPL License, Version 3.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     https://www.gnu.org/licenses/gpl-3.0.en.html
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%%----------------------------------------------------------------------


-module(emongo).

-author('jeet@glabbr.com').

%% API
-export([start/1, add_tenant/2,
  set_write_concern/3, set_write_concern/4,
  set_read_concern/3, set_read_concern/4,
  create_collection/3, create_collection/4,
  list_collections/1, list_collections/2,
  find_all/1, find_all/2, find_all/3,
  find_by_id/2, find_by_id/3,
  find/2, find/3, find/4,
  exists/2, exists/3,
  count/3, count/4, count/5,
  insert/2, insert/3, insert/4,
  update_by_id/3, update_by_id/4, update_by_id/5,
  update/3, update/4, update/5,
  delete_by_id/2, delete_by_id/3, delete_by_id/4,
  delete/2, delete/3, delete/4,
  drop_db/1, drop_db/2,
  drop_collection/2,drop_collection/3,
  rename_collection/3,rename_collection/4,
  run_command/2, run_command/3
]).

-type emongo_code() :: error | ok.

-type emongo_response() :: {emongo_code(), string()}.

-define(DEFAULT_TENANT, default).
-define(DEFAULT_NIF_REPLY_TIMEOUT, 60000 * 5). %% 5 minutes

-define(OPTS_WRITE_CONCERN, #{nodes => is_integer,
  journal => is_integer,
  timeout =>  is_integer,
  majority =>  is_integer,
  tag => is_list,
  acknowledge_level  => is_atom}).

-define(OPTS_READ_CONCERN, #{nodes => is_integer,
  journal => is_integer,
  timeout =>  is_integer,
  majority =>  is_integer,
  tag => is_list,
  acknowledge_level  => is_atom}).

-define(OPTS_CREATE_COLLECTION, #{validator => is_list,
  validation_level => is_atom,
  validation_action =>  is_atom,
  size =>  is_integer,
  storage_engine => is_list,
  capped  => is_boolean,
  no_padding  => is_boolean,
  max=> is_integer,
  collation => is_list}).

-define(OPTS_FIND, #{sort => is_list,
  cursor_type => is_atom,
  skip =>  is_integer,
  batch_size=> is_integer,
  comment => is_list,
  allow_partial_results  => is_boolean,
  show_record_id  => is_boolean,
  min => is_list,
  hint => is_list,
  max_time =>  is_integer,
  max_await_time=> is_integer,
  projection =>  is_list,
  return_key  => is_boolean,
  no_cursor_timeout  => is_boolean,
  limit =>  is_integer,
  max_staleness=> is_integer,
  tags => is_list,
  max=> is_list,
  collation => is_list,
  read_mode => is_atom
}).

-define(OPTS_COUNT, #{
  collation => is_list,
  hint => is_list,
  limit =>  is_integer,
  max_time =>  is_integer,
  skip =>  is_integer,
  read_mode => is_atom,
  tags => is_list,
  max_staleness=> is_integer
}).

-define(OPTS_INSERT, #{
  bypass_document_validation => is_boolean,
  ordered => is_boolean
}).

-define(OPTS_UPDATE, #{
  bypass_document_validation => is_boolean,
  upsert => is_boolean,
  collation => is_list
}).

-define(OPTS_DELETE, #{
  collation => is_list
}).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec start(string()) -> emongo_response().
start(Uri)->
  emongo_app:start([],[{uri, Uri}, {tenant_id,?DEFAULT_TENANT}]).


-spec add_tenant(string(), string()) -> emongo_response().
add_tenant(TenantId, Uri) ->
  emongo_sup:start_child(TenantId, Uri).

-spec set_read_concern(string(), string(), map()) -> emongo_response().
set_read_concern(DbName, Collection, Opts) ->
  set_read_concern(?DEFAULT_TENANT, DbName, Collection, Opts).

-spec set_read_concern(string(), string(), string(), map()) -> emongo_response().
set_read_concern(TenantName, DbName, Collection, Opts) ->
  try
    validate(read_concern, Opts, ?OPTS_READ_CONCERN),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {set_read_concern, DbName, Collection, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    _ : W ->
      W
  end.

-spec set_write_concern(string(), string(), map()) -> emongo_response().
set_write_concern(DbName, Collection, Opts) ->
  set_write_concern(?DEFAULT_TENANT, DbName, Collection, Opts).


-spec set_write_concern(string(), string(), string(), map()) -> emongo_response().
set_write_concern(TenantName, DbName, Collection, Opts) ->
  try
    validate(write_concern, Opts, ?OPTS_READ_CONCERN),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {set_write_concern, DbName, Collection, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    _ : W ->
      W
  end.

-spec find_all(string()) -> emongo_response().
find_all(Collection) ->
  find_all(?DEFAULT_TENANT, Collection).

-spec find_all(string(), string() | map()) -> emongo_response().
find_all(Collection, Opts) when is_map(Opts) ->
  find_all(?DEFAULT_TENANT, Collection, Opts);

find_all(TenantName, Collection) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {find_all, Collection},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

-spec find_all(string(), string(), map()) -> emongo_response().
find_all(TenantName, Collection, Opts) ->
  try
    validate(find, Opts, ?OPTS_FIND),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {find_all, Collection, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw : {error, ~p}~n",[Reason]),
      {error, Reason}
  end.

find_by_id(Collection, Id) ->
  find_by_id(?DEFAULT_TENANT, Collection, Id).

find_by_id(TenantName, Collection, Id) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {find_by_id, Collection, Id},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

find(Collection, Filter) ->
  find(?DEFAULT_TENANT, Collection, Filter).

find(Collection, Filter, Opts) when is_map(Opts) ->
  find(?DEFAULT_TENANT, Collection, Filter, Opts);

find(TenantName, Collection, Filter) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {find, Collection, Filter},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

find(TenantName, Collection, Filter, Opts) ->
  try
    validate(find, Opts, ?OPTS_FIND),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {find, Collection, Filter, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

exists(Collection, Filter) ->
  exists(?DEFAULT_TENANT, Collection, Filter).

exists(TenantName, Collection, Filter) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {exists, Collection, Filter},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

count(DbName, Collection, Filter) ->
  count(?DEFAULT_TENANT, DbName, Collection, Filter).

count(DbName, Collection, Filter, Opts) when is_map(Opts) ->
  count(?DEFAULT_TENANT, DbName, Collection, Filter, Opts);

count(TenantName, DbName, Collection, Filter) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {count, DbName, Collection, Filter},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

count(TenantName, DbName, Collection, Filter, Opts) ->
  try
    validate(count, Opts, ?OPTS_COUNT),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {count, DbName, Collection, Filter, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw : {error, ~p}~n",[Reason]),
      {error, Reason}
  end.

insert(Collection, JsonData) ->
  insert(?DEFAULT_TENANT, Collection, JsonData).

insert(Collection, JsonData, Opts) when is_map(Opts) ->
  insert(?DEFAULT_TENANT, Collection, JsonData, Opts);

insert(TenantName, Collection, JsonData) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {insert, Collection, length(JsonData), JsonData},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

insert(TenantName, Collection, JsonData, Opts) ->
  try
    validate(insert, Opts, maps:merge(?OPTS_INSERT, ?OPTS_WRITE_CONCERN)),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {insert, Collection, length(JsonData), JsonData, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

update_by_id(Collection, Id, JsonData) ->
  update_by_id(?DEFAULT_TENANT, Collection, Id, JsonData).

update_by_id(Collection, Id, JsonData, Opts) when is_map(Opts) ->
  update_by_id(?DEFAULT_TENANT, Collection, Id, JsonData, Opts);

update_by_id(TenantName, Collection, Id, JsonData) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {update_by_id, Collection, Id, length(JsonData), JsonData},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

update_by_id(TenantName, Collection, Id, JsonData, Opts) ->
  try
    validate(update, Opts, maps:merge(?OPTS_UPDATE, ?OPTS_WRITE_CONCERN)),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {update_by_id, Collection, Id, length(JsonData), JsonData, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

update(Collection, Filter, JsonData) ->
  update(?DEFAULT_TENANT, Collection, Filter, JsonData).

update(Collection, Filter, JsonData, Opts) when is_map(Opts) ->
  update(?DEFAULT_TENANT, Collection, Filter, JsonData, Opts);

update(TenantName, Collection, Filter, JsonData) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {update_by_query, Collection, length(Filter), Filter, length(JsonData), JsonData},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

update(TenantName, Collection, Filter, JsonData, Opts) ->
  try
    validate(update, Opts, maps:merge(?OPTS_UPDATE, ?OPTS_WRITE_CONCERN)),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {update_by_query, Collection, length(Filter), Filter, length(JsonData), JsonData, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

delete_by_id(Collection, Id) ->
  delete_by_id(?DEFAULT_TENANT, Collection, Id).

delete_by_id(Collection, Id, Opts) when is_map(Opts) ->
  delete_by_id(?DEFAULT_TENANT, Collection, Id, Opts);

delete_by_id(TenantName, Collection, Id) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {delete_by_id, Collection, Id},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

delete_by_id(TenantName, Collection, Id, Opts) ->
  try
    validate(delete, Opts, maps:merge(?OPTS_DELETE, ?OPTS_WRITE_CONCERN)),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {delete_by_id, Collection, Id, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

delete(Collection, Filter) ->
  delete(?DEFAULT_TENANT, Collection, Filter).

delete(Collection, Filter, Opts) when is_map(Opts) ->
  delete(?DEFAULT_TENANT, Collection, Filter, Opts);

delete(TenantName, Collection, Filter) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {delete_by_query, Collection, length(Filter), Filter},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

delete(TenantName, Collection, Filter, Opts) ->
  try
    validate(delete, Opts, maps:merge(?OPTS_DELETE, ?OPTS_WRITE_CONCERN)),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {delete_by_query, Collection, length(Filter), Filter, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.


-spec list_collections(string()) -> emongo_response().
list_collections(DbName) ->
  list_collections(?DEFAULT_TENANT, DbName).

-spec list_collections(string(), string()) -> emongo_response().
list_collections(TenantName, DbName) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {list_collections, DbName}, ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

-spec create_collection(string(), string(), map()) -> emongo_response().
create_collection(DbName, Collection, Opts) ->
  create_collection(?DEFAULT_TENANT, DbName, Collection, Opts).

-spec create_collection(string(), string(), string(), map()) -> emongo_response().
create_collection(TenantName, DbName, Collection, Opts) ->
  try
    validate(create_collection, Opts, ?OPTS_CREATE_COLLECTION),
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {create_collection, DbName, Collection, Opts},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    throw : {error, Reason}  ->
      io:fwrite("Exception: Reason ==> throw :{error, ~p}~n",[Reason]),
      {error, Reason}
  end.

drop_db(DbName) ->
  drop_db(?DEFAULT_TENANT, DbName).

drop_db(TenantName, DbName) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {drop_db, DbName},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

drop_collection(DbName, Collection) ->
  drop_collection(?DEFAULT_TENANT, DbName, Collection).

drop_collection(TenantName, DbName, Collection) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {drop_collection, DbName, Collection},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

rename_collection(DbName, Collection, NewName) ->
  rename_collection(?DEFAULT_TENANT, DbName, Collection, NewName).

rename_collection(TenantName, DbName, Collection, NewName) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {rename_collection, DbName, Collection, NewName},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

run_command(DbName, Command) ->
  run_command(?DEFAULT_TENANT, DbName, Command).

run_command(TenantName, DbName, Command) ->
  try
    poolboy:transaction(TenantName, fun(Worker) ->
      gen_server:call(Worker, {run_command, DbName, Command},
        ?DEFAULT_NIF_REPLY_TIMEOUT)
                                    end)
  catch
    exit : {noproc, Reason}  ->
      io:fwrite("Exception: Reason ==> exit : {noproc, ~p}~n",[Reason]),
      {noproc, Reason}
  end.

%%% internal
validate(ValidateFor, Opts, ValidateAgainst) when is_map(Opts);
  length(Opts) =< length(ValidateAgainst) ->
  maps:fold(fun(Key, Val, Acc) ->
    KeyType = maps:get(Key, ValidateAgainst, error),
    if (KeyType == error) ->
      throw({error, lists:concat(["Invalid ", atom_to_list(ValidateFor),
        "option ", atom_to_list(Key)])});
      true ->
        IsValidOpt = erlang:KeyType(Val),
        if
          IsValidOpt == false -> throw({error,
            lists:concat(["Invalid", atom_to_list(ValidateFor), " option type ", atom_to_list(Key),
              " should be ", string:prefix(atom_to_list(KeyType), "is_")])});
          true ->
            Acc
        end,
        Acc
    end,
    Acc end, Opts, Opts);

validate(ValidateFor, _Opts, _ValidateAgainst) ->
  throw({error, lists:concat(["Invalid mongo ", atom_to_list(ValidateFor), " options"])}).
