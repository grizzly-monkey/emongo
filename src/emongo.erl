-module(emongo).

-behaviour(application).
-behaviour(supervisor).

%% Application callbacks
-export([start/1, start/2, stop/0, stop/1, init/1]).

%% API
-export([find_all/2, execQuery/2, execQueryWithArgs/3, execCallProc/3, exec/3]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(Args) ->
  application:start(?MODULE),
  start(normal, Args).

stop() ->
  stop([]).

start(_StartType, _Args) ->
  emongo_nif:init(),
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
  application:stop(?MODULE),
  ok.


init([]) ->
  {ok, Pools} = application:get_env(emongo, pools),
  PoolSpecs = lists:map(fun({Name, SizeArgs, WorkerArgs}) ->
    PoolArgs = [{name, {local, Name}},
      {worker_module, emongo_worker}] ++ SizeArgs,
    poolboy:child_spec(Name, PoolArgs, WorkerArgs)
                        end, Pools),
  {ok, {{one_for_all, 2, 2}, PoolSpecs}}.

%%%===================================================================
%%% API functions
%%%===================================================================

find_all(PoolName, Collection) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {find_all, Collection})
                                end).

execQuery(PoolName, Sql) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {plain_sql, Sql})
                                end).

execQueryWithArgs(PoolName, Sql, Args) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {args_query, Sql, Args})
                                end).

exec(PoolName, Sql, Args) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    {ok, Stmt} = gen_server:call(Worker, {prepare_sql, Sql}),
    gen_server:call(Worker, {bind_params, Stmt, Args}),
    gen_server:call(Worker, {fetchmany, 100})
                                end).

execCallProc(PoolName, Sql, Args) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {call_proc_no_args, Sql, Args})
                                end).