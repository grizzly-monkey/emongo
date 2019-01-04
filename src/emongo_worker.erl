-module(emongo_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-record(state, {id, conn}).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
  process_flag(trap_exit, true),
  case emongo_nif:connect("mongodb+srv://glabbr:Secure04@glabbr-hbcsf.mongodb.net/test") of
    {ok, Conn} ->
      {ok, #state{id = "default", conn = Conn}};
    {error, Error} ->
      {error, Error}
  end.
handle_call({find_all, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:find_all(Conn, Collection), State};
handle_call({list_databases}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_databases(Conn), State};
handle_call({list_collections, DbName}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:list_collections(Conn, DbName), State};
handle_call({collection_exists, DbName, Collection}, _From, #state{conn = Conn} = State) ->
  {reply, emongo_nif:collection_exists(Conn, DbName, Collection), State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{conn = _Conn}) ->
  {ok, "disconnect"}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.