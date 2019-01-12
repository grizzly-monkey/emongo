emongo
=========

MongoDB  driver for erlang
rewritten from [erlang-mongo-driver](https://github.com/comtihon/mongodb-erlang) 

Why emongo
==========
 Emongo is developed using NIF binding instead of pure native implementation.
 this maintains upstream client changes as well as server version compatibility.
 * Emongo supports multi-tenantancy out of the box.
 * It uses C++ based mongocxx pools for connection pools , reducing thread management overhead.
 * Performant erlang mongo client.
 * Simple API's in terms with mongo client API's.
 
Design principle
==========
 Starts a different erlang pid for each tenant using poolboy. Each tenant then creates its own connection pool.
 To avoid VM blocking NIF's are written as  ```ERL_NIF_DIRTY_JOB_IO_BOUND``` 

Usage
=========

0.Run erlang <br>
<code>erl -pa ebin  deps/poolboy/ebin</code>

```erl
%% Start emongo app with default connection/tenant.
emongo:start("mongodb://localhost/db").

%% Find all documents in a collection
emongo:find_all("collection").

%% Find all documents in a collection with find options.
emongo:find_all("collection",#{limit=>23}).

%% Add a tenant 
emongo:add_tenant(tenant1,"mongodb://localhost/t1db").

%% Find all documents in a collection for tenant1
emongo:find_all(tenant1,"collection").
```

Alternatives
============
* [erlang-mongo-driver](https://github.com/comtihon/mongodb-erlang)

Note
==================
I have little knowledge with C/C++ so would encourage 
someone to look at the C++ code that is written to 
make sure I have written it in a good way.