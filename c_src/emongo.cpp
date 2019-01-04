#include "emongo.h"
#include "mongo_response.h"

#define MAX_BUFFER_LEN 1024

static ErlNifResourceType *emongo_cursor;
static ErlNifResourceType *emongo_srsr;

mongo_api *api;

static ERL_NIF_TERM build_response(ErlNifEnv *env, mongo_response api_response) {

    if (api_response.status) {
        return enif_make_tuple2(env, emongo_atoms.error,
                                enif_make_string(env, api_response.result.c_str(), ERL_NIF_LATIN1));
    }

    return enif_make_tuple2(env, emongo_atoms.ok,
                            enif_make_string(env, api_response.result.c_str(), ERL_NIF_LATIN1));
}

static ERL_NIF_TERM connect(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]) {

    char *connection_id = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[0], connection_id, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *url = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], url, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_connection *conn = new mongo_connection(env, connection_id);


    mongo_response response = conn->connect(url);

    if (response.status) {
        return enif_make_tuple2(env, emongo_atoms.error,
                                enif_make_string(env, "error connecting", ERL_NIF_LATIN1));
    }

    auto *mongo_conn_handle =
            (mongo_conn *) enif_alloc_resource(emongo_cursor, sizeof(mongo_conn));

    mongo_conn_handle->connection = conn;

    ERL_NIF_TERM result = enif_make_resource(env, mongo_conn_handle);
    enif_release_resource(mongo_conn_handle);

    return enif_make_tuple2(env, emongo_atoms.ok, result);
}

static ERL_NIF_TERM find_all(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]) {

    if (argc != 2) {
        return build_response(env, mongo_response{true, "find_all needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->find_all(mongo_conn_handle->connection, collection);

    return build_response(env, response);
}


static ERL_NIF_TERM find_all_query(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "find_all_query needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *sort_key = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], sort_key, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    int order;

    if (!enif_get_int(env, argv[3], &order)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->find_all(mongo_conn_handle->connection, collection, sort_key, order);

    return build_response(env, response);
}

static ERL_NIF_TERM find(ErlNifEnv *env, int argc,
                         const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "find needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *filter = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], filter, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->find(mongo_conn_handle->connection, collection, filter);

    return build_response(env, response);
}


static ERL_NIF_TERM find_query(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {

    if (argc != 5) {
        return build_response(env, mongo_response{true, "find_query needs 5 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *filter = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], filter, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *sort_key = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[3], sort_key, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    int order;

    if (!enif_get_int(env, argv[4], &order)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->find(mongo_conn_handle->connection, collection, filter, sort_key, order);

    return build_response(env, response);
}

static ERL_NIF_TERM find_by_id(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "find_by_id needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }


    char *id = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], id, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->find_by_id(mongo_conn_handle->connection, collection, id);

    return build_response(env, response);
}


static ERL_NIF_TERM exists(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "exists needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *query = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], query, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->exists(mongo_conn_handle->connection, collection, query);

    return build_response(env, response);
}


static ERL_NIF_TERM count(ErlNifEnv *env, int argc,
                          const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "count needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *db_name = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], db_name, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }


    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }


    char *filter = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[3], filter, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->count(mongo_conn_handle->connection, db_name, collection, filter);

    return build_response(env, response);
}

static ERL_NIF_TERM insert(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "insert needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }


    long json_data_len;

    if (!enif_get_long(env, argv[2], &json_data_len)) {
        return enif_make_badarg(env);
    }

    char *json_data = (char *) malloc(json_data_len);

    if (!enif_get_string(env, argv[3], json_data, json_data_len,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->insert(mongo_conn_handle->connection, collection, json_data);

    return build_response(env, response);
}

static ERL_NIF_TERM update_by_id(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]) {

    if (argc != 5) {
        return build_response(env, mongo_response{true, "update_by_id needs 5 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *id = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], id, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    long json_data_len;

    if (!enif_get_long(env, argv[3], &json_data_len)) {
        return enif_make_badarg(env);
    }

    char *json_data = (char *) malloc(json_data_len);

    if (!enif_get_string(env, argv[4], json_data, json_data_len,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->update_by_id(mongo_conn_handle->connection, collection, id, json_data);

    return build_response(env, response);
}

static ERL_NIF_TERM update_by_query(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 6) {
        return build_response(env, mongo_response{true, "update_by_query needs 6 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    long json_query_len;

    if (!enif_get_long(env, argv[2], &json_query_len)) {
        return enif_make_badarg(env);
    }

    char *json_query = (char *) malloc(json_query_len);

    if (!enif_get_string(env, argv[3], json_query, json_query_len,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    long json_data_len;

    if (!enif_get_long(env, argv[4], &json_data_len)) {
        return enif_make_badarg(env);
    }

    char *json_data = (char *) malloc(json_data_len);

    if (!enif_get_string(env, argv[5], json_data, json_data_len,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->update_by_query(mongo_conn_handle->connection, collection,
                                                   json_query, json_data);

    return build_response(env, response);
}

static ERL_NIF_TERM delete_by_id(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "delete_by_id needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *id = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], id, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->delete_by_id(mongo_conn_handle->connection, collection, id);

    return build_response(env, response);
}

static ERL_NIF_TERM delete_by_query(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "delete_by_query needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    long json_query_len;

    if (!enif_get_long(env, argv[2], &json_query_len)) {
        return enif_make_badarg(env);
    }

    char *json_query = (char *) malloc(json_query_len);

    if (!enif_get_string(env, argv[3], json_query, json_query_len,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->delete_by_query(mongo_conn_handle->connection, collection,
                                                   json_query);

    return build_response(env, response);
}


static ERL_NIF_TERM list_databases(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {

    if (argc != 1) {
        return build_response(env, mongo_response{true, "list_databases needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->list_databases(mongo_conn_handle->connection);

    return build_response(env, response);
}

static ERL_NIF_TERM list_collections(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {

    if (argc != 2) {
        return build_response(env, mongo_response{true, "list_collections needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *db_name = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], db_name, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->list_collections(mongo_conn_handle->connection, db_name);

    return build_response(env, response);
}

static ERL_NIF_TERM collection_exists(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "collection_exists needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }

    char *db_name = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[1], db_name, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    char *collection = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv[2], collection, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }

    mongo_response response = api->collection_exists(mongo_conn_handle->connection, db_name, collection);

    return build_response(env, response);
}

/*static ERL_NIF_TERM disconnect(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    mongo_conn *mongo_conn_handle;
    if (!enif_get_resource(env, argv[0], emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        return enif_make_badarg(env);
    }
    if (mongo_conn_handle->connection->close()) {
        return build_response(env, mongo_response{false, "disconnected"});
    }
    return build_response(env, mongo_response{true, "disconnect failed"});
}*/


static void unload_connect(ErlNifEnv *env, void *arg) {}

static int load_init(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info) {
    auto flags =
            (ErlNifResourceFlags) (ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER);

    emongo_cursor = enif_open_resource_type(env, "emongo_nif", "emongo_cursor",
                                            &unload_connect, flags, nullptr);
    if (!emongo_cursor) {
        return -1;
    }

    emongo_srsr = enif_open_resource_type(env, "emongo_nif", "emongo_srsr",
                                          &unload_connect, flags, nullptr);
    if (!emongo_srsr) {
        return -1;
    }

    mongocxx::instance instance{};
    api = new mongo_api();

    emongo_atoms.ok = make_atom(env, "ok");
    emongo_atoms.error = make_atom(env, "error");

    return 0;
}

static ErlNifFunc nif_funcs[] = {{"connect",           2, connect,           ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"find_all",          2, find_all,          ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find_all",          4, find_all_query,    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find",              3, find,              ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find",              5, find_query,        ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find_by_id",        3, find_by_id,        ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"exists",            3, exists,            ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"count",             4, count,             ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"insert",            4, insert,            ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_id",      5, update_by_id,      ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_query",   6, update_by_query,   ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_id",      3, delete_by_id,      ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_query",   4, delete_by_query,   ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"list_databases",    1, list_databases,    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"list_collections",  2, list_collections,  ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"collection_exists", 3, collection_exists, ERL_NIF_DIRTY_JOB_IO_BOUND}
        /*{"disconnect", 1, disconnect, ERL_NIF_DIRTY_JOB_IO_BOUND}*/};

ERL_NIF_INIT(emongo_nif, nif_funcs, &load_init, NULL, NULL, NULL
);
