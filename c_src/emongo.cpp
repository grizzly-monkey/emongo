#include "emongo.h"

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


mongo_conn *get_connection(ErlNifEnv *env, ERL_NIF_TERM argv) {
    mongo_conn *mongo_conn_handle;

    if (!enif_get_resource(env, argv, emongo_cursor,
                           (void **) &mongo_conn_handle)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid Connection reference");
    }
    return mongo_conn_handle;
}

char *get_collection_name(ErlNifEnv *env, ERL_NIF_TERM argv) {
    char *collection = (char *) malloc(MAX_COLLECTION_NAME_LEN);

    if (!enif_get_string(env, argv, collection, MAX_COLLECTION_NAME_LEN,
                         ERL_NIF_LATIN1)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid Collection Name");
    }
    return collection;
}

char *get_db_name(ErlNifEnv *env, ERL_NIF_TERM argv) {
    char *db_name = (char *) malloc(MAX_DB_NAME_LEN);

    if (!enif_get_string(env, argv, db_name, MAX_DB_NAME_LEN,
                         ERL_NIF_LATIN1)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid Database Name");
    }
    return db_name;
}

char *get_json(ErlNifEnv *env, ERL_NIF_TERM argv) {
    char *json_doc = (char *) malloc(MAX_BUFFER_LEN);

    if (!enif_get_string(env, argv, json_doc, MAX_BUFFER_LEN,
                         ERL_NIF_LATIN1)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid string");
    }
    return json_doc;
}

char *get_json(ErlNifEnv *env, ERL_NIF_TERM len, ERL_NIF_TERM argv) {
    long json_data_len;

    if (!enif_get_long(env, len, &json_data_len)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid length");
    }


    char *json_doc = (char *) malloc((size_t) json_data_len++);

    if (!enif_get_string(env, argv, json_doc, json_data_len,
                         ERL_NIF_LATIN1)) {
        throw std::system_error(std::error_code(EINVAL, std::generic_category()),
                                "Invalid string");
    }
    return json_doc;
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
                                enif_make_string(env, response.result.c_str(), ERL_NIF_LATIN1));
    }

    auto *mongo_conn_handle =
            (mongo_conn *) enif_alloc_resource(emongo_cursor, sizeof(mongo_conn));

    mongo_conn_handle->connection = conn;

    ERL_NIF_TERM result = enif_make_resource(env, mongo_conn_handle);
    enif_release_resource(mongo_conn_handle);

    return enif_make_tuple2(env, emongo_atoms.ok, result);
}

void build_read_concern(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::read_concern &rc) {

    ERL_NIF_TERM map_val;


    if (enif_get_map_value(env, opt_map, emongo_atoms.acknowledge_level, &map_val)) {
        char *acknowledge_level = (char *) malloc(20);
        if (enif_get_atom(env, map_val, acknowledge_level, 20, ERL_NIF_LATIN1)) {
            if (strcasecmp(acknowledge_level, "majority") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_majority);
            } else if (strcasecmp(acknowledge_level, "available") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_available);
            } else if (strcasecmp(acknowledge_level, "linearizable") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_linearizable);
            } else if (strcasecmp(acknowledge_level, "local") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_local);
            } else if (strcasecmp(acknowledge_level, "server_default") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_server_default);
            } else if (strcasecmp(acknowledge_level, "snapshot") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_snapshot);
            } else if (strcasecmp(acknowledge_level, "unknown") == 0) {
                rc.acknowledge_level(mongocxx::read_concern::level::k_unknown);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()),
                                         "Invalid read concern acknowledge level");
            }
        }
    }
}


static ERL_NIF_TERM set_read_concern(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "set_read_concern needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[3], &map_len);

    mongocxx::read_concern read_concern{};

    if (map_len > 0) {
        try {
            build_read_concern(env, argv[3], read_concern);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->set_read_concern(mongo_conn_handle->connection, db_name,
                                                    collection, read_concern);

    return build_response(env, response);
}

void build_write_concern(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::write_concern &wc) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.nodes, &map_val)) {
        int nodes;
        if (enif_get_int(env, map_val, &nodes)) {
            wc.nodes(nodes);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.timeout, &map_val)) {
        long timeout;
        if (enif_get_int64(env, map_val, &timeout)) {
            wc.timeout(std::chrono::milliseconds(timeout));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.majority, &map_val)) {
        long majority;
        if (enif_get_int64(env, map_val, &majority)) {
            wc.majority(std::chrono::milliseconds(majority));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.journal, &map_val)) {
        char *journal = (char *) malloc(6);
        if (enif_get_atom(env, map_val, journal, 6, ERL_NIF_LATIN1)) {
            wc.journal(strcasecmp(journal, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.tag, &map_val)) {
        char *tag = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, tag, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            wc.tag(tag);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.acknowledge_level, &map_val)) {
        char *acknowledge_level = (char *) malloc(20);
        if (enif_get_atom(env, map_val, acknowledge_level, 20, ERL_NIF_LATIN1)) {
            if (strcasecmp(acknowledge_level, "acknowledged") == 0) {
                wc.acknowledge_level(mongocxx::write_concern::level::k_acknowledged);
            } else if (strcasecmp(acknowledge_level, "unacknowledged") == 0) {
                wc.acknowledge_level(mongocxx::write_concern::level::k_unacknowledged);
            } else if (strcasecmp(acknowledge_level, "majority") == 0) {
                wc.acknowledge_level(mongocxx::write_concern::level::k_majority);
            } else if (strcasecmp(acknowledge_level, "tag") == 0) {
                wc.acknowledge_level(mongocxx::write_concern::level::k_tag);
            } else if (strcasecmp(acknowledge_level, "default") == 0) {
                wc.acknowledge_level(mongocxx::write_concern::level::k_default);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()),
                                         "Invalid write concern acknowledge level");
            }
        }
    }
}


static ERL_NIF_TERM set_write_concern(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "collection_exists needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[3], &map_len);

    mongocxx::write_concern write_concern{};

    if (map_len > 0) {
        try {
            build_write_concern(env, argv[3], write_concern);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }


    mongo_response response = api->set_write_concern(mongo_conn_handle->connection, db_name,
                                                     collection, write_concern);

    return build_response(env, response);
}


void build_find_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::options::find &find_opts) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.limit, &map_val)) {
        int limit;
        if (enif_get_int(env, map_val, &limit)) {
            find_opts.limit(limit);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.batch_size, &map_val)) {
        int batch_size;
        if (enif_get_int(env, map_val, &batch_size)) {
            find_opts.batch_size(batch_size);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.skip, &map_val)) {
        long skip;
        if (enif_get_int64(env, map_val, &skip)) {
            find_opts.skip(skip);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.max_await_time, &map_val)) {
        long max_await_time;
        if (enif_get_int64(env, map_val, &max_await_time)) {
            find_opts.max_await_time(std::chrono::milliseconds(max_await_time));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.max_time, &map_val)) {
        long max_time;
        if (enif_get_int64(env, map_val, &max_time)) {
            find_opts.max_time(std::chrono::milliseconds(max_time));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.no_cursor_timeout, &map_val)) {
        char *no_cursor_timeout = (char *) malloc(6);
        if (enif_get_atom(env, map_val, no_cursor_timeout, 6, ERL_NIF_LATIN1)) {
            find_opts.no_cursor_timeout(strcasecmp(no_cursor_timeout, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.return_key, &map_val)) {
        char *return_key = (char *) malloc(6);
        if (enif_get_atom(env, map_val, return_key, 6, ERL_NIF_LATIN1)) {
            find_opts.return_key(strcasecmp(return_key, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.show_record_id, &map_val)) {
        char *show_record_id = (char *) malloc(6);
        if (enif_get_atom(env, map_val, show_record_id, 6, ERL_NIF_LATIN1)) {
            find_opts.show_record_id(strcasecmp(show_record_id, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.allow_partial_results, &map_val)) {
        char *allow_partial_results = (char *) malloc(6);
        if (enif_get_atom(env, map_val, allow_partial_results, 6, ERL_NIF_LATIN1)) {
            find_opts.allow_partial_results(strcasecmp(allow_partial_results, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.max, &map_val)) {
        char *max = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, max, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.max(bsoncxx::from_json(max));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.collation, &map_val)) {
        char *collation = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, collation, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.collation(bsoncxx::from_json(collation));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.projection, &map_val)) {
        char *projection = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, projection, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.projection(bsoncxx::from_json(projection));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.min, &map_val)) {
        char *min = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, min, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.min(bsoncxx::from_json(min));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.sort, &map_val)) {
        char *sort = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, sort, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.sort(bsoncxx::from_json(sort));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.comment, &map_val)) {
        char *comment = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, comment, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.comment(comment);
        }
    }


    if (enif_get_map_value(env, opt_map, emongo_atoms.hint, &map_val)) {
        char *hint = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, hint, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            find_opts.hint(mongocxx::hint{bsoncxx::from_json(hint)});
        }
    }


    if (enif_get_map_value(env, opt_map, emongo_atoms.cursor_type, &map_val)) {
        char *cursor_type = (char *) malloc(20);
        if (enif_get_atom(env, map_val, cursor_type, 20, ERL_NIF_LATIN1)) {
            if (strcasecmp(cursor_type, "non_tailable") == 0) {
                find_opts.cursor_type(mongocxx::cursor::type::k_non_tailable);
            } else if (strcasecmp(cursor_type, "tailable") == 0) {
                find_opts.cursor_type(mongocxx::cursor::type::k_tailable);
            } else if (strcasecmp(cursor_type, "tailable_await") == 0) {
                find_opts.cursor_type(mongocxx::cursor::type::k_tailable_await);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()),
                                         "Invalid cursor type");
            }
        }
    }

    bool set_read_pref = false;
    mongocxx::read_preference read_preference_opts{};

    if (enif_get_map_value(env, opt_map, emongo_atoms.max_staleness, &map_val)) {
        long max_staleness;
        if (enif_get_int64(env, map_val, &max_staleness)) {
            set_read_pref = true;
            read_preference_opts.max_staleness(std::chrono::seconds(max_staleness));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.tags, &map_val)) {
        char *tags = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, tags, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            set_read_pref = true;
            read_preference_opts.tags(bsoncxx::from_json(tags));
        }
    }


    if (enif_get_map_value(env, opt_map, emongo_atoms.read_mode, &map_val)) {
        char *read_mode = (char *) malloc(30);
        if (enif_get_atom(env, map_val, read_mode, 30, ERL_NIF_LATIN1)) {
            if (strcasecmp(read_mode, "k_nearest") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_nearest);
            } else if (strcasecmp(read_mode, "primary") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_primary);
            } else if (strcasecmp(read_mode, "primary_preferred") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_primary_preferred);
            } else if (strcasecmp(read_mode, "secondary") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_secondary);
            } else if (strcasecmp(read_mode, "secondary_preferred") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_secondary_preferred);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()),
                                         "Invalid read preference mode ");
            }
        }

        if (set_read_pref) {
            find_opts.read_preference(read_preference_opts);
        }
    }
}

void build_default_find_opts(const int limit, mongocxx::options::find &find_opts) {
    find_opts.batch_size(DEFAULT_BATCH_SIZE);
    find_opts.limit(limit == 0 ? DEFAULT_QUERY_LIMIT : limit);
    find_opts.max_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    find_opts.no_cursor_timeout(true);
    find_opts.max_await_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
}

static ERL_NIF_TERM find_all(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[]) {

    if (argc != 2) {
        return build_response(env, mongo_response{true, "find_all needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[2], &map_len);

    mongocxx::options::find find_opts{};
    build_default_find_opts(0, find_opts);
    mongo_response response = api->find_all(mongo_conn_handle->connection, collection,
                                            find_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM find_all_with_opts(ErlNifEnv *env, int argc,
                                       const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "find_all_with_opts needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[2], &map_len);

    mongocxx::options::find find_opts{};

    if (map_len > 0) {
        try {
            build_find_opts(env, argv[2], find_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->find_all(mongo_conn_handle->connection, collection, find_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM find(ErlNifEnv *env, int argc,
                         const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "find needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *filter;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        filter = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::find find_opts{};
    build_default_find_opts(0, find_opts);

    mongo_response response = api->find(mongo_conn_handle->connection, collection, filter, find_opts);

    return build_response(env, response);
}


static ERL_NIF_TERM find_with_opts(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "find_with_opts needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *filter;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        filter = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[3], &map_len);

    mongocxx::options::find find_opts{};

    if (map_len > 0) {
        try {
            build_find_opts(env, argv[3], find_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->find(mongo_conn_handle->connection, collection, filter, find_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM find_by_id(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "find_by_id needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *id;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        id = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::find find_opts{};
    build_default_find_opts(1, find_opts);

    mongo_response response = api->find_by_id(mongo_conn_handle->connection, collection, id, find_opts);

    return build_response(env, response);
}


static ERL_NIF_TERM exists(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "exists needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *query;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        query = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::find find_opts{};
    build_default_find_opts(1, find_opts);

    mongo_response response = api->exists(mongo_conn_handle->connection, collection, query, find_opts);

    return build_response(env, response);
}

void build_count_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::options::count &count_opts) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.limit, &map_val)) {
        int limit;
        if (enif_get_int(env, map_val, &limit)) {
            count_opts.limit(limit);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.skip, &map_val)) {
        long skip;
        if (enif_get_int64(env, map_val, &skip)) {
            count_opts.skip(skip);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.max_time, &map_val)) {
        long max_time;
        if (enif_get_int64(env, map_val, &max_time)) {
            count_opts.max_time(std::chrono::milliseconds(max_time));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.collation, &map_val)) {
        char *collation = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, collation, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            count_opts.collation(bsoncxx::from_json(collation));
        }
    }


    if (enif_get_map_value(env, opt_map, emongo_atoms.hint, &map_val)) {
        char *hint = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, hint, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            count_opts.hint(mongocxx::hint{bsoncxx::from_json(hint)});
        }
    }


    bool set_read_pref = false;
    mongocxx::read_preference read_preference_opts{};

    if (enif_get_map_value(env, opt_map, emongo_atoms.max_staleness, &map_val)) {
        long max_staleness;
        if (enif_get_int64(env, map_val, &max_staleness)) {
            set_read_pref = true;
            read_preference_opts.max_staleness(std::chrono::seconds(max_staleness));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.tags, &map_val)) {
        char *tags = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, tags, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            set_read_pref = true;
            read_preference_opts.tags(bsoncxx::from_json(tags));
        }
    }


    if (enif_get_map_value(env, opt_map, emongo_atoms.read_mode, &map_val)) {
        char *read_mode = (char *) malloc(30);
        if (enif_get_atom(env, map_val, read_mode, 30, ERL_NIF_LATIN1)) {
            if (strcasecmp(read_mode, "k_nearest") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_nearest);
            } else if (strcasecmp(read_mode, "primary") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_primary);
            } else if (strcasecmp(read_mode, "primary_preferred") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_primary_preferred);
            } else if (strcasecmp(read_mode, "secondary") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_secondary);
            } else if (strcasecmp(read_mode, "secondary_preferred") == 0) {
                set_read_pref = true;
                read_preference_opts.mode(mongocxx::read_preference::read_mode::k_secondary_preferred);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()),
                                         "Invalid read preference mode ");
            }
        }

        if (set_read_pref) {
            count_opts.read_preference(read_preference_opts);
        }
    }
}


static ERL_NIF_TERM count(ErlNifEnv *env, int argc,
                          const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "count needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    char *filter;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
        filter = get_json(env, argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::count count_opts;
    count_opts.limit(DEFAULT_QUERY_LIMIT);
    count_opts.max_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));

    mongo_response response = api->count(mongo_conn_handle->connection, db_name, collection, filter, count_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM count_with_opts(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 5) {
        return build_response(env, mongo_response{true, "count_with_opts needs 5 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    char *filter;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
        filter = get_json(env, argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[4], &map_len);

    mongocxx::options::count count_opts{};

    if (map_len > 0) {
        try {
            build_count_opts(env, argv[4], count_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->count(mongo_conn_handle->connection, db_name, collection, filter, count_opts);

    return build_response(env, response);
}

void build_insert_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::options::insert &insert_opts) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.ordered, &map_val)) {
        char *ordered = (char *) malloc(8);
        if (enif_get_atom(env, map_val, ordered, 8, ERL_NIF_LATIN1)) {
            insert_opts.ordered(strcasecmp(ordered, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.bypass_document_validation, &map_val)) {
        char *bypass_document_validation = (char *) malloc(30);
        if (enif_get_atom(env, map_val, bypass_document_validation, 30, ERL_NIF_LATIN1)) {
            insert_opts.bypass_document_validation(strcasecmp(bypass_document_validation, "true") == 0);
        }
    }


    mongocxx::write_concern write_concern_opts{};

    build_write_concern(env, opt_map, write_concern_opts);
    insert_opts.write_concern(write_concern_opts);
}


static ERL_NIF_TERM insert(ErlNifEnv *env, int argc,
                           const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "insert needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_data = get_json(env, argv[2], argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::insert insert_opts{};
    insert_opts.bypass_document_validation(false);
    insert_opts.ordered(false);

    mongo_response response = api->insert(mongo_conn_handle->connection, collection, json_data, insert_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM insert_with_opts(ErlNifEnv *env, int argc,
                                     const ERL_NIF_TERM argv[]) {

    if (argc != 5) {
        return build_response(env, mongo_response{true, "insert_with_opts needs 5 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_data = get_json(env, argv[2], argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[4], &map_len);

    mongocxx::options::insert insert_opts{};

    if (map_len > 0) {
        try {
            build_insert_opts(env, argv[4], insert_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }


    mongo_response response = api->insert(mongo_conn_handle->connection, collection, json_data, insert_opts);

    return build_response(env, response);
}

void build_update_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::options::update &update_opts) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.upsert, &map_val)) {
        char *upsert = (char *) malloc(8);
        if (enif_get_atom(env, map_val, upsert, 8, ERL_NIF_LATIN1)) {
            update_opts.upsert(strcasecmp(upsert, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.bypass_document_validation, &map_val)) {
        char *bypass_document_validation = (char *) malloc(30);
        if (enif_get_atom(env, map_val, bypass_document_validation, 30, ERL_NIF_LATIN1)) {
            update_opts.bypass_document_validation(strcasecmp(bypass_document_validation, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.collation, &map_val)) {
        char *collation = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, collation, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            update_opts.collation(bsoncxx::from_json(collation));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.array_filters, &map_val)) {
        char *array_filters = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, array_filters, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            auto bson_docs = bsoncxx::from_json(array_filters);
            for (auto &&element : bson_docs.view()) {
                if (element.type() == bsoncxx::type::k_array) {
                    update_opts.array_filters(element.get_array().value);
                }
                break;
            }

        }
    }

    mongocxx::write_concern write_concern_opts{};

    build_write_concern(env, opt_map, write_concern_opts);
    update_opts.write_concern(write_concern_opts);
}


static ERL_NIF_TERM update_by_id(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]) {

    if (argc != 5) {
        return build_response(env, mongo_response{true, "update_by_id needs 5 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *id;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        id = get_json(env, argv[2]);
        json_data = get_json(env, argv[3], argv[4]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::update update_opts{};
    update_opts.upsert(false);
    update_opts.bypass_document_validation(false);

    mongo_response response = api->update_by_id(mongo_conn_handle->connection, collection, id, json_data, update_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM update_by_id_with_opts(ErlNifEnv *env, int argc,
                                           const ERL_NIF_TERM argv[]) {

    if (argc != 6) {
        return build_response(env, mongo_response{true, "update_by_id_with_opts needs 6 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *id;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        id = get_json(env, argv[2]);
        json_data = get_json(env, argv[3], argv[4]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }


    size_t map_len;
    enif_get_map_size(env, argv[5], &map_len);

    mongocxx::options::update update_opts{};

    if (map_len > 0) {
        try {
            build_update_opts(env, argv[5], update_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }


    mongo_response response = api->update_by_id(mongo_conn_handle->connection, collection, id, json_data, update_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM update_by_query(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 6) {
        return build_response(env, mongo_response{true, "update_by_query needs 6 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_query;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_query = get_json(env, argv[2], argv[3]);
        json_data = get_json(env, argv[4], argv[5]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::update update_opts{};
    update_opts.upsert(false);
    update_opts.bypass_document_validation(false);

    mongo_response response = api->update_by_query(mongo_conn_handle->connection, collection,
                                                   json_query, json_data, update_opts);

    return build_response(env, response);
}

static ERL_NIF_TERM update_by_query_with_opts(ErlNifEnv *env, int argc,
                                              const ERL_NIF_TERM argv[]) {

    if (argc != 7) {
        return build_response(env, mongo_response{true, "update_by_query needs 7 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_query;
    char *json_data;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_query = get_json(env, argv[2], argv[3]);
        json_data = get_json(env, argv[4], argv[5]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[6], &map_len);

    mongocxx::options::update update_opts{};

    if (map_len > 0) {
        try {
            build_update_opts(env, argv[6], update_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->update_by_query(mongo_conn_handle->connection, collection,
                                                   json_query, json_data, update_opts);

    return build_response(env, response);
}

void build_delete_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map, mongocxx::options::delete_options &delete_options) {

    ERL_NIF_TERM map_val;


    if (enif_get_map_value(env, opt_map, emongo_atoms.collation, &map_val)) {
        char *collation = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, collation, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            delete_options.collation(bsoncxx::from_json(collation));
        }
    }


    mongocxx::write_concern write_concern_opts{};

    build_write_concern(env, opt_map, write_concern_opts);
    delete_options.write_concern(write_concern_opts);
}


static ERL_NIF_TERM delete_by_id(ErlNifEnv *env, int argc,
                                 const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "delete_by_id needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *id;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        id = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::delete_options delete_options{};

    mongo_response response = api->delete_by_id(mongo_conn_handle->connection, collection, id, delete_options);

    return build_response(env, response);
}

static ERL_NIF_TERM delete_by_id_with_opts(ErlNifEnv *env, int argc,
                                           const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "delete_by_id_with_opts needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *id;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        id = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::delete_options delete_options{};
    size_t map_len;
    enif_get_map_size(env, argv[3], &map_len);


    if (map_len > 0) {
        try {
            build_delete_opts(env, argv[3], delete_options);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }


    mongo_response response = api->delete_by_id(mongo_conn_handle->connection, collection, id, delete_options);

    return build_response(env, response);
}

static ERL_NIF_TERM delete_by_query(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "delete_by_query needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_query;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_query = get_json(env, argv[2], argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::delete_options delete_options{};

    mongo_response response = api->delete_by_query(mongo_conn_handle->connection, collection,
                                                   json_query, delete_options);

    return build_response(env, response);
}

static ERL_NIF_TERM delete_by_query_with_opts(ErlNifEnv *env, int argc,
                                              const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "delete_by_query needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *json_query;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        collection = get_collection_name(env, argv[1]);
        json_query = get_json(env, argv[2], argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongocxx::options::delete_options delete_options{};
    size_t map_len;
    enif_get_map_size(env, argv[4], &map_len);


    if (map_len > 0) {
        try {
            build_delete_opts(env, argv[4], delete_options);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }


    mongo_response response = api->delete_by_query(mongo_conn_handle->connection, collection,
                                                   json_query, delete_options);

    return build_response(env, response);
}


static ERL_NIF_TERM list_databases(ErlNifEnv *env, int argc,
                                   const ERL_NIF_TERM argv[]) {

    if (argc != 1) {
        return build_response(env, mongo_response{true, "list_databases needs 1 args"});
    }

    mongo_conn *mongo_conn_handle;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
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
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
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
    char *collection;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongo_response response = api->collection_exists(mongo_conn_handle->connection, db_name, collection);

    return build_response(env, response);
}


static ERL_NIF_TERM drop_db(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]) {

    if (argc != 2) {
        return build_response(env, mongo_response{true, "drop_db needs 2 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongo_response response = api->drop_db(mongo_conn_handle->connection, db_name);

    return build_response(env, response);
}


static ERL_NIF_TERM drop_collection(ErlNifEnv *env, int argc,
                                    const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "drop_collection needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongo_response response = api->drop_collection(mongo_conn_handle->connection, db_name, collection);

    return build_response(env, response);
}

void build_create_collection_opts(ErlNifEnv *env, ERL_NIF_TERM opt_map,
                                  mongocxx::options::create_collection &coll_opts) {

    ERL_NIF_TERM map_val;

    if (enif_get_map_value(env, opt_map, emongo_atoms.size, &map_val)) {
        int size;
        if (enif_get_int(env, map_val, &size)) {
            coll_opts.size(size);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.max, &map_val)) {
        int max;
        if (enif_get_int(env, map_val, &max)) {
            coll_opts.max(max);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.no_padding, &map_val)) {
        char *no_padding = (char *) malloc(6);
        if (enif_get_atom(env, map_val, no_padding, 6, ERL_NIF_LATIN1)) {
            coll_opts.no_padding(strcasecmp(no_padding, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.capped, &map_val)) {
        char *capped = (char *) malloc(6);
        if (enif_get_atom(env, map_val, capped, 6, ERL_NIF_LATIN1)) {
            coll_opts.capped(strcasecmp(capped, "true") == 0);
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.storage_engine, &map_val)) {
        char *storage_engine = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, storage_engine, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            coll_opts.storage_engine(bsoncxx::from_json(storage_engine));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.collation, &map_val)) {
        char *collation = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, collation, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            coll_opts.collation(bsoncxx::from_json(collation));
        }
    }
    mongocxx::validation_criteria valid_criteria{};

    if (enif_get_map_value(env, opt_map, emongo_atoms.validator, &map_val)) {
        char *validator = (char *) malloc(MAX_BUFFER_LEN);
        if (enif_get_string(env, map_val, validator, MAX_BUFFER_LEN, ERL_NIF_LATIN1)) {
            valid_criteria.rule(bsoncxx::from_json(validator));
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.validation_level, &map_val)) {
        char *validation_level = (char *) malloc(12);
        if (enif_get_atom(env, map_val, validation_level, 12, ERL_NIF_LATIN1)) {
            if (strcasecmp(validation_level, "moderate") == 0) {
                valid_criteria.level(mongocxx::validation_criteria::validation_level::k_moderate);
            } else if (strcasecmp(validation_level, "strict") == 0) {
                valid_criteria.level(mongocxx::validation_criteria::validation_level::k_strict);
            } else if (strcasecmp(validation_level, "off") == 0) {
                valid_criteria.level(mongocxx::validation_criteria::validation_level::k_off);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()), "Invalid validation level");
            }
        }
    }

    if (enif_get_map_value(env, opt_map, emongo_atoms.validation_action, &map_val)) {
        char *validation_action = (char *) malloc(10);
        if (enif_get_atom(env, map_val, validation_action, 10, ERL_NIF_LATIN1)) {
            if (strcasecmp(validation_action, "error") == 0) {
                valid_criteria.action(mongocxx::validation_criteria::validation_action::k_error);
            } else if (strcasecmp(validation_action, "warn") == 0) {
                valid_criteria.action(mongocxx::validation_criteria::validation_action::k_warn);
            } else {
                throw bsoncxx::exception(std::error_code(EINVAL, std::generic_category()), "Invalid validation action");
            }
        }
    }
}

static ERL_NIF_TERM create_collection(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "create_collection needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    size_t map_len;
    enif_get_map_size(env, argv[3], &map_len);

    mongocxx::options::create_collection coll_opts{};

    if (map_len > 0) {
        try {
            build_create_collection_opts(env, argv[3], coll_opts);
        } catch (bsoncxx::exception &exception) {
            return build_response(env, mongo_response{true, exception.what()});
        }
    }

    mongo_response response = api->create_collection(mongo_conn_handle->connection, db_name, collection, coll_opts);

    return build_response(env, response);
}


static ERL_NIF_TERM rename_collection(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {

    if (argc != 4) {
        return build_response(env, mongo_response{true, "rename_collection needs 4 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *collection;
    char *db_name;
    char *new_name;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        collection = get_collection_name(env, argv[2]);
        new_name = get_collection_name(env, argv[3]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongo_response response = api->rename_collection(mongo_conn_handle->connection,
                                                     db_name, collection, new_name);

    return build_response(env, response);
}


static ERL_NIF_TERM run_command(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[]) {

    if (argc != 3) {
        return build_response(env, mongo_response{true, "run_command needs 3 args"});
    }

    mongo_conn *mongo_conn_handle;
    char *db_name;
    char *command;
    try {
        mongo_conn_handle = get_connection(env, argv[0]);
        db_name = get_db_name(env, argv[1]);
        command = get_json(env, argv[2]);
    } catch (std::system_error &error) {
        return build_response(env, mongo_response{true, error.what()});
    }

    mongo_response response = api->run_command(mongo_conn_handle->connection, db_name, command);

    return build_response(env, response);
}


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

    emongo_atoms.validator = make_atom(env, "validator");
    emongo_atoms.validation_action = make_atom(env, "validation_action");
    emongo_atoms.validation_level = make_atom(env, "validation_level");
    emongo_atoms.size = make_atom(env, "size");
    emongo_atoms.capped = make_atom(env, "capped");
    emongo_atoms.collation = make_atom(env, "collation");
    emongo_atoms.max = make_atom(env, "max");
    emongo_atoms.no_padding = make_atom(env, "no_padding");
    emongo_atoms.storage_engine = make_atom(env, "storage_engine");
    emongo_atoms.nodes = make_atom(env, "nodes");
    emongo_atoms.journal = make_atom(env, "journal");
    emongo_atoms.timeout = make_atom(env, "timeout");
    emongo_atoms.majority = make_atom(env, "majority");
    emongo_atoms.tag = make_atom(env, "tag");
    emongo_atoms.acknowledge_level = make_atom(env, "acknowledge_level");
    emongo_atoms.sort = make_atom(env, "sort");
    emongo_atoms.cursor_type = make_atom(env, "cursor_type");
    emongo_atoms.skip = make_atom(env, "skip");
    emongo_atoms.batch_size = make_atom(env, "batch_size");
    emongo_atoms.comment = make_atom(env, "comment");
    emongo_atoms.allow_partial_results = make_atom(env, "allow_partial_results");
    emongo_atoms.show_record_id = make_atom(env, "show_record_id");
    emongo_atoms.min = make_atom(env, "min");
    emongo_atoms.hint = make_atom(env, "hint");
    emongo_atoms.max_time = make_atom(env, "max_time");
    emongo_atoms.max_await_time = make_atom(env, "max_await_time");
    emongo_atoms.projection = make_atom(env, "projection");
    emongo_atoms.return_key = make_atom(env, "return_key");
    emongo_atoms.no_cursor_timeout = make_atom(env, "no_cursor_timeout");
    emongo_atoms.limit = make_atom(env, "limit");
    emongo_atoms.max_staleness = make_atom(env, "max_staleness");
    emongo_atoms.read_mode = make_atom(env, "read_mode");
    emongo_atoms.tags = make_atom(env, "tags");


    return 0;
}

static ErlNifFunc nif_funcs[] = {{"connect",           2, connect,                   ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"find_all",          2, find_all,                  ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find_all",          3, find_all_with_opts,        ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find",              3, find,                      ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find",              4, find_with_opts,            ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"find_by_id",        3, find_by_id,                ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"exists",            3, exists,                    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"count",             4, count,                     ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"count",             5, count_with_opts,           ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"insert",            4, insert,                    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"insert",            5, insert_with_opts,          ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_id",      5, update_by_id,              ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_id",      6, update_by_id_with_opts,    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_query",   6, update_by_query,           ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"update_by_query",   7, update_by_query_with_opts, ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_id",      3, delete_by_id,              ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_id",      4, delete_by_id_with_opts,    ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_query",   4, delete_by_query,           ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"delete_by_query",   5, delete_by_query_with_opts, ERL_NIF_DIRTY_JOB_IO_BOUND},


                                 {"list_databases",    1, list_databases,            ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"list_collections",  2, list_collections,          ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"collection_exists", 3, collection_exists,         ERL_NIF_DIRTY_JOB_IO_BOUND},

                                 {"set_read_concern",  4, set_read_concern,          ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"set_write_concern", 4, set_write_concern,         ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"drop_db",           2, drop_db,                   ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"drop_collection",   3, drop_collection,           ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"create_collection", 4, create_collection,         ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"rename_collection", 4, rename_collection,         ERL_NIF_DIRTY_JOB_IO_BOUND},
                                 {"run_command",       3, run_command,               ERL_NIF_DIRTY_JOB_IO_BOUND}};

ERL_NIF_INIT(emongo_nif, nif_funcs, &load_init, NULL, NULL, NULL);
