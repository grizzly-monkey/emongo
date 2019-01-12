#include <fstream>
#include <string>
#include <iostream>

#include "erl_nif.h"

#include "mongo_api.h"
#include "mongo_connection.h"

#define MAX_BUFFER_LEN 1024
#define MAX_DB_NAME_LEN 66
#define MAX_COLLECTION_NAME_LEN 130

#define DEFAULT_BATCH_SIZE 10000
#define DEFAULT_QUERY_LIMIT 50000

#define DEFAULT_EXEC_TIME 1000000


struct mongo_conn {
    mongo_connection *connection;
};

static struct {
    ERL_NIF_TERM ok;
    ERL_NIF_TERM error;

    // Create collection opts
    ERL_NIF_TERM size; /// int
    ERL_NIF_TERM storage_engine; /// document
    ERL_NIF_TERM capped; /// boolean
    ERL_NIF_TERM no_padding; /// boolean
    ERL_NIF_TERM max; /// int
    ERL_NIF_TERM collation; ///document
    // validation for collection
    ERL_NIF_TERM validator; /// document
    ERL_NIF_TERM validation_level; /// atom
    ERL_NIF_TERM validation_action; /// atom


    // Write Concern
    ERL_NIF_TERM nodes; /// int
    ERL_NIF_TERM journal; /// int
    ERL_NIF_TERM timeout; /// long
    ERL_NIF_TERM majority; /// long
    ERL_NIF_TERM tag; /// string
    ERL_NIF_TERM acknowledge_level; /// atom


    // Find Opts
    ERL_NIF_TERM sort; /// document
    ERL_NIF_TERM cursor_type; /// atom
    ERL_NIF_TERM skip; /// int
    ERL_NIF_TERM batch_size; /// int
    ERL_NIF_TERM comment; /// document
    ERL_NIF_TERM allow_partial_results; /// boolean
    ERL_NIF_TERM show_record_id; /// boolean
    ERL_NIF_TERM min; /// document
    ERL_NIF_TERM hint; /// document
    ERL_NIF_TERM max_time; /// int
    ERL_NIF_TERM max_await_time; /// int
    ERL_NIF_TERM projection; /// document
    ERL_NIF_TERM return_key; /// boolean
    ERL_NIF_TERM no_cursor_timeout; /// boolean
    ERL_NIF_TERM limit; /// int
    // read preferences for find otps
    ERL_NIF_TERM max_staleness; /// int
    ERL_NIF_TERM read_mode;  /// atom
    ERL_NIF_TERM tags; /// document

    //insert_opts
    ERL_NIF_TERM bypass_document_validation; /// boolean
    ERL_NIF_TERM ordered; /// boolean

    ERL_NIF_TERM array_filters; /// document
    ERL_NIF_TERM upsert; /// boolean


} emongo_atoms;

typedef struct mongo_conn mongo_conn;


inline ERL_NIF_TERM make_atom(ErlNifEnv *env, const char *atom_str) {
    ERL_NIF_TERM atom;
    if (!enif_make_existing_atom(env, atom_str, &atom, ERL_NIF_LATIN1)) {
        atom = enif_make_atom(env, atom_str);
    }
    return atom;
}
