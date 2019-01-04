#include <fstream>
#include <string>
#include <iostream>

#include "erl_nif.h"

#include "mongo_api.h"
#include "mongo_connection.h"

struct mongo_conn {
    mongo_connection *connection;
};

static struct {
    ERL_NIF_TERM ok;
    ERL_NIF_TERM error;
} emongo_atoms;

typedef struct mongo_conn mongo_conn;


inline ERL_NIF_TERM make_atom(ErlNifEnv *env, const char *atom_str) {
    ERL_NIF_TERM atom;
    if (!enif_make_existing_atom(env, atom_str, &atom, ERL_NIF_LATIN1)) {
        atom = enif_make_atom(env, atom_str);
    }
    return atom;
}
