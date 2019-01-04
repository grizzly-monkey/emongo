#include <utility>

#include "emongo.h"


mongo_connection::mongo_connection(ErlNifEnv *env, std::string connection_id)
        : connection_id(std::move(connection_id)), env_(env) {}

mongo_connection::~mongo_connection() {
}

void *mongo_connection::get_connection() { return this; }

std::string mongo_connection::get_connection_id() { return this->connection_id; }

std::string mongo_connection::get_db() {
    return this->db;
}

mongocxx::pool *mongo_connection::get_connection_pool() {
    return this->mongo_conn_pool;
}

mongo_response mongo_connection::connect(const char *conn_str) {
    try {
        mongocxx::uri uri(conn_str);
        if (uri.database().empty()) {
            return mongo_response{true, "Database cannot be empty in an URI"};
        }
        db = uri.database();
        mongo_conn_pool = new mongocxx::pool{uri};
        return mongo_response{false, "connected"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__);
}