#ifndef MONGO_CONNECTION_H
#define MONGO_CONNECTION_H

#include <string.h>
#include <stdio.h>
#include <iostream>
#include "mongo_exception.h"
#include "mongo_response.h"


#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

class mongo_connection {
public:
    mongo_connection(ErlNifEnv *env, std::string connection_id);

    virtual ~mongo_connection();

    void *get_connection();

    std::string get_connection_id();

    std::string get_db();

    mongocxx::pool *get_connection_pool();

    /** @brief Connect to mongo database.
     *  @see Connection::connect.
     */
    mongo_response connect(const char *mongo_uri);

protected:
    bool is_connected;
    std::string connection_id; //tenant_id
    mongo_connection(const mongo_connection &);

    mongo_connection &operator=(const mongo_connection &);

    ErlNifEnv *env_;
private:
    mongocxx::pool *mongo_conn_pool;
    std::string db;
};

#endif // SYBCONNECTION_H
