#ifndef MONGO_API_H
#define MONGO_API_H

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>


#include "mongo_response.h"
#include "mongo_connection.h"

using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

class mongo_api {
public:
    //get
    virtual mongo_response list_databases(mongo_connection *connection);

    virtual mongo_response list_collections(mongo_connection *connection, const std::string &db_name);

    virtual mongo_response collection_exists(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection);

    virtual mongo_response find_all(mongo_connection *connection, const std::string &collection,
                                    mongocxx::options::find &find_opts);


    virtual mongo_response find_by_id(mongo_connection *connection, const std::string &collection,
                                      const std::string &id, mongocxx::options::find &find_opts);


    virtual mongo_response find(mongo_connection *connection, const std::string &collection,
                                const std::string &filter, mongocxx::options::find &find_opts);

    virtual mongo_response exists(mongo_connection *connection, const std::string &collection,
                                  const std::string &query, mongocxx::options::find &find_opts);

    virtual mongo_response count(mongo_connection *connection, const std::string &db_name,
                                 const std::string &collection, const std::string &filter,
                                 mongocxx::options::count &count_opts);

    virtual mongo_response insert(mongo_connection *connection, const std::string &collection,
                                  const std::string &json_data, mongocxx::options::insert &insert_opts);

    virtual mongo_response update_by_id(mongo_connection *connection, const std::string &collection,
                                        const std::string &id, const std::string &json_data,
                                        mongocxx::options::update &update_opts);

    virtual mongo_response update_by_query(mongo_connection *connection, const std::string &collection,
                                           const std::string &json_query, const std::string &json_data,
                                           mongocxx::options::update &update_opts);

    virtual mongo_response delete_by_id(mongo_connection *connection, const std::string &collection,
                                        const std::string &id,
                                        mongocxx::options::delete_options &delete_options);

    virtual mongo_response delete_by_query(mongo_connection *connection, const std::string &collection,
                                           const std::string &filter,
                                           mongocxx::options::delete_options &delete_options);

    virtual mongo_response set_read_concern(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection, mongocxx::read_concern &rc);

    virtual mongo_response set_write_concern(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection, mongocxx::write_concern &wc);

    virtual mongo_response drop_db(mongo_connection *connection, const std::string &db_name);

    virtual mongo_response drop_collection(mongo_connection *connection, const std::string &db_name,
                                           const std::string &collection);

    virtual mongo_response create_collection(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection_name,
                                             mongocxx::options::create_collection &options);

    virtual mongo_response rename_collection(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection, const std::string &new_name);

    virtual mongo_response run_command(mongo_connection *connection, const std::string &db_name,
                                       const std::string &command);

private:
    mongo_response build_mongo_response(mongocxx::cursor &cursor);
};

#endif //MONGO_API_H