#ifndef MONGO_API_H
#define MONGO_API_H

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>


#include "mongo_response.h"
#include "mongo_connection.h"

#define DEFAULT_BATCH_SIZE 10000
#define DEFAULT_QUERY_LIMIT 50000

#define DEFAULT_EXEC_TIME 1000000

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

    virtual mongo_response find_all(mongo_connection *connection, const std::string &collection);

    virtual mongo_response find_all(mongo_connection *connection, const std::string &collection,
                                    const std::string &sort_key, int &order);


    virtual mongo_response find_by_id(mongo_connection *connection, const std::string &collection,
                                      const std::string &id);

    virtual mongo_response find(mongo_connection *connection, const std::string &collection,
                                const std::string &filter);

    virtual mongo_response find(mongo_connection *connection, const std::string &collection,
                                const std::string &filter, const std::string &sort_key, int &order);

    virtual mongo_response exists(mongo_connection *connection, const std::string &collection,
                                  const std::string &query);

    virtual mongo_response count(mongo_connection *connection, const std::string &db_name,
                                 const std::string &collection, const std::string &filter);

    virtual mongo_response insert(mongo_connection *connection, const std::string &collection,
                                  const std::string &json_data);

    virtual mongo_response update_by_id(mongo_connection *connection, const std::string &collection,
                                        const std::string &id, const std::string &json_data);

    virtual mongo_response update_by_query(mongo_connection *connection, const std::string &collection,
                                           const std::string &json_query, const std::string &json_data);

    virtual mongo_response delete_by_id(mongo_connection *connection, const std::string &collection,
                                        const std::string &id);

    virtual mongo_response delete_by_query(mongo_connection *connection, const std::string &collection,
                                           const std::string &filter);

    virtual mongo_response set_read_concern(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection, const std::string &read_concern);

    virtual mongo_response set_write_concern(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection, const bool &journal,
                                             const bool &majority_time, const int &nodes, const std::string &level);

    virtual mongo_response drop_db(mongo_connection *connection, const std::string &db_name);

    virtual mongo_response drop_collection(mongo_connection *connection, const std::string &db_name,
                                           const std::string &collection);

    virtual mongo_response create_collection(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection_name, const std::string &validation_json);

    virtual mongo_response rename_collection(mongo_connection *connection, const std::string &db_name,
                                             const std::string &collection, const std::string &new_name);

    virtual mongo_response run_command(mongo_connection *connection, const std::string &db_name,
                                       const std::string &command);

private:
    mongo_response build_mongo_response(mongocxx::cursor &cursor);

    //TODO allow opts map to be sent from erlang to build opts

    mongocxx::options::find build_find_opts(int limit, std::string sort_key, int order);

    mongocxx::options::find build_find_opts(int limit);

    mongocxx::options::find build_find_opts();
};

#endif //MONGO_API_H