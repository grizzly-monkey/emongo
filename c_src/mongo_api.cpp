//
// Created by Jeet Parmar on 2019-01-01.
//

#include "mongo_api.h"

mongo_response mongo_api::set_read_concern(mongo_connection *connection, const std::string &db_name,
                                           const std::string &collection, mongocxx::read_concern &rc) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        std::string old_read_concern = std::string(db[collection].read_concern().acknowledge_string());

        db[collection].read_concern(rc);

        std::string new_read_concern = std::string(db[collection].read_concern().acknowledge_string());

        std::string response = R"({"old_read_concern": ")" + old_read_concern
                               + R"(", "new_read_concern": ")" + new_read_concern + "\"}";

        return mongo_response{false, response};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::set_write_concern(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection, mongocxx::write_concern &wc) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        db[collection].write_concern(wc);

        return mongo_response{false, "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::drop_db(mongo_connection *connection, const std::string &db_name) {
    try {

        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);
        db.drop();

        return mongo_response{false, "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::run_command(mongo_connection *connection, const std::string &db_name,
                                      const std::string &command) {
    try {

        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);
        bsoncxx::document::value response = db.run_command(bsoncxx::from_json(command));

        return mongo_response{false, bsoncxx::to_json(response)};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::drop_collection(mongo_connection *connection, const std::string &db_name,
                                          const std::string &collection) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        db[collection].drop();
        return mongo_response{false, "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::create_collection(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection_name,
                                            mongocxx::options::create_collection &options) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        auto collection = db.create_collection(collection_name, options);

        return mongo_response{false, collection ? "true" : "false"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::rename_collection(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection, const std::string &new_name) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        db[collection].rename(new_name);
        return mongo_response{false, "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::list_databases(mongo_connection *connection) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto cursor = client->list_databases();

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::list_collections(mongo_connection *connection, const std::string &db_name) {

    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);
        auto cursor = db.list_collections({});

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::collection_exists(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);
        auto exist = db.has_collection(collection);

        return mongo_response{false, exist ? "true" : "false"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::find_all(mongo_connection *connection, const std::string &collection,
                                   mongocxx::options::find &find_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        auto cursor = db[collection].find({}, find_opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::find_by_id(mongo_connection *connection, const std::string &collection,
                                     const std::string &id, mongocxx::options::find &find_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::stdx::optional<bsoncxx::document::value> maybe_result =
                db[collection].find_one(document{} << "_id" << bsoncxx::oid(id) << finalize, find_opts);

        if (maybe_result) {
            auto view = maybe_result->view();
            std::string result = bsoncxx::to_json(view);
            return mongo_response{false, result};
        }

        return mongo_response{false, "{}"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::find(mongo_connection *connection, const std::string &collection,
                               const std::string &filter, mongocxx::options::find &find_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        auto cursor = db[collection].find(bsoncxx::from_json(filter), find_opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::exists(mongo_connection *connection, const std::string &collection,
                                 const std::string &query, mongocxx::options::find &find_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        auto cursor = db[collection].find(bsoncxx::from_json(query), find_opts);

        return mongo_response{false, cursor.begin() == cursor.end() ? "false" : "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::count(mongo_connection *connection, const std::string &db_name,
                                const std::string &collection, const std::string &filter,
                                mongocxx::options::count &count_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        auto count = db[collection].count_documents(bsoncxx::from_json(filter), count_opts);

        return mongo_response{false, std::to_string(count)};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::insert(mongo_connection *connection, const std::string &collection,
                                 const std::string &json_data, mongocxx::options::insert &insert_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        if (json_data.at(0) == '{') {
            mongocxx::stdx::optional<mongocxx::result::insert_one> result = db[collection].insert_one(
                    bsoncxx::from_json(json_data), insert_opts);

            return mongo_response{false, result ? "true" : "false"};

        } else {
            std::vector<bsoncxx::document::view> documents_to_insert{};
            auto bson_docs = bsoncxx::from_json(json_data);
            for (auto &&element : bson_docs.view()) {
                documents_to_insert.emplace_back(element.get_document().value);
            }

            mongocxx::stdx::optional<mongocxx::result::insert_many> result = db[collection].insert_many(
                    documents_to_insert, insert_opts);

            std::string count = std::to_string(result->inserted_count());
            return mongo_response{false, result ? count + " documents inserted."
                                                : "insert failed"};
        }

    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::update_by_id(mongo_connection *connection, const std::string &collection,
                                       const std::string &id, const std::string &json_data,
                                       mongocxx::options::update &update_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::stdx::optional<mongocxx::result::update> result = db[collection].update_one(
                document{} << "_id" << bsoncxx::oid(id) << finalize,
                bsoncxx::from_json(json_data), update_opts);

        std::string matched_count = std::to_string(result->matched_count());
        std::string modified_count = std::to_string(result->modified_count());

        return mongo_response{false, result ? matched_count + " matched and " + modified_count
                                              + " documents updated."
                                            : "update by id failed"};

    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::update_by_query(mongo_connection *connection, const std::string &collection,
                                          const std::string &json_query, const std::string &json_data,
                                          mongocxx::options::update &update_opts) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::stdx::optional<mongocxx::result::update> result = db[collection].update_many(
                bsoncxx::from_json(json_query),
                bsoncxx::from_json(json_data), update_opts);

        std::string matched_count = std::to_string(result->matched_count());
        std::string modified_count = std::to_string(result->modified_count());

        return mongo_response{false, result ? matched_count + " matched and " + modified_count
                                              + " documents updated."
                                            : "update by query failed"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::delete_by_id(mongo_connection *connection, const std::string &collection,
                                       const std::string &id, mongocxx::options::delete_options &delete_options) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());


        mongocxx::stdx::optional<mongocxx::result::delete_result> result = db[collection].delete_one(
                document{} << "_id" << bsoncxx::oid(id) << finalize, delete_options);

        std::string deleted_count = std::to_string(result->deleted_count());

        return mongo_response{false, result ? deleted_count + " documents deleted."
                                            : "delete by id failed"};

    }
    MONGOCATCH(__FUNCTION__, __LINE__)

}

mongo_response mongo_api::delete_by_query(mongo_connection *connection, const std::string &collection,
                                          const std::string &filter,
                                          mongocxx::options::delete_options &delete_options) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());


        mongocxx::stdx::optional<mongocxx::result::delete_result> result = db[collection].delete_many(
                bsoncxx::from_json(filter), delete_options);

        std::string deleted_count = std::to_string(result->deleted_count());

        return mongo_response{false, result ? deleted_count + " documents deleted."
                                            : "delete by query failed"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::build_mongo_response(mongocxx::cursor &cursor) {
    std::string result;

    if (cursor.begin() == cursor.end()) {
        result.append("[]");
        return mongo_response{false, ""};
    }

    result.append("[");
    for (auto &&doc : cursor) {
        result.append(bsoncxx::to_json(doc));
        result.append(",");
    }
    result.pop_back();
    result.append("]");
    return mongo_response{false, result};
}
