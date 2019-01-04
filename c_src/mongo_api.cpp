//
// Created by Jeet Parmar on 2019-01-01.
//

#include "mongo_api.h"

mongocxx::write_concern::level get_level_from_string(const std::string &level) {
    if (level == "majority") {
        return mongocxx::write_concern::level::k_majority;
    }
    if (level == "acknowledged") {
        return mongocxx::write_concern::level::k_acknowledged;
    }
    if (level == "tag") {
        return mongocxx::write_concern::level::k_tag;
    }
    if (level == "unacknowledged") {
        return mongocxx::write_concern::level::k_unacknowledged;
    }
    return mongocxx::write_concern::level::k_default;
}

mongo_response mongo_api::set_read_concern(mongo_connection *connection, const std::string &db_name,
                                           const std::string &collection, const std::string &read_concern) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);
        mongocxx::read_concern rc{};
        rc.acknowledge_string(read_concern);

        std::string old_read_concern = std::string(db[collection].read_concern().acknowledge_string());

        db[collection].read_concern(rc);

        std::string new_read_concern = std::string(db[collection].read_concern().acknowledge_string());

        std::string response = R"({"old_read_concern": ")" + read_concern
                               + R"(", "new_read_concern": ")" + new_read_concern + "\"}";

        return mongo_response{false, response};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::set_write_concern(mongo_connection *connection, const std::string &db_name,
                                            const std::string &collection, const bool &journal,
                                            const bool &majority_time, const int &nodes, const std::string &level) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        mongocxx::write_concern wc{};
        wc.journal(journal);
        wc.majority(std::chrono::milliseconds(majority_time));
        wc.nodes(nodes);

        wc.acknowledge_level(get_level_from_string(level));

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
                                            const std::string &collection_name, const std::string &validation_json) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        mongocxx::validation_criteria valid_criteria{};
        valid_criteria.rule(bsoncxx::from_json(validation_json));
        valid_criteria.level(mongocxx::validation_criteria::validation_level::k_moderate);
        valid_criteria.action(mongocxx::validation_criteria::validation_action::k_error);

        mongocxx::options::create_collection opts{};
        opts.validation_criteria(valid_criteria);
        opts.size(10);
        opts.storage_engine(bsoncxx::from_json(validation_json));
        opts.capped(true);
        opts.no_padding();
        opts.max();
        opts.collation();

        auto collection = db.create_collection(collection_name, opts);

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

mongo_response mongo_api::find_all(mongo_connection *connection, const std::string &collection) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts();

        auto cursor = db[collection].find({}, opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::find_all(mongo_connection *connection, const std::string &collection,
                                   const std::string &sort_key, int &order) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts(DEFAULT_QUERY_LIMIT, sort_key, order);

        auto cursor = db[collection].find({}, opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::find_by_id(mongo_connection *connection, const std::string &collection,
                                     const std::string &id) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts(1);


        mongocxx::stdx::optional<bsoncxx::document::value> maybe_result =
                db[collection].find_one(document{} << "_id" << bsoncxx::oid(id) << finalize, opts);

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
                               const std::string &filter) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts();

        auto cursor = db[collection].find(bsoncxx::from_json(filter), opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::find(mongo_connection *connection, const std::string &collection,
                               const std::string &filter, const std::string &sort_key, int &order) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts(DEFAULT_QUERY_LIMIT, sort_key, order);

        auto cursor = db[collection].find(bsoncxx::from_json(filter), opts);

        return build_mongo_response(cursor);
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::exists(mongo_connection *connection, const std::string &collection,
                                 const std::string &query) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::find opts = build_find_opts(1);

        auto cursor = db[collection].find(bsoncxx::from_json(query), opts);

        return mongo_response{false, cursor.begin() == cursor.end() ? "false" : "true"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::count(mongo_connection *connection, const std::string &db_name,
                                const std::string &collection, const std::string &filter) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(db_name);

        auto count = db[collection].count_documents(bsoncxx::from_json(filter));

        return mongo_response{false, std::to_string(count)};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}


mongo_response mongo_api::insert(mongo_connection *connection, const std::string &collection,
                                 const std::string &json_data) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::insert opts{};
        opts.ordered(false);
        opts.bypass_document_validation(true);

        if (json_data.at(0) == '{') {
            mongocxx::stdx::optional<mongocxx::result::insert_one> result = db[collection].insert_one(
                    bsoncxx::from_json(json_data), opts);

            return mongo_response{false, result ? "true" : "false"};

        } else {
            std::vector<bsoncxx::document::view> documents_to_insert{};
            auto bson_docs = bsoncxx::from_json(json_data);
            for (auto &&element : bson_docs.view()) {
                documents_to_insert.emplace_back(element.get_document().value);
            }

            mongocxx::stdx::optional<mongocxx::result::insert_many> result = db[collection].insert_many(
                    documents_to_insert, opts);

            std::string count = std::to_string(result->inserted_count());
            return mongo_response{false, result ? count + " documents inserted."
                                                : "insert failed"};
        }

    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::update_by_id(mongo_connection *connection, const std::string &collection,
                                       const std::string &id, const std::string &json_data) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::update opts{};
        opts.upsert(false);
        opts.bypass_document_validation(true);

        mongocxx::stdx::optional<mongocxx::result::update> result = db[collection].update_one(
                document{} << "_id" << bsoncxx::oid(id) << finalize,
                bsoncxx::from_json(json_data), opts);

        std::string matched_count = std::to_string(result->matched_count());
        std::string modified_count = std::to_string(result->modified_count());

        return mongo_response{false, result ? matched_count + " matched and " + modified_count
                                              + " documents updated."
                                            : "update by id failed"};

    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::update_by_query(mongo_connection *connection, const std::string &collection,
                                          const std::string &json_query, const std::string &json_data) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());

        mongocxx::options::update opts{};
        opts.upsert(false);
        opts.bypass_document_validation(true);

        mongocxx::stdx::optional<mongocxx::result::update> result = db[collection].update_many(
                bsoncxx::from_json(json_query),
                bsoncxx::from_json(json_data), opts);

        std::string matched_count = std::to_string(result->matched_count());
        std::string modified_count = std::to_string(result->modified_count());

        return mongo_response{false, result ? matched_count + " matched and " + modified_count
                                              + " documents updated."
                                            : "update by query failed"};
    }
    MONGOCATCH(__FUNCTION__, __LINE__)
}

mongo_response mongo_api::delete_by_id(mongo_connection *connection, const std::string &collection,
                                       const std::string &id) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());


        mongocxx::stdx::optional<mongocxx::result::delete_result> result = db[collection].delete_one(
                document{} << "_id" << bsoncxx::oid(id) << finalize);

        std::string deleted_count = std::to_string(result->deleted_count());

        return mongo_response{false, result ? deleted_count + " documents deleted."
                                            : "delete by id failed"};

    }
    MONGOCATCH(__FUNCTION__, __LINE__)

}

mongo_response mongo_api::delete_by_query(mongo_connection *connection, const std::string &collection,
                                          const std::string &filter) {
    try {
        auto client = connection->get_connection_pool()->acquire();
        auto db = client->database(connection->get_db());


        mongocxx::stdx::optional<mongocxx::result::delete_result> result = db[collection].delete_many(
                bsoncxx::from_json(filter));

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

mongocxx::options::find mongo_api::build_find_opts(int limit, std::string sort_key, int order) {
    mongocxx::options::find opts{};
    opts.batch_size(DEFAULT_BATCH_SIZE);
    opts.limit(limit);
    opts.max_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    opts.no_cursor_timeout(true);
    opts.max_await_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));

    if (sort_key.empty()) {
        return opts;
    }
    opts.sort(make_document(kvp(sort_key, order)));
    return opts;
}


mongocxx::options::find mongo_api::build_find_opts(int limit) {
    mongocxx::options::find opts{};
    opts.batch_size(DEFAULT_BATCH_SIZE);
    opts.limit(limit);
    opts.max_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    opts.no_cursor_timeout(true);
    opts.max_await_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    return opts;
}


mongocxx::options::find mongo_api::build_find_opts() {
    mongocxx::options::find opts{};
    opts.batch_size(DEFAULT_BATCH_SIZE);
    opts.limit(DEFAULT_QUERY_LIMIT);
    opts.max_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    opts.no_cursor_timeout(true);
    opts.max_await_time(std::chrono::milliseconds(DEFAULT_EXEC_TIME));
    return opts;
}
