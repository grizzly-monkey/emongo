#ifndef MONGO_RESPONSE_H
#define MONGO_RESPONSE_H

struct mongo_response {
    bool status; //true for error and false for success
    std::string result;
};

#endif
