#include "erl_nif.h"

#include "mongocxx/exception/exception.hpp"
#include "mongocxx/exception/query_exception.hpp"
#include "mongocxx/exception/bulk_write_exception.hpp"
#include "mongocxx/exception/logic_error.hpp"
#include "mongocxx/exception/write_exception.hpp"

#define  MONGOCATCH(function, line)     catch(mongocxx::query_exception er)\
    {\
        return mongo_response{true , er.what()};\
    }\
    catch(mongocxx::bulk_write_exception er)\
    {\
        return mongo_response{true , er.what()};\
    }\
    catch(mongocxx::logic_error er)\
    {\
        return mongo_response{true , er.what()};\
    }\
    catch(mongocxx::write_exception er)\
    {\
        return mongo_response{true , er.what()};\
    }\
    catch(mongocxx::operation_exception er)\
    {\
       return mongo_response{true , er.what()};\
    }\
    catch(mongocxx::exception er)\
    {\
        return mongo_response{true , er.what()};\
    }\
    catch (...)\
    {\
       return mongo_response{true ," Unknown Exception"};\
    }\

