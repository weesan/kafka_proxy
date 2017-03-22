#include "kafka_db.h"
#include "json.hpp"

using Json = nlohmann::json;

KafkaDB::KafkaDB (const string &topic, const string &gid) :
    _db(new Db(NULL, 0)),
    _dbc(NULL),
    _topic(topic),
    _gid(gid)
{
    if (!_db) {
        fprintf(stderr, "Failed to init a bdb.");
        exit(-1);
    }

    if (_db->open(NULL,
                  db_file().c_str(),
                  NULL, DB_BTREE, DB_CREATE|DB_THREAD, 0)) {
        DB_ERROR("Failed to call open()");
        exit(-1);
    }

    fprintf(stderr, "Opened %s\n", db_file().c_str());
}

KafkaDB::~KafkaDB (void)
{
    // Using boost::shared_ptr, so no need to free here.
    /*
    if (!_db) {
        return;
    }

    delete _db;
    _db = NULL;
    */
    
    fprintf(stderr, "Closed %s\n", db_file().c_str());
}

void KafkaDB::put (int32_t partition, int64_t offset, void *data, size_t len)
{
    Key key(partition, offset);
    Value value(data, len);

    fprintf(stderr,
            "Adding partition %d offset %d value %.*s to the DB.\n",
            partition, offset, len, (char *)data);
    if (_db->put(NULL, &key, &value, 0)) {
        DB_ERROR("Failed to call put()");
        exit(-1);
    }
}

const KafkaDB::Value KafkaDB::get (int32_t partition, int64_t offset)
{
    Key key(partition, offset);
    Value value;

    if (_db->get(NULL, &key, &value, 0)) {
        DB_ERROR("Failed to call get()");
    }

    printf("Looked up %s key %d + %lu and found %.*s\n",
           db_file().c_str(), partition, offset,
           value.size(), value.data()); 

    return value;
}

const char *KafkaDB::get (int n, string &response) {
    lock_guard<mutex> lock(_mutex);

    if (!_dbc) {
        if (_db->cursor(NULL, &_dbc, 0)) {
            DB_ERROR("Failed to call cursor()");
            exit(-1);
        } 
    }

    Json json;
    for (int i = 0; i < n; i++) {
        Key key;
        Value value;

        if (_dbc->get(&key, &value, DB_NEXT)) {
            // End of the cursor.
            _dbc->close();
            _dbc = NULL;
            break;
        }

        Json j;
        j["partition"] = key.partition();
        j["offset"] = key.offset();
        char data[1024];
        snprintf(data, sizeof(data), "%.*s", value.size(), value.data());
        j["message"] = data;

        json.push_back(j);
    }
        
    // Gather the result.
    response += json.dump();

    return response.c_str();
}

bool KafkaDB::del (int32_t partition, int64_t offset)
{
    Key key(partition, offset);

    try {
        if (_db->del(NULL, &key, 0)) {
            DB_ERROR("Failed to call del()");
        }
    } catch(exception &e) {
        DB_ERROR(e.what());
        return false;
    }
    
    return true;
}

void KafkaDB::dump(void) {
    printf("Dump ...\n");
    Dbc *dbc = NULL;
    if (_db->cursor(NULL, &dbc, 0)) {
        DB_ERROR("Failed to call cursor()");
        exit(-1);
    }

    int i = 0;
    while (true) {
        Key key;
        Value value;
            
        if (dbc->get(&key, &value, DB_NEXT)) {
            break;
        }
        printf("%d: key %d + %d value %.*s\n",
               ++i,
               key.partition(), key.offset(),
               value.size(), value.data());
    }

    if (dbc->close()) {
        DB_ERROR("Failed to call close()");
        exit(-1);
    }
}
