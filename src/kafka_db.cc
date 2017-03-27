#include <time.h>
#include <unordered_map>
#include "kafka_db.h"
#include "json.hpp"

#define KAFKA_OUTSTANDING_MSG_TTL    60 // in secs.

using Json = nlohmann::json;
using namespace std;

/*
 * Private classes and structs.
 */
struct KafkaKeyHasher {
    std::size_t operator()(const KafkaKey& k) const {
        return (hash<int32_t>()(k._partition) ^
                (hash<int64_t>()(k._offset) << 1));
    }
};

class OutstandingMsgs :
    public unordered_map<KafkaKey, time_t, KafkaKeyHasher> {

public:
    bool outstanding(int32_t partition, int64_t offset) {
        auto itr = find(KafkaKey(partition, offset));
        if (itr != end() &&
            time(NULL) - itr->second <= KAFKA_OUTSTANDING_MSG_TTL) {
            return true;
        }

        return false;
    }
    void add(int32_t partition, int64_t offset) {
        (*this)[KafkaKey(partition, offset)] = time(NULL);
    }
    void del(int32_t partition, int64_t offset) {
        erase(KafkaKey(partition, offset));
    }
};

/*
 * Static variables.
 */
static OutstandingMsgs outstanding_msgs;

/*
 * Constructor.
 */
KafkaDB::KafkaDB (const string &topic, const string &gid,
                  const string &db_path) :
    _db(new Db(NULL, 0)),
    _dbc(NULL),
    _topic(topic),
    _gid(gid),
    _db_path(db_path.empty() ? KAFKA_DB_PATH : db_path)
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

/*
 * Destructor.
 */
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

/*
 * Add key-value to the DB.  In this case, partition + offset is the
 * key and data is the value.
 */
void KafkaDB::put (int32_t partition, int64_t offset, void *data, size_t len)
{
    Key key(partition, offset);
    Value value(data, len);

    /*
    fprintf(stderr,
            "Adding partition %d offset %d value %.*s to the DB.\n",
            partition, offset, len, (char *)data);
    */
    if (_db->put(NULL, &key, &value, 0)) {
        DB_ERROR("Failed to call put()");
        exit(-1);
    }
}

/*
 * Retrieve a single key-value from the DB.
 */
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

/*
 * Retrieve a number of key-value's from the DB in json format.
 */
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

        // If the message is still outstanding (ie. not being ack'd by
        // the client, we will skip that.
        if (outstanding_msgs.outstanding(key.partition(), key.offset())) {
            continue;
        }
        
        // Construct the json response.
        Json j;
        j["partition"] = key.partition();
        j["offset"] = key.offset();
        char data[1024];
        snprintf(data, sizeof(data), "%.*s", value.size(), value.data());
        j["message"] = data;
        json.push_back(j);

        // Keep track of the outstanding messages.
        outstanding_msgs.add(key.partition(), key.offset());
    }
        
    // Gather the result.
    response += json.dump();

    return response.c_str();
}

/*
 * Delete a key (partition + offset) from the DB.
 */
bool KafkaDB::del (int32_t partition, int64_t offset)
{
    lock_guard<mutex> lock(_mutex);

    // Remove the outstadning message first.
    outstanding_msgs.del(partition, offset);

    // Delete the key from the DB.
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

/*
 * Dump all the key-value from the DB.
 */
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
