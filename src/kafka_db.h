#ifndef KAFKA_DB_H
#define KAFKA_DB_H

#include <db_cxx.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <mutex>
#include <boost/smart_ptr.hpp>

#define KAFKA_DB_PATH  "/dev/shm/"
#define DB_ERROR(msg)  _db->err(errno, "%s:%d - %s", __FILE__, __LINE__, msg)

using namespace std;

class KafkaDB {
private:
    boost::shared_ptr<Db> _db;
    Dbc *_dbc;
    mutex _mutex;
    string _topic;
    string _gid;

public:
    class Key : public Dbt {
    private:
        // _partition has to be the first field.
        int32_t _partition;
        int64_t _offset;

    public:
        Key(int32_t partition = 0, int64_t offset = 0) {
            memset(this, 0, sizeof(Key));
            _partition = partition;
            _offset = offset;
            set_data(&_partition);
            set_size(sizeof(Key) - sizeof(Dbt));
            set_flags(DB_DBT_USERMEM);
            set_ulen(sizeof(Key) - sizeof(Dbt));
        }
        int32_t partition(void) const {
            return _partition;
        }
        int64_t offset(void) const {
            return _offset;
        }
    };

    // In the case when we know the size of the value, we should
    // provide a buffer (from the stack) to avoid memory allocation.
    class Value : public Dbt {
    public:
        Value(void *data = NULL, size_t len = 0) :
            Dbt(data, len) {
            if (data) {
                // The memory is managed by the caller.
                set_flags(DB_DBT_USERMEM);
                set_ulen(len);
            } else {
                // The memory is managed by the Value class.
                set_flags(DB_DBT_MALLOC);
            }
        }
        virtual ~Value(void) {
            if (get_flags() == DB_DBT_MALLOC) {
                //printf("Freeing %d bytes\n", size());
                free(get_data());
            }
        }
        // Return as const char * for convenience.
        const char *data(void) const {
            return static_cast<const char *>(get_data());
        }
        size_t size(void) const {
            return get_size();
        }
    };

public:
    KafkaDB(const string &topic, const string &gid);
    virtual ~KafkaDB(void);
    const string &topic(void) const {
        return _topic;
    }
    const string &gid(void) const {
        return _gid;
    }
    const string db_file(void) const {
        return string(KAFKA_DB_PATH) + "kafka-" + _topic + "-" + _gid + ".db";
    }
    void put(int32_t partition, int64_t offset, void *data, size_t len);
    const Value get(int32_t partition, int64_t offset);
    const char *get(int n, string &response);
    bool del(int32_t partition, int64_t offset);
    void dump(void);
};

#endif // KAFKA_DB_H
