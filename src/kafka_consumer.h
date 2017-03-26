#ifndef KAFKA_CONSUMER_H
#define KAFKA_CONSUMER_H

#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <unordered_map>
#include <boost/smart_ptr.hpp>
#include "kafka_db.h"

#define KAFKA_CONSUMER_TIMEOUT    1000

using namespace std;

class KafkaConsumer : public KafkaDB {
private:
    const string _brokers;
    boost::shared_ptr<RdKafka::KafkaConsumer> _consumer;

private:
    void process(RdKafka::Message *message);
    
public:
    KafkaConsumer(const string &brokers,
                  const string &topic, const string &gid,
                  const string &db_path = "");
    ~KafkaConsumer(void);
    void shutdown(void);
    void consume(int timeout = KAFKA_CONSUMER_TIMEOUT);
};

class KafkaConsumers : public unordered_map<string, KafkaConsumer *> {
private:
    string _brokers;
    string _db_path;
    
private:
    void scandir(const char *path);
    bool parse_db_name(const char *db_name, string &topic, string &gid);

public:
    KafkaConsumers(void);
    ~KafkaConsumers(void);
    void setBrokers(const string &brokers) {
        _brokers = brokers;
    }
    void setDBPath(const string &db_path);
    void shutdown(void);
    KafkaConsumer &operator()(const string &topic, const string &gid);
};

extern KafkaConsumers kafka;

#endif // KAFKA_CONSUMER_H
