#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <dirent.h>
#include "kafka_consumer.h"

#define KAFKA_BROKERS_DEFAULT    "localhost:9092"

/*
 * Global variables.
 */
KafkaConsumers kafka;

/*
 * Static functions.
 */
static void sigterm (int sig)
{
    kafka.shutdown();
    exit(0);
}

/*
 * Local classes.
 */

/*
 * Callback class when partition re-balance is needed.  For example,
 * when a new consumer belonging to a consumder group subscribes to a
 * topic, the partitions of the topic will be reshuffled among the
 * consumers in the same consumer group.  Another case is when the #
 * of partitions of an existing topic has been increased, the
 * partitions of the topic will be reshuffled among existing consumers
 * in the same consumer group.
 */
class MyRebalanceCb : public RdKafka::RebalanceCb {
public:
    void rebalance_cb (RdKafka::KafkaConsumer *consumer,
                       RdKafka::ErrorCode err,
                       vector<RdKafka::TopicPartition*> &partitions) {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            fprintf(stderr, "Assign %d partitions: ", partitions.size());
            for (int i = 0; i < partitions.size(); i++) {
                fprintf(stderr, "%d ", partitions[i]->partition());
            }
            if (partitions.size() == 0) {
                printf("none\n");
            } else {
                printf("\n");
            }

            // XXX
            if (partitions.size() != 0) {
                //partitions[0]->set_offset(0);
            }

            consumer->assign(partitions);
        } else {
            fprintf(stderr, "Unassign %d partitions: ", partitions.size());

            for (int i = 0; i < partitions.size(); i++) {
                fprintf(stderr, "%d ", partitions[i]->partition());
            }
            if (partitions.size() == 0) {
                printf("none\n");
            } else {
                printf("\n");
            }
            consumer->unassign();
        }
    }
};

/*
 * Event callback class.
 */
class MyEventCb : public RdKafka::EventCb {
public:
    void event_cb (RdKafka::Event &event) {
        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            fprintf(stderr, "Error: %s: %s",
                    RdKafka::err2str(event.err()).c_str(), event.str().c_str());
            //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) {
            //    run = false;
            //}
            break;
        case RdKafka::Event::EVENT_STATS:
            fprintf(stderr, "STATS: %s\n", event.str().c_str());
            break;
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                    event.severity(), event.fac().c_str(), event.str().c_str());
            break;
        case RdKafka::Event::EVENT_THROTTLE:
            fprintf(stderr, "THROTTLED: %dms by %s id %d\n",
                    event.throttle_time(), 
                    event.broker_name().c_str(),
                    (int)event.broker_id());
            break;
        default:
            fprintf(stderr, "EVENT %s (%s): %s\n",
                    event.type(),
                    err2str(event.err()).c_str(),
                    event.str().c_str());
            break;
        }
    }
};

/*
 * Offset commit callback class.
 */
class MyOffsetCommitCb : public RdKafka::OffsetCommitCb {
public:
    void offset_commit_cb (RdKafka::ErrorCode err,
                           std::vector<RdKafka::TopicPartition*> &offsets) {
        fprintf(stderr, "Offset size %d\n", offsets.size());
        
        /* No offsets to commit, dont report anything. */
        if (err == RdKafka::ERR__NO_OFFSET) {
            return;
        }

        for (unsigned int i = 0 ; i < offsets.size() ; i++) {
            fprintf(stderr, "Committed topic %s partition %d offset %d\n",
                    offsets[i]->topic().c_str(),
                    offsets[i]->partition(),
                    offsets[i]->offset());
        }
    }
};

/*
 * KafkaConsumer class
 */

KafkaConsumer::KafkaConsumer (const string &brokers,
                              const string &topic, const string &gid,
                              const string &db_path) :
    KafkaDB(topic, gid, db_path),
    _brokers(brokers.empty() ? KAFKA_BROKERS_DEFAULT : brokers),
    _consumer(NULL)
{
    string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!conf) {
        fprintf(stderr, "Failed to create kafka conf.\n");
        exit(-1);
    }

    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    // For new topics, we always consume from the beginning.
    if (tconf->set("auto.offset.reset", "smallest", errstr) !=
        RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to setup auto.offset.reset: %s\n",
                errstr.c_str());
        exit(-1);
    }

    // Setup the topic config.
    conf->set("default_topic_conf", tconf, errstr);
    delete tconf;

    // Set the brokers.
    if (conf->set("metadata.broker.list", _brokers, errstr)
        != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to set the brokers: %s\n", _brokers.c_str());
        exit(-1);
    }

    static MyRebalanceCb myRebalanceCb;
    if (conf->set("rebalance_cb", &myRebalanceCb, errstr)
        != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to setup rebalance_cb\n");
        exit(-1);
    }

    static MyEventCb myEventCb;
    if (conf->set("event_cb", &myEventCb, errstr)
        != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to setup event_cb\n");
        exit(-1);
    }

    static MyOffsetCommitCb myOffsetCommitCb;
    if (conf->set("offset_commit_cb", &myOffsetCommitCb, errstr)
        != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to setup event_cb\n");
        exit(-1);
    }
    
    // Set the consumer group id.
    if (conf->set("group.id", gid, errstr) !=
        RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "Failed to create group id: %s\n", errstr.c_str());
        exit(-1);
    }

    // Create a consumer.
    _consumer.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    if (!_consumer) {
        fprintf(stderr, "Error: %s\n", errstr.c_str());
        exit(-1);
    }

    delete conf;

    fprintf(stderr, "Created a kafka consumer group \"%s\", brokers \"%s\"\n",
            gid.c_str(), _brokers.c_str());
    
    // Subscribe to a topic.
    vector<string> topics;
    topics.push_back(topic);
    RdKafka::ErrorCode ret = _consumer->subscribe(topics);
    if (ret) {
        fprintf(stderr, "Failed to subscribe to topic: %s\n",
                RdKafka::err2str(ret).c_str());
        exit(-1);
    }

    fprintf(stderr, "Subscribed to topic %s\n", topic.c_str());
}

KafkaConsumer::~KafkaConsumer (void) {
    shutdown();
}

void KafkaConsumer::shutdown (void) {
    if (_consumer) {
        _consumer->close();
        // Using shared_ptr, no need to free here.
        /*
        delete _consumer;
        _consumer = NULL;
        */

        /*
         * Wait for RdKafka to decommission.
         * This is not strictly needed (with check outq_len() above), but
         * allows RdKafka to clean up all its resources before the application
         * exits so that memory profilers such as valgrind wont complain about
         * memory leaks.
         */
        RdKafka::wait_destroyed(5000);
    }

    fprintf(stderr, "Shutdown a Kafka consumer.\n");
}

void KafkaConsumer::process (RdKafka::Message *message)
{
    switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
        //fprintf(stderr, "Timeout!\n");
        break;
    case RdKafka::ERR_NO_ERROR:
        /* Real message */
        /*
        printf("Partition %d offset %d len %d key %s value %.*s\n",
               message->partition(), message->offset(), message->len(),
               message->key() ? message->key()->c_str() : "null",
               message->len(), static_cast<const char *>(message->payload()));
        */
        put(message->partition(), message->offset(),
            message->payload(), message->len());
        break;
    case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        fprintf(stderr, "(%s, %s): Partition %d ends\n",
                topic().c_str(), gid().c_str(), message->partition());
        break;
    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
        fprintf(stderr, "Unknown topic/partition: %s\n",
                message->errstr().c_str());
        break;
    default:
        /* Errors */
        fprintf(stderr, "Failed to consume: %s\n", message->errstr().c_str());
        break;
    }
}

void KafkaConsumer::consume (int timeout) {
    if (!_consumer) {
        return;
    }
        
    RdKafka::Message *msg = _consumer->consume(timeout);
    process(msg);
    delete msg;
}

/*
 * KafkaConsumers class
 */

KafkaConsumers::KafkaConsumers (void) {
    signal(SIGINT,  sigterm);
    signal(SIGTERM, sigterm);
    signal(SIGQUIT, sigterm);
}

KafkaConsumers::~KafkaConsumers (void) {
    shutdown();
}

bool KafkaConsumers::parse_db_name (const char *db_name,
                                    string &topic, string &gid)
{
    char kafka_str[256] = {0};
    char topic_str[256] = {0};
    char gid_str[256]   = {0};
    char db_str[256]    = {0};
    
    int n = sscanf(db_name, "%[^\\-]-%[^\\-]-%[^\\.].%s",
                   kafka_str, topic_str, gid_str, db_str);
    if (n == 4 &&
        strcmp(kafka_str, "kafka") == 0 &&
        strcmp(db_str, "db") == 0) {
        // Ok.
        topic = topic_str;
        gid = gid_str;
        return true;
    } else {
        // Not ok.
        return false;
    }
}

void KafkaConsumers::scandir(const char *path)
{
    struct dirent **namelist = NULL;
    int n = ::scandir(path, &namelist, NULL, alphasort);
    if (n < 0) {
        fprintf(stderr, "Warning: failed to scandir(%s)\n", path);
    } else {
        while (n--) {
            string topic, gid;
            
            if (parse_db_name(namelist[n]->d_name, topic, gid)) {
                fprintf(stderr, "Loading existing db %s/%s\n",
                        path, namelist[n]->d_name);
                // Subsribe topic and create consumer group.
                (*this)(topic, gid);
            }
            free(namelist[n]);
        }
        free(namelist);
    }
}

void KafkaConsumers::setDBPath(const string &db_path) {
    _db_path = db_path;

    scandir(_db_path.c_str());
}

KafkaConsumer &KafkaConsumers::operator() (const string &topic,
                                           const string &gid)
{
    auto itr = find(topic + gid);
    if (itr != end()) {
        return *itr->second;
    } else {
        KafkaConsumer *consumer =
            new KafkaConsumer(_brokers, topic, gid, _db_path);
        if (!consumer) {
            throw "Out of memory";
        }
        (*this)[topic + gid] = consumer;
        return *consumer;
    }
}

void KafkaConsumers::shutdown (void) {
    // Free up the memory first.
    for (auto itr = begin(); itr != end(); ++itr) {
        delete itr->second;
    }

    // Clear the map.
    clear();
}
