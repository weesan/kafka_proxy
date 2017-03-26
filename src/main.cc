#include <unistd.h>
#include "kafka_proxy.h"
#include "kafka_consumer.h"

static void usage (const char *program)
{
    fprintf(stderr,
            "\n"
            "Usage: %s [-h] [-b brokers] [-p port#] [-P path]\n"
            "\n"
            "For example:\n"
            "  %s -b localhost:9092 -p 8888 -P /tmp/kafka_proxy\n"
            "\n",
            program, program);
}

int main (int argc, char* argv[])
{
    int opt;
    int port = 0;
    const char *program = argv[0];
    const char *brokers = NULL;
    const char *db_path = NULL; 

    while ((opt = getopt(argc, argv, "b:hp:P:")) != EOF) {
        switch (opt) {
        case 'b':
            brokers = optarg;
            break;
        case 'p':
            if (sscanf(optarg, "%d", &port) != 1) {
                fprintf(stderr, "Incorrect port number.\n");
                exit(1);
            }
            break;
        case 'P':
            db_path = optarg;
            break;
        case 'h':
        default:
            usage(program);
            exit(1);
        }
    }

    argc -= optind;
    argv += optind;

    // Brokers need to be set before db_path; otherwise, the brokers
    // won't be taken effect when existing db files are scanned.
    if (brokers) {
        kafka.setBrokers(brokers);
    }

    if (db_path) {
        kafka.setDBPath(db_path);
    }
            
    try {
        KafkaProxy proxy(port);
        proxy.run();
    } catch (exception &e) {
        fprintf(stderr, "Exception: %s\n", e.what());
    }

    return 0;
}
