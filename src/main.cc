#include <unistd.h>
#include "kafka_proxy.h"
#include "kafka_consumer.h"

static void usage (const char *program)
{
    fprintf(stderr,
            "\n"
            "Usage: %s [-h] [-b brokers] [-p port#]\n"
            "\n"
            "For example:\n"
            "  %s -b localhost:9092 -p 8888\n"
            "\n",
            program, program);
}

int main (int argc, char* argv[])
{
    int opt;
    int port = 0;
    const char *program = argv[0];
    const char *brokers = NULL;

    while ((opt = getopt(argc, argv, "b:hp:")) != EOF) {
        switch (opt) {
        case 'b':
            brokers = optarg;
            kafka.setBrokers(brokers);
            break;
        case 'p':
            if (sscanf(optarg, "%d", &port) != 1) {
                fprintf(stderr, "Incorrect port number.\n");
                exit(1);
            }
            break;
        case 'h':
        default:
            usage(program);
            exit(1);
        }
    }

    argc -= optind;
    argv += optind;

    /*
    kafka("foo", "bar").dump();
    for (int i = 0; i < 4; i++) {
        KafkaDB::Value value = kafka("foo", "bar").get(0, i);
    }
    */

    try {
        KafkaProxy proxy(port, brokers);
        proxy.run();
    } catch (exception &e) {
        fprintf(stderr, "Exception: %s\n", e.what());
    }

    return 0;
}
