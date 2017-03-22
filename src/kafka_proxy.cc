/*
 * Source: http://www.boost.org/doc/libs/1_52_0/doc/html/boost_asio/example/echo/blocking_tcp_echo_server.cpp
 *
 * RESTful APIs:
 *
 * GET /_consume?gid=<id>&topic=<topic>&size=<n>
 * DELETE /_delete?gid=<id>&topic=<topic>&partition=<partition>&offset=<offset>
 *
 * For example:
 *
 * $ curl -s -XGET 'http://localhost:8888/_consume?gid=bar&topic=foo&size=3'
 * $ curl -s -XDELETE 'http://localhost:8888/_delete?gid=bar&topic=foo&partition=0&offset=2'
 */

#include <string>
#include "kafka_proxy.h"
#include "kafka_consumer.h"
#include "http_parser.h"

using namespace std;
//using namespace boost;
using boost::asio::ip::tcp;

/*
 * Static functions.
 */

static void process_get (socket_ptr sock, HttpRequest &request,
                         string &response)
{
    int size = atoi(request()["size"].c_str());

    string ep("/_consume?");
    int pos = request["query"].find(ep);

    if (pos == string::npos) {
        response = "HTTP/1.0 400 Bad Request\r\n\r\n";
        return;
    }

    const string &topic   = request()["topic"];
    const string &gid     = request()["gid"];
        
    // Construct the response.
    response = "HTTP/1.0 200 OK\r\n\r\n";
    kafka(topic, gid).get(size, response);
}

static void process_post (socket_ptr sock, HttpRequest &request,
                          string &response)
{
    response = "HTTP/1.0 200 OK\r\n\r\n";
    fprintf(stderr, "%s: request: [%s]\n",
            __FUNCTION__, request["method"].c_str());
}

static void process_delete (socket_ptr sock, HttpRequest &request,
                            string &response)
{
    string ep("/_delete?");
    int pos = request["query"].find(ep);

    if (pos == string::npos) {
        response = "HTTP/1.0 400 Bad Request\r\n\r\n";
        return;
    }

    const string &topic = request()["topic"];
    const string &gid   = request()["gid"];
    int32_t partition = atoi(request()["partition"].c_str());
    int64_t offset = atoll(request()["offset"].c_str());
    
    fprintf(stderr, "Deleting topic %s parition %d offset %lu\n",
            topic.c_str(), partition, offset);

    if (kafka(topic, gid).del(partition, offset)) {
        response = "HTTP/1.0 200 OK\r\n\r\n";
    } else {
        response = "HTTP/1.0 404 Not Found\r\n\r\n";
    }
}

/*
 * KafkaProxy class.
 */

KafkaProxy::KafkaProxy(int port, const char *brokers) :
    _port(port),
    _brokers(brokers) {
    // Start a timer thread.
    boost::thread(boost::bind(timer_cb));
}

/*
 * This function handles every new connection/request.
 */
void KafkaProxy::session (socket_ptr sock) {
    try {
        char req[KAFKA_PROXY_BUF_SIZE];
            
        boost::system::error_code error;
        int len = sock->read_some(boost::asio::buffer(req), error);
        if (error == boost::asio::error::eof) {
            return;
        } else if (error) {
            throw boost::system::system_error(error);
        }

        if (len >= KAFKA_PROXY_BUF_SIZE) {
            throw std::runtime_error("Request is too long");
        }

        HttpRequest request;
        if (!http_parser(req, len, request)) {
            throw std::runtime_error("HTTP parse error");
        }
        //request.dump();
        
        // Parse the request and get the size.
        const char *method = request["method"].c_str();
        string response;

        if (strcasecmp(method, "GET") == 0) {
            process_get(sock, request, response);
        } else if (strcasecmp(method, "POST") == 0) {
            process_post(sock, request, response);
        } else if (strcasecmp(method, "DELETE") == 0) {
            process_delete(sock, request, response);
        } else {
            response = "HTTP/1.0 200 OK\r\n\r\n";
        }
        
        boost::asio::write(*sock,
                           boost::asio::buffer(response));
    } catch (std::exception& e) {
        std::cerr << "Exception in thread: " << e.what() << "\n";
    }
}

/*
 * This timer callback, a different thread, will continue consuming
 * the kafka messages and store them into the database.
 */
void KafkaProxy::timer_cb (void)
{
    while (true) {
        if (!kafka.size()) {
            sleep(1);
        } else {
            for (auto itr = kafka.begin(); itr != kafka.end(); ++itr) {
                itr->second->consume();
            }
        }
    }
}

/*
 * This run function will accept RESTful APIs from the clients and
 * serve them.
 */
void KafkaProxy::run (void) {
    fprintf(stderr, "Started Kafka Proxy on port %d ...\n", _port);
    tcp::acceptor acceptor(*this, tcp::endpoint(tcp::v4(), _port));
    while (true) {
        socket_ptr new_sock(new tcp::socket(*this));
        acceptor.accept(*new_sock);
        boost::thread(boost::bind(session, new_sock));
    }
}