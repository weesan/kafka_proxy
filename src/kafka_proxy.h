#ifndef KAFKA_PROXY_H
#define KAFKA_PROXY_H

#include <boost/bind.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>

#define KAFKA_PROXY_BUF_SIZE  1024

using boost::asio::ip::tcp;

typedef boost::shared_ptr<tcp::socket> socket_ptr;

class KafkaProxy : public boost::asio::io_service {
private:
    int _port;

private:
    static void session(socket_ptr sock);
    static void timer_cb(void);
    
public:
    KafkaProxy(int port);
    void run(void);
};

#endif // KAFKA_PROXY_H
