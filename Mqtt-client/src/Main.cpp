#include <iostream>
#include <vector>
#include <memory>
#include <boost/asio.hpp>

class MqttClient : public std::enable_shared_from_this<MqttClient> {
public:
    MqttClient(boost::asio::io_context& ioContext, const std::string& brokerAddress, const std::string& port)
        : socket_(ioContext), resolver_(ioContext), brokerAddress_(brokerAddress), port_(port)
    {
    }

    void connect()
    {
        auto self = shared_from_this();
        boost::asio::ip::tcp::resolver::query query(brokerAddress_, port_);
        resolver_.async_resolve(query, [self](const boost::system::error_code& ec, boost::asio::ip::tcp::resolver::iterator endpointIterator) {
            if (!ec) {
                boost::asio::async_connect(self->socket_, endpointIterator, [self](const boost::system::error_code& connectEc, const boost::asio::ip::tcp::resolver::iterator& /*unused*/) {
                    if (!connectEc) {
                        std::cout << "Connected to the broker." << std::endl;
                        self->sendSubscribePacket();
                    }
                    else {
                        std::cerr << "Connection error: " << connectEc.message() << std::endl;
                    }
                    });
            }
            else {
                std::cerr << "Resolution error: " << ec.message() << std::endl;
            }
            });
    }

    void sendSubscribePacket()
    {
        std::shared_ptr<std::string> subscribe_message = std::make_shared<std::string>("\x82\x0c\x00\x01\x00\x04my/topic\x01");
        boost::asio::async_write(socket_, boost::asio::buffer(*subscribe_message),
            [this, subscribe_message](const boost::system::error_code& ec, std::size_t /*bytes_transferred*/) {
                if (!ec) {
                    std::cout << "Subscribed to topic." << std::endl;
                }
                else {
                    std::cerr << "Subscribe error: " << ec.message() << std::endl;
                }
            });
    }

private:
    boost::asio::ip::tcp::socket socket_;
    boost::asio::ip::tcp::resolver resolver_;
    std::string brokerAddress_;
    std::string port_;
};

int main()
{
    boost::asio::io_context ioContext;

    auto mqttClient = std::make_shared<MqttClient>(ioContext, "localhost", "1883");
    mqttClient->connect();

    ioContext.run();

    while (true)
    {

    }

    return 0;
}
