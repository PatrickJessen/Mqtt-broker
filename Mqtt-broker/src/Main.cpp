#include "Mqtt/MqttServer.h"


int main()
{
    try {
        boost::asio::io_service io_service;
        MqttServer mqttServer(io_service, 1883); // MQTT default port

        io_service.run();
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}

//#include <iostream>
//#include <vector>
//#include <boost/asio.hpp>
//
//using boost::asio::ip::tcp;
//
//class MqttServer {
//public:
//    MqttServer(boost::asio::io_service& io_context, uint16_t port)
//        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)), socket_(io_context) {
//        startAccept1();
//    }
//
//private:
//    void startAccept1() {
//        if (socket_.is_open()) {
//            acceptor_.async_accept(socket_,
//                [this](const boost::system::error_code& ec) {
//                    if (!ec) {
//                        std::cout << "Client connected." << std::endl;
//                        startRead();
//                    }
//                    else {
//                        std::cerr << "Accept error: " << ec.message() << std::endl;
//                        socket_.close();
//                    }
//
//                    startAccept1();  // Accept the next connection
//                });
//        }
//    }
//
//    void startRead() {
//        boost::asio::async_read(socket_, boost::asio::buffer(headerBuffer_, 2),
//            [this](const boost::system::error_code& ec, std::size_t bytes_transferred) {
//                if (!ec) {
//                    if (bytes_transferred == 2) {
//                        std::size_t remainingBytes = (headerBuffer_[1] & 0x7F);
//                        payloadBuffer_.resize(remainingBytes);
//
//                        // Check if the socket is still open
//                        if (socket_.is_open()) {
//                            boost::asio::async_read(socket_, boost::asio::buffer(payloadBuffer_, remainingBytes),
//                                [this](const boost::system::error_code& ec, std::size_t bytes_transferred) {
//                                    if (!ec) {
//                                        processPacket();
//                                        startRead();
//                                    }
//                                    else if (ec == boost::asio::error::eof) {
//                                        // Handle the case when the client closes the connection
//                                        std::cerr << "Connection closed by client." << std::endl;
//                                        socket_.close();
//                                    }
//                                    else {
//                                        std::cerr << "Read error: " << ec.message() << std::endl;
//                                        socket_.close();
//                                    }
//                                });
//                        }
//                        else {
//                            std::cerr << "Socket closed during read." << std::endl;
//                        }
//                    }
//                    else {
//                        std::cerr << "Invalid header size." << std::endl;
//                        socket_.close();
//                    }
//                }
//                else {
//                    std::cerr << "Read error: " << ec.message() << std::endl;
//                    socket_.close();
//                }
//            });
//    }
//
//    void processPacket() {
//        // Assuming headerBuffer_ and payloadBuffer_ contain the received MQTT packet
//        std::vector<uint8_t> packetData;
//        packetData.insert(packetData.end(), headerBuffer_.begin(), headerBuffer_.end());
//        packetData.insert(packetData.end(), payloadBuffer_.begin(), payloadBuffer_.end());
//
//        // Your existing logic for processing the packet based on its type
//        // ...
//
//        // Example: Handling a SUBSCRIBE packet
//        if (packetData.size() > 2 && (packetData[0] >> 4) == 0x08) {
//            handleSubscribe(packetData);
//        }
//    }
//
//    void handleSubscribe(const std::vector<uint8_t>& packetData) {
//        // Your logic to handle a SUBSCRIBE packet
//        // Example: Extracting the topic from the payload
//        if (packetData.size() >= 5) {
//            std::size_t topicLength = (packetData[3] << 8) + packetData[4];
//            if (packetData.size() >= 5 + topicLength) {
//                std::string topic(packetData.begin() + 5, packetData.begin() + 5 + topicLength);
//                std::cout << "Received SUBSCRIBE packet for topic: " << topic << std::endl;
//                // Process the subscription details
//            }
//        }
//    }
//
//private:
//    tcp::acceptor acceptor_;
//    tcp::socket socket_;
//    std::array<uint8_t, 2> headerBuffer_;  // Buffer for storing MQTT packet header
//    std::vector<uint8_t> payloadBuffer_;    // Buffer for storing MQTT packet payload
//};
//
//int main() {
//    try {
//        boost::asio::io_service io_context;
//        MqttServer server(io_context, 1883);
//        io_context.run();
//    }
//    catch (const std::exception& e) {
//        std::cerr << "Exception: " << e.what() << std::endl;
//    }
//
//    return 0;
//}
//
