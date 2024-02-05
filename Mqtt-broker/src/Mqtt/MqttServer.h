#pragma once
#include "MqttClient.h"
#include "Mqtt_Packet.h"

class MqttServer {
public:
    MqttServer(boost::asio::io_service& io_service, short port)
        : acceptor_(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
        clients_(), socket_(io_service) {
        std::cout << "Mqtt server started...\n";
        startAccept();
    }

private:
    void startAccept();

    void readHeader(std::shared_ptr<MqttClient> client);

    void sendSuback(std::shared_ptr<MqttClient> client, uint8_t msgId);

    void sendConnack(std::shared_ptr<MqttClient> client);

    void readPayload(std::shared_ptr<MqttClient> client);

    void handleMqttMessage(std::shared_ptr<MqttClient> client, const std::string& message);

    void handleMqttPublish(std::shared_ptr<MqttClient> sender, const std::string& topic, const std::string& publishMessage);

    void handleClientDisconnect(std::shared_ptr<MqttClient> client);

    void processHeader(std::shared_ptr<MqttClient> client, const std::size_t& length);

    void processPayload(std::shared_ptr<MqttClient> client, const std::size_t& length);

private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket socket_;
    std::unordered_set<std::shared_ptr<MqttClient>> clients_;
    PacketType type;
    ConnectOptions connectPacket;
    SubscriptionOptions subscribePacket;
    PublishPacket publishPacket;
    int nextId = 0;
};