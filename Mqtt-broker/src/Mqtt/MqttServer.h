#pragma once
#include "MqttClient.h"
#include "Mqtt_Packet.h"
#include "MqttParser.h"

enum {
    FAIL = 0,
    SUCCESS = 1
};

class MqttServer {
public:
    MqttServer(boost::asio::io_service& io_service, short port);

private:
    void startAccept();

    void readHeader(std::shared_ptr<MqttClient> client);

    bool sendSuback(std::shared_ptr<MqttClient> client);

    bool sendConnack(std::shared_ptr<MqttClient> client, uint8_t flag);

    bool sendPingResponse(std::shared_ptr<MqttClient> client);

    bool sendPuback(std::shared_ptr<MqttClient> client);

    void readPayload(std::shared_ptr<MqttClient> client);

    bool handleMqttPublish(std::shared_ptr<MqttClient> client);

    bool handleClientDisconnect(std::shared_ptr<MqttClient> client);

    bool processHeader(std::shared_ptr<MqttClient> client, const std::size_t& length);

    bool processPayload(std::shared_ptr<MqttClient> client, const std::size_t& length);

    void startKeepAlive();

    bool handleConnectPacket(std::shared_ptr<MqttClient> client, MqttParser parser);

    bool handleSubscribePacket(std::shared_ptr<MqttClient> client, MqttParser parser);

    bool handlePublishPacket(std::shared_ptr<MqttClient> client, MqttParser parser);

    void generateClientId(std::string& clientId);

    void handleQoS(std::shared_ptr<MqttClient> client, const int& qos);
private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket socket_;
    std::vector<std::shared_ptr<MqttClient>> clients_;
    PacketType type;
    ConnectOptions connectPacket;
    SubscriptionOptions subscribePacket;
    PublishPacket publishPacket;
    int nextId = 0;
    std::thread thread;
    friend class ServerUnitTest;
};