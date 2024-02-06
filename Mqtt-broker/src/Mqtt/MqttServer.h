#pragma once
#include "MqttClient.h"
#include "Mqtt_Packet.h"
#include "MqttParser.h"

struct Thread {
    std::thread thread;
    int id;
};

enum {
    FAIL = 0,
    SUCCESS = 1
};

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

    bool sendSuback(std::shared_ptr<MqttClient> client);

    bool sendConnack(std::shared_ptr<MqttClient> client, bool canConnect);

    bool sendPingResponse(std::shared_ptr<MqttClient> client);

    void readPayload(std::shared_ptr<MqttClient> client);

    bool handleMqttPublish(std::shared_ptr<MqttClient> sender);

    bool handleClientDisconnect(std::shared_ptr<MqttClient> client);

    bool processHeader(std::shared_ptr<MqttClient> client, const std::size_t& length);

    bool processPayload(std::shared_ptr<MqttClient> client, const std::size_t& length);

    void startKeepAlive(std::shared_ptr<MqttClient> client);

    bool handleConnectPacket(std::shared_ptr<MqttClient> client, MqttParser parser);

    bool handleSubscribePacket(std::shared_ptr<MqttClient> client, MqttParser parser);

    bool handlePublishPacket(std::shared_ptr<MqttClient> client, MqttParser parser);
private:
    boost::asio::ip::tcp::acceptor acceptor_;
    boost::asio::ip::tcp::socket socket_;
    std::unordered_set<std::shared_ptr<MqttClient>> clients_;
    PacketType type;
    ConnectOptions connectPacket;
    SubscriptionOptions subscribePacket;
    PublishPacket publishPacket;
    int nextId = 0;
    std::vector<Thread> threadPool;
    friend class ServerUnitTest;
};