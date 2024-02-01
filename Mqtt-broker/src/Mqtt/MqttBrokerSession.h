#pragma once
#include <iostream>
#include <vector>
#include <memory>
#include <boost/asio.hpp>
#include "../interfaces/ISession.h"
#include "Mqtt_Packet.h"

using namespace boost::asio;

class MqttBrokerSession : public std::enable_shared_from_this<MqttBrokerSession>, public ISession<std::string> {
public:
    MqttBrokerSession(ip::tcp::socket socket) : socket_(std::move(socket)) {}

    virtual void Start() override;
private:
    virtual void ReadMessageAsync() override;
    virtual void SendMessageAsync(const std::string& message) override;
    virtual void IsAlive() override;
    virtual const bool GetIsAlive() override;
    void ReadPayload();
    void ReadHeader();
    void ProcessMessage(std::size_t length);
    void ProcessHeader(std::size_t length);

private:
    ConnectPacket* connectPacket = nullptr;
    PacketType packetType;
    ip::tcp::socket socket_;
    std::vector<char> header_;
    std::vector<char> payload_;
    std::size_t payload_length_;
};