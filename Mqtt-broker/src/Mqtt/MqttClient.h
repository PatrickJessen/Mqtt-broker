#pragma once
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <boost/asio.hpp>
#include "Mqtt_Packet.h"

class MqttClient 
{
public:
    MqttClient(boost::asio::ip::tcp::socket socket, int sessionId);

    boost::asio::ip::tcp::socket& socket();

    const std::vector<SubscriptionOptions>& subscribedTopics() const;

    void subscribe(const SubscriptionOptions& topic);

    void unsubscribe(const SubscriptionOptions& topic);

    std::vector<char>& payload();

    std::vector<char>& header();

    std::size_t& payloadLength();

    uint16_t& lastMessageId();

    const int& sessionId() const;

    ClientInformation& clientInformation();

private:
    boost::asio::ip::tcp::socket socket_;
    std::vector<SubscriptionOptions> subscribedTopics_;
    std::vector<char> payload_;
    std::vector<char> header_;
    std::size_t payload_length_;
    int sessionId_;
    uint16_t lastMessageId_ = 0;
    ClientInformation info;
};