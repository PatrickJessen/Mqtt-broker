#pragma once
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <boost/asio.hpp>
#include "Mqtt_Packet.h"

class MqttClient 
{
public:
    MqttClient(boost::asio::ip::tcp::socket socket, int sessionId)
        : socket_(std::move(socket)), sessionId_(sessionId), subscribedTopics_() 
    {
        header_ = std::vector<char>(2);
    }

    boost::asio::ip::tcp::socket& socket() {
        return socket_;
    }

    const std::vector<SubscriptionOptions>& subscribedTopics() const {
        return subscribedTopics_;
    }

    void subscribe(const SubscriptionOptions& topic) {
        subscribedTopics_.push_back(topic);
    }

    void unsubscribe(const SubscriptionOptions& topic) {
        //subscribedTopics_.erase(topic);
    }

    std::vector<char>& payload() {
        return payload_;
    }

    std::vector<char>& header() {
        return header_;
    }

    std::size_t& payloadLength() {
        return payload_length_;
    }

    std::vector<char> payload_;
private:
    boost::asio::ip::tcp::socket socket_;
    std::vector<SubscriptionOptions> subscribedTopics_;
    std::vector<char> header_;
    std::size_t payload_length_;
    int sessionId_;
};