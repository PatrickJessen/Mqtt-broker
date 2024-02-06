#include "MqttClient.h"


MqttClient::MqttClient(boost::asio::ip::tcp::socket socket, int sessionId)
    : socket_(std::move(socket)), sessionId_(sessionId), subscribedTopics_()
{
    // Predefine header length to 2 bytes
    header_ = std::vector<char>(2);
}

boost::asio::ip::tcp::socket& MqttClient::socket() {
    return socket_;
}

const std::vector<SubscriptionOptions>& MqttClient::subscribedTopics() const {
    return subscribedTopics_;
}

void MqttClient::subscribe(const SubscriptionOptions& topic) {
    subscribedTopics_.push_back(topic);
}

void MqttClient::unsubscribe(const SubscriptionOptions& topic) {

}

std::vector<char>& MqttClient::payload() {
    return payload_;
}

std::vector<char>& MqttClient::header() {
    return header_;
}

std::size_t& MqttClient::payloadLength() {
    return payload_length_;
}

uint16_t& MqttClient::lastMessageId() {
    return lastMessageId_;
}

const int& MqttClient::sessionId() const {
    return sessionId_;
}

ClientInformation& MqttClient::clientInformation() {
    return info;
}