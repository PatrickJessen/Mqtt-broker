#include "MqttParser.h"

MqttParser::MqttParser(const std::vector<char>& data)
	: data_(data), index_(0)
{
}

ConnectOptions MqttParser::parseConnect()
{
    ConnectOptions options;
    options.packetType = readByte();
    options.protocolNameLength = readByte();
    options.protocolName = readString(options.protocolNameLength);
    options.protocolLevel = readByte();
    options.connectFlags = readByte();
    options.keepAlive = readUint16();
    options.clientId = readString(readUint16());

    // Should handle this in a better way
    if (options.connectFlags == 192 || options.connectFlags == 194) {
        options.username = readString(readUint16());
        options.password = readString(readUint16());
    }
    else if (options.connectFlags == 198) {
        options.willTopic = readString(readUint16());
        options.willMessage = readString(readUint16());
        options.username = readString(readUint16());
        options.password = readString(readUint16());
    }

    return options;
}

SubscriptionOptions MqttParser::parseSubscription()
{
    SubscriptionOptions options;
    size_t offset = 2; // Start of variable header
    uint16_t packetIdentifier = (data_[offset] << 8) | data_[offset + 1];
    offset += 2;

    while (offset < data_.size()) {
        // Extract the Topic Filter
        std::string topic;
        while (data_[offset] != 0x00 && offset < data_.size()) {
            topic += static_cast<char>(data_[offset]);
            offset++;
        }
        if (offset + 1 < data_.size()) {
            offset++; // Move past the null character
        }
        QoSType qosLevel;
        uint8_t qos = data_[offset] & 0x03;
        qosLevel = (QoSType)qos;
        // Extract QoS Level
        offset++;

        options = { topic, qosLevel }; // should convert to vector to handle multiple topics
    }
    return options;
    // Ensure the packet is a SUBSCRIBE packet (assuming it is well-formed)
}

PublishPacket MqttParser::parsePublish() {
    // Assuming 'data' is the received PUBLISH packet
    PublishPacket publishPacket;

    // Extract the QoS level from the fixed header
    publishPacket.qos = data_[0];

    // Extract the topic length from the variable header
    uint16_t topicLength = data_[1];
    uint16_t topicStart = 2;
    // Extract the topic name from the variable header
    for (uint16_t i = topicStart; i < topicLength + topicStart; ++i) {
        publishPacket.topic.push_back(static_cast<char>(data_[i]));
    }

    // Extract the payload from the packet
    for (size_t i = 2 + topicLength; i < data_.size(); ++i) {
        publishPacket.payload.push_back(static_cast<char>(data_[i]));
    }

    return publishPacket;
}

uint8_t MqttParser::readByte()
{
    if (data_.size() > index_) {
        return data_[index_++];
    }
    return 0;
}

uint16_t MqttParser::readUint16()
{
    uint16_t value = (static_cast<uint16_t>(readByte()) << 8) | static_cast<uint16_t>(readByte());
    return value;
}

std::string MqttParser::readString(std::size_t length)
{
    std::string str;
    for (std::size_t i = 0; i < length; ++i) {
        str.push_back(static_cast<char>(readByte()));
    }
    return str;
}