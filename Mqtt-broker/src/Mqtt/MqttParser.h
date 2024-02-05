#pragma once
#include <iostream>
#include <vector>
#include "Mqtt_Packet.h"

class MqttParser {
public:
    MqttParser(const std::vector<char>& data) : data_(data), index_(0) 
    {
    }

    ConnectOptions parseConnect() 
    {

        ConnectOptions options;
        options.packetType = readByte();
        options.protocolNameLength = readByte();
        options.protocolName = readString(options.protocolNameLength);
        options.protocolLevel = readByte();
        options.connectFlags = readByte();
        options.keepAlive = readUint16();
        options.clientId = readString(readUint16());
        options.willTopic = readString(readUint16());
        options.willMessage = readString(readUint16());
        options.username = readString(readUint16());
        options.password = readString(readUint16());

        return options;
    }

    SubscriptionOptions parseSubscription(std::vector<char> data) 
    {
        SubscriptionOptions options;
        size_t offset = 2; // Start of variable header
        uint16_t packetIdentifier = (data[offset] << 8) | data[offset + 1];
        offset += 2;

        while (offset < data.size()) {
            // Extract the Topic Filter
            std::string topic;
            while (data[offset] != 0x00 && offset < data.size()) {
                topic += static_cast<char>(data[offset]);
                offset++;
            }
            if (offset +1 < data.size()) {
                offset++; // Move past the null character
            }
            QoSType qosLevel;
            uint8_t qos = data[offset] & 0x03;
            qosLevel = (QoSType)qos;
            // Extract QoS Level
            offset++;

            options = { topic, qosLevel }; // should convert to vector to handle multiple topics
        }
        return options;
        // Ensure the packet is a SUBSCRIBE packet (assuming it is well-formed)
    }

    PublishPacket parsePublish(std::vector<char> data) {
        // Assuming 'data' is the received PUBLISH packet
        PublishPacket publishPacket;

        // Extract the QoS level from the fixed header
        publishPacket.qos = (data[0] & 0x06) >> 1;

        // Extract the topic length from the variable header
        uint16_t topicLength = data[1];

        // Extract the topic name from the variable header
        for (uint16_t i = 0; i < topicLength; ++i) {
            publishPacket.topic.push_back(static_cast<char>(data[i]));
        }

        // Extract the payload from the packet
        for (size_t i = 4 + topicLength; i < data.size(); ++i) {
            publishPacket.payload.push_back(static_cast<char>(data[i]));
        }

        return publishPacket;
    }

    /*std::string toString() {
        std::string str = "Packet type: ";
        str.append(std::to_string(options->packetType) + "\nProtocol name: " + options->protocolName + "\n" +
                                "Protocol level : " + std::to_string(options->protocolLevel) + "\nConnect flags : " + std::to_string(options->connectFlags)
                                + "\nKeep alive: " + std::to_string(options->keepAlive) + "\nClient id: " + options->clientId + "\nWill topic: "
                                + options->willTopic + "\nWill message: " + options->willMessage + "\nUsername: " + options->username + "\nPassword: "
                                + options->password);
        return str;
    }*/
private:
    uint8_t readByte() 
    {
        if (data_.size() > index_) {
            return data_[index_++];
        }
        return 0;
    }

    uint16_t readUint16() 
    {
        uint16_t value = (static_cast<uint16_t>(readByte()) << 8) | static_cast<uint16_t>(readByte());
        return value;
    }

    std::string readString(std::size_t length) 
    {
        std::string str;
        for (std::size_t i = 0; i < length; ++i) {
            str.push_back(static_cast<char>(readByte()));
        }
        return str;
    }


private:
    const std::vector<char>& data_;
    std::size_t index_;
};