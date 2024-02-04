#pragma once
#include <iostream>
#include <vector>
#include "Mqtt_Packet.h"

class MqttConnectParser {
public:
    MqttConnectParser(const std::vector<char>& data) : data_(data), index_(0) {
        options = new ConnectOptions();
    }

    ConnectOptions* parse() {

        options->packetType = readByte();
        options->protocolNameLength = readByte();
        options->protocolName = readString(options->protocolNameLength);
        options->protocolLevel = readByte();
        options->connectFlags = readByte();
        options->keepAlive = readUint16();
        options->clientId = readString(readUint16());
        options->willTopic = readString(readUint16());
        options->willMessage = readString(readUint16());
        options->username = readString(readUint16());
        options->password = readString(readUint16());

        return options;
    }

    std::string toString() {
        std::string str = "Packet type: ";
        str.append(std::to_string(options->packetType) + "\nProtocol name: " + options->protocolName + "\n" +
                                "Protocol level : " + std::to_string(options->protocolLevel) + "\nConnect flags : " + std::to_string(options->connectFlags)
                                + "\nKeep alive: " + std::to_string(options->keepAlive) + "\nClient id: " + options->clientId + "\nWill topic: "
                                + options->willTopic + "\nWill message: " + options->willMessage + "\nUsername: " + options->username + "\nPassword: "
                                + options->password);
        return str;
    }
private:
    uint8_t readByte() {
        if (data_.size() > index_) {
            return data_[index_++];
        }
        return 0;
    }

    uint16_t readUint16() {
        uint16_t value = (static_cast<uint16_t>(readByte()) << 8) | static_cast<uint16_t>(readByte());
        return value;
    }

    std::string readString(std::size_t length) {
        std::string str;
        for (std::size_t i = 0; i < length; ++i) {
            str.push_back(static_cast<char>(readByte()));
        }
        return str;
    }


private:
    const std::vector<char>& data_;
    std::size_t index_;
    ConnectOptions* options;
};