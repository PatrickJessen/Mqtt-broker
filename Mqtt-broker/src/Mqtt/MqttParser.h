#pragma once
#include <iostream>
#include <vector>
#include "Mqtt_Packet.h"

class MqttParser {
public:
    MqttParser(const std::vector<char>& data);

    ConnectOptions parseConnect();

    SubscriptionOptions parseSubscription();

    PublishPacket parsePublish();
private:
    uint8_t readByte();

    uint16_t readUint16();

    std::string readString(std::size_t length);
private:
    const std::vector<char>& data_;
    std::size_t index_;
};