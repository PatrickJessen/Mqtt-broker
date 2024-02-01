#pragma once
#include <string>

// MQTT Packet Types
enum class PacketType : uint8_t {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    PUBREC = 5,
    PUBREL = 6,
    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRESP = 13,
    DISCONNECT = 14
};

// MQTT Connect Flags
enum ConnectFlags : uint8_t {
    CLEAN_SESSION = 2,
    WILL_FLAG = 4,
    WILL_QOS = 8,
    WILL_RETAIN = 32,
    PASSWORD_FLAG = 64,
    USERNAME_FLAG = 128
};

// MQTT Fixed Header
struct FixedHeader {
    uint8_t controlPacketType : 4;
    uint8_t flags : 4;
};

struct ConnectPacket {
    uint8_t fixedHeader;
    uint8_t remainingLength;
    uint8_t protocolNameLength;
    std::string protocolName;
    uint8_t protocolLevel;
    uint8_t connectFlags;
    uint16_t keepAlive;
    uint16_t clientIdLength;
    std::string clientId;
    std::string username;
    std::string password;
    std::string will;

    // Constructor to initialize the fields
    ConnectPacket(const std::string& protoName, const std::string& clientID, uint16_t keepAliveSecs)
        : fixedHeader(static_cast<uint8_t>(PacketType::CONNECT) << 4),
        protocolNameLength(static_cast<uint8_t>(strlen(protoName.c_str()))),
        protocolName(protoName),
        protocolLevel(4),  // MQTT protocol version 3.1.1
        connectFlags(0),
        keepAlive(keepAliveSecs),
        clientIdLength(static_cast<uint16_t>(strlen(clientID.c_str()))),
        clientId(clientID)
    {
        // Set CONNECT flags as needed
        connectFlags |= CLEAN_SESSION;
        // ... set other connect flags
    }

    // Function to encode the CONNECT packet
    void encode(char* buffer) const {
        buffer[0] = fixedHeader;
        buffer[1] = remainingLength;
        buffer[2] = protocolNameLength;
        std::memcpy(buffer + 3, protocolName.c_str(), protocolNameLength);
        buffer[3 + protocolNameLength] = protocolLevel;
        buffer[4 + protocolNameLength] = connectFlags;
        std::memcpy(buffer + 5 + protocolNameLength, &keepAlive, 2);
        buffer[7 + protocolNameLength] = clientIdLength >> 8;
        buffer[8 + protocolNameLength] = clientIdLength & 0xFF;
        std::memcpy(buffer + 9 + protocolNameLength, clientId.c_str(), clientIdLength);
        // ... encode additional fields
    }
};

// MQTT Publish Packet
struct PublishPacket {
    FixedHeader fixedHeader;
    uint16_t topicLength;
    std::string topic;
    // Quality of Service (QoS), Retain Flag, Message Identifier, and Payload
    // ...

    // Additional fields for QoS handling
};

// MQTT Subscription Packet
struct SubscribePacket {
    FixedHeader fixedHeader;
    // Packet Identifier
    // List of Topic Filters and corresponding QoS levels
    // ...
};