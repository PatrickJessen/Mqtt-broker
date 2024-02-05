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

enum class QoSType {
    QOS0 = 0,
    QOS1 = 1,
    QOS2 = 2
};

// MQTT Fixed Header
struct FixedHeader {
    uint8_t controlPacketType : 4;
    uint8_t flags : 4;
};

struct SubscriptionOptions {
    std::string topic;
    QoSType qosLevel;
};

struct ConnectOptions {
    uint8_t packetType;
    uint8_t protocolNameLength;
    std::string protocolName;
    uint8_t protocolLevel;
    uint8_t connectFlags;
    uint16_t keepAlive;
    std::string clientId;
    std::string willTopic;
    std::string willMessage;
    std::string username;
    std::string password;
};


// MQTT Publish Packet
struct PublishPacket {
    uint8_t qos;
    std::string topic;
    std::string payload;
};

// MQTT Subscription Packet
struct SubscribePacket {
    FixedHeader fixedHeader;
    // Packet Identifier
    // List of Topic Filters and corresponding QoS levels
    // ...
};