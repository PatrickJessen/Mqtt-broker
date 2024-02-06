#include "MqttServer.h"
#include <iomanip>

void MqttServer::startAccept()
{
    auto newClient = std::make_shared<MqttClient>(std::move(socket_), nextId);
    try
    {
        acceptor_.async_accept(newClient->socket(),
            [this, newClient](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "New client trying to connect." << std::endl;
                    clients_.insert(newClient);
                    nextId++;
                    readHeader(newClient);
                }
                startAccept();
            });
    }
    catch (std::exception e)
    {
        std::cout << "Failed to accept new client\n";
    }
}
//mosquitto_pub -p 1883 -t test -m "hello world"
//mosquitto_pub -h localhost -p 1883 -t test -q 0 -u username -P password --will-topic "will_topic" --will-payload "Will message" -m "" -d
void MqttServer::readHeader(std::shared_ptr<MqttClient> client)
{
    client->header().clear();
    client->header().resize(2);
    try
    {
        boost::asio::async_read(client->socket(), boost::asio::buffer(client->header()),
            [this, client](boost::system::error_code ec, std::size_t length) {
                if (!ec && length > 0) {
                    uint8_t packetType = (client->header()[0] & 0xF0) >> 4;
                    processHeader(client, length);
                    switch (packetType)
                    {
                    case 1:
                    {
                        type = PacketType::CONNECT;
                        break;
                    }
                    case 8:
                    {
                        type = PacketType::SUBSCRIBE;
                        break;
                    }
                    case 3:
                    {
                        type = PacketType::PUBLISH;
                        break;
                    }
                    case 14:
                    {
                        type = PacketType::DISCONNECT;
                        handleClientDisconnect(client);
                        break;
                    }
                    case 12:
                    {
                        type = PacketType::PINGREQ;
                        break;
                    }
                    default:
                        break;
                    }
                    if (type != PacketType::DISCONNECT && type != PacketType::PINGREQ) {
                        readPayload(client);
                    }
                }
                else {
                    std::cerr << "Read error: " << ec.message() << std::endl;
                    handleClientDisconnect(client);
                }
            });
    }
    catch (std::exception e)
    {
        std::cout << "Failed to read header\n";
    }
}

bool MqttServer::sendSuback(std::shared_ptr<MqttClient> client)
{
    client->lastMessageId()++;
    std::vector<uint8_t> qosLevels = { 0x00, 0x01, 0x02 };
    std::vector<char> subackPacket;
    subackPacket.push_back(0x90); // Fixed Header: SUBACK
    subackPacket.push_back(0x03); // Remaining Length
    subackPacket.push_back(static_cast<uint8_t>(client->lastMessageId() >> 8)); // Message Identifier (MSB)
    subackPacket.push_back(static_cast<uint8_t>(client->lastMessageId() & 0xFF)); // Message Identifier (LSB)
    subackPacket.push_back(0x00);

    // Send SUBACK packet
    try
    {
        boost::asio::async_write(client->socket(), boost::asio::buffer(subackPacket.data(), subackPacket.size()),
            [client](boost::system::error_code ec, std::size_t /*length*/) {
                if (ec) {
                    std::cerr << "Write error to " << client->clientInformation().clientId << ": " << ec.message() << std::endl;
                    return FAIL;
                }
                else {
                    std::cout << "Send SUBACK to " << client->clientInformation().clientId << "\n";
                    return SUCCESS;
                }
            });
    }
    catch (std::exception e)
    {
        std::cout << "Failed to write SUBACK\n";
        return FAIL;
    }
}

bool MqttServer::sendConnack(std::shared_ptr<MqttClient> client, bool canConnect)
{
    try {
        if (canConnect) {
            // Send connection successfull packet
            const char connackPacket[] = { 0x20, 0x02, 0x00, 0x00 };
            boost::asio::write(client->socket(), boost::asio::buffer(connackPacket, sizeof(connackPacket)));
            std::cout << "Send CONNACK to " << client->clientInformation().clientId << "\n";
            return SUCCESS;
        }
        // Send connection denied packet (code 5)
        const char connackPacket[] = { 0x20, 0x02, 0x00, 0x05 };
        boost::asio::write(client->socket(), boost::asio::buffer(connackPacket, sizeof(connackPacket)));
        std::cout << "Send CONNACK to " << client->clientInformation().clientId << "\n";
        return FAIL;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception during Connack packet send: " << e.what() << std::endl;
        return FAIL;
    }
}

bool MqttServer::sendPingResponse(std::shared_ptr<MqttClient> client)
{
    try {
        const char pingresp[] = { 0x0E, 0x00 };
        boost::asio::write(client->socket(), boost::asio::buffer(pingresp, sizeof(pingresp)));
        std::cout << "Send PINGRESP to " << client->clientInformation().clientId << "\n";
        return SUCCESS;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception during Connack packet send: " << e.what() << std::endl;
        return FAIL;
    }
}

void MqttServer::readPayload(std::shared_ptr<MqttClient> client)
{
    client->payload().clear();
    // Resize the payload buffer to the expected length
    client->payload().resize(client->payloadLength());
    if (client->socket().is_open()) {
        try
        {
            boost::asio::async_read(client->socket(), boost::asio::buffer(client->payload(), client->payloadLength()),
                [this, client](boost::system::error_code ec, std::size_t length) {
                    if (!ec) {
                        processPayload(client, length);
                        readHeader(client);
                    }
                    else {
                        std::cerr << "Read error: " << ec.message() << std::endl;
                        handleClientDisconnect(client);
                    }
                });
        }
        catch (std::exception e)
        {
            std::cout << "Failed to read payload\n";
        }
    }
}

bool MqttServer::handleMqttPublish(std::shared_ptr<MqttClient> sender)
{
    std::vector<uint8_t> packetVec;

    // QoS level
    packetVec.push_back(0x30);

    uint16_t topicLength = publishPacket.topic.size();
    uint16_t payloadLength = publishPacket.payload.size();
    // remaining length
    packetVec.push_back(payloadLength + topicLength +2);
    // topic length 0x00 -> topic length
    packetVec.push_back(0x00);
    packetVec.push_back(static_cast<uint8_t>(topicLength & 0xFF));

    // Append topic bytes to the packet
    for (char c : publishPacket.topic) {
        packetVec.push_back(static_cast<uint8_t>(c));
    }

    // Append payload bytes to the packet
    for (char c : publishPacket.payload) {
        packetVec.push_back(static_cast<uint8_t>(c));
    }

    // MQTT PUBLISH packet example (QoS 0)
    std::vector<uint8_t> test = {
        0x30,       // Fixed Header: PUBLISH, QoS 0, retain flag 0
        0x0F + 2,       // Remaining Length
        0x00, 0x04, // Topic Length (4 bytes)
        't', 'e', 's', 't', // Topic: "test"
        'H', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!' // Message: "Hello world!"
    };

    // Send the Publish packet
    for (const auto& client : clients_) {
        if (client != sender) {
            for (const auto& subscription : client->subscribedTopics()) {
                if (subscription.topic == publishPacket.topic) {
                    boost::asio::async_write(client->socket(), boost::asio::buffer(packetVec.data(), packetVec.size()),
                        [client](boost::system::error_code ec, std::size_t /*length*/) {
                            if (ec) {
                                std::cerr << "Write error to " << client->clientInformation().clientId << " " << ec.message() << std::endl;
                                return FAIL;
                            }
                            else {
                                std::cout << "Message sent to " << client->clientInformation().clientId << "\n";
                            }
                        });
                }
            }
        }
    }
    return SUCCESS;
}

bool MqttServer::handleClientDisconnect(std::shared_ptr<MqttClient> client)
{
    try
    {
        clients_.erase(client);
        std::cout << "Client " << client->clientInformation().clientId << " disconnected." << std::endl;
        // Cut the thread if exists
        for (int i = 0; i < threadPool.size(); i++) {
            if (client->sessionId() == threadPool[i].id) {
                threadPool[i].thread.join();
            }
        }
        return SUCCESS;
    }
    catch (std::exception e)
    {
        std::cout << "Failed to disconnect " << client->clientInformation().clientId << "\n";
        return FAIL;
    }
}

bool MqttServer::processHeader(std::shared_ptr<MqttClient> client, const std::size_t& length)
{
    // Extract payload length from the fixed header
    if (length >= 2) {
        client->payloadLength() = 0;
        uint8_t multiplier = 1;
        size_t index = 1;

        do {
            if (index >= length) {
                std::cerr << "Invalid Remaining Length encoding." << std::endl;
                return FAIL;
            }
            client->payloadLength() += (static_cast<uint8_t>(client->header()[index]) & 0x7F) * multiplier;
            multiplier *= 128;
            ++index;
        } while ((static_cast<uint8_t>(client->header()[index - 1]) & 0x80) != 0);
        return SUCCESS;
    }
    else {
        std::cerr << "Invalid header length." << std::endl;
        return FAIL;
    }
    return FAIL;
}

bool MqttServer::processPayload(std::shared_ptr<MqttClient> client, const std::size_t& length)
{
    if (length > 0) {
        std::vector<char> received_data(client->payload().begin(), client->payload().begin() + length);
        // Parse received data
        MqttParser parser(received_data);
        // Check Mqtt packet type
        if (type == PacketType::CONNECT) {  
            return handleConnectPacket(client, parser);
        }
        else if (type == PacketType::SUBSCRIBE) {
            return handleSubscribePacket(client, parser);
        }
        else if (type == PacketType::PUBLISH) {
            return handlePublishPacket(client, parser);
        }
        else {
            std::cerr << "Unknown data: ";
            for (char c : received_data) {
                std::cerr << static_cast<int>(static_cast<unsigned char>(c)) << " ";
            }
            std::cerr << std::endl;
            return FAIL;
        }
    }
    return FAIL;
}

void MqttServer::startKeepAlive(std::shared_ptr<MqttClient> client)
{
    /*try
    {
        while (client->clientInformation().keepAlive > 0)
        {
            Sleep(1000);
            client->clientInformation().keepAlive--;
        }
        handleClientDisconnect(client);
    }
    catch (std::exception e)
    {
        handleClientDisconnect(client);
    }*/
}

bool MqttServer::handleConnectPacket(std::shared_ptr<MqttClient> client, MqttParser parser)
{
    connectPacket = parser.parseConnect();
    if (connectPacket.username.empty() || connectPacket.password.empty()) {
        sendConnack(client, false);
        handleClientDisconnect(client);
        return FAIL;
    }
    sendConnack(client, true);
    client->clientInformation() = { connectPacket.clientId, connectPacket.username, connectPacket.password, connectPacket.keepAlive };
    connectPacket.print();
    int clientId = client->sessionId();
    /*if (client) {
        threadPool.push_back({ std::thread([&]() {
            startKeepAlive(client);
            }), clientId });
    }*/
    return SUCCESS;
}

bool MqttServer::handleSubscribePacket(std::shared_ptr<MqttClient> client, MqttParser parser)
{
    subscribePacket = parser.parseSubscription();
    client->subscribe(subscribePacket);
    sendSuback(client);
    return SUCCESS;
}

bool MqttServer::handlePublishPacket(std::shared_ptr<MqttClient> client, MqttParser parser)
{
    publishPacket = parser.parsePublish();
    handleMqttPublish(client);
    return SUCCESS;
}
