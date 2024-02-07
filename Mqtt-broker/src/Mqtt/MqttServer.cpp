#include "MqttServer.h"
#include <iomanip>

static bool firstInit = true;

MqttServer::MqttServer(boost::asio::io_service& io_service, short port)
    : acceptor_(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
    clients_(), socket_(io_service) {
    std::cout << "Mqtt server started...\n";
    startAccept();
}

void MqttServer::startAccept()
{
    auto newClient = std::make_shared<MqttClient>(std::move(socket_), nextId);
    try
    {
        acceptor_.async_accept(newClient->socket(),
            [this, newClient](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "New client trying to connect." << std::endl;
                    clients_.push_back(newClient);
                    nextId++;
                    if (firstInit) {
                        firstInit = false;
                        thread = std::thread([&]() {
                            startKeepAlive();
                            });
                    }
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
                        client->clientInformation().timeToDisconnect = client->clientInformation().keepAlive * 1.5;
                        std::cout << "received ping request\n";
                        sendPingResponse(client);
                        break;
                    }
                    default:
                        break;
                    }
                    if (type != PacketType::DISCONNECT && type != PacketType::PINGREQ) {
                        readPayload(client);
                    }
                    else {
                        readHeader(client);
                    }
                }
                else {
                    std::cerr << "Read error: " << ec.message() << std::endl;
                    handleClientDisconnect(client);
                    readHeader(client);
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

bool MqttServer::sendConnack(std::shared_ptr<MqttClient> client, uint8_t flag)
{
    try {
        const char connackPacket[] = { 0x20, 0x02, 0x00, flag };
        boost::asio::write(client->socket(), boost::asio::buffer(connackPacket, sizeof(connackPacket)));
        std::cout << "Send CONNACK to " << client->clientInformation().clientId << "\n";
        return SUCCESS;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception during Connack packet send: " << e.what() << std::endl;
        return FAIL;
    }
}

bool MqttServer::sendPingResponse(std::shared_ptr<MqttClient> client)
{
    try {
        const char pingresp[] = { 0xD0, 0x00 };
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
        for (int i = 0; i < clients_.size(); i++) {
            if (client->sessionId() == clients_[i]->sessionId()) {
                clients_.erase(clients_.begin() + i);
            }
        }
        std::cout << "Client " << client->clientInformation().clientId << " disconnected." << std::endl;
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

void MqttServer::startKeepAlive()
{
    std::shared_ptr<MqttClient> clientToDisconnect;
    try
    {
        for (int i = 0; i < clients_.size(); i++) {
            if (clients_[i]->clientInformation().timeToDisconnect > 0) {
                clients_[i]->clientInformation().timeToDisconnect--;
                //std::cout << "ClientId: " << clients_[i]->sessionId() << " Keep alive: " << clients_[i]->clientInformation().timeToDisconnect << "\n";
            }
            else {
                if (type != PacketType::PINGRESP && clients_[i]->clientInformation().keepAlive != 0) {
                    clientToDisconnect = clients_[i];
                    handleClientDisconnect(clientToDisconnect);
                }
            }
        }
        Sleep(1000);
    }
    catch (std::exception e)
    {
        handleClientDisconnect(clientToDisconnect);
    }
    startKeepAlive();
}

bool MqttServer::handleConnectPacket(std::shared_ptr<MqttClient> client, MqttParser parser)
{
    connectPacket = parser.parseConnect();
    if (connectPacket.keepAlive > 65535) {
        connectPacket.keepAlive = 65535;
    }
    if (connectPacket.clientId.empty()) {
        generateClientId(connectPacket.clientId);
    }
    // Check if clientId exists already
    for (int i = 0; i < clients_.size(); i++) {
        if (connectPacket.clientId == clients_[i]->clientInformation().clientId) {
            handleClientDisconnect(clients_[i]);
        }
    }
    client->clientInformation() = { connectPacket.clientId, connectPacket.username, connectPacket.password, 
        connectPacket.keepAlive, static_cast<int>(connectPacket.keepAlive * 1.5) };
    connectPacket.print();
    if (!connectPacket.username.empty() && connectPacket.password.empty()) {
        sendConnack(client, 0x05);
        handleClientDisconnect(client);
        return FAIL;
    }
    sendConnack(client, 0x00);
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

void MqttServer::generateClientId(std::string& clientId)
{
    std::string availableChars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (int i = 0; i < 22; i++) {
        clientId += availableChars[rand() % availableChars.size()];
    }
}
