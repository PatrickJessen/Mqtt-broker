#include "MqttServer.h"
#include "MqttParser.h"
#include <iomanip>

void MqttServer::startAccept()
{
    auto newClient = std::make_shared<MqttClient>(std::move(socket_), nextId);
    acceptor_.async_accept(newClient->socket(),
        [this, newClient](boost::system::error_code ec) {
            if (!ec) {
                std::cout << "Client connected." << std::endl;
                clients_.insert(newClient);
                nextId++;
                readHeader(newClient);
            }
            startAccept();
        });
}
//mosquitto_pub -p 1883 -t test -m "hello world"
//mosquitto_pub -h localhost -p 1883 -t "test" -q 0 -u your_username -P your_password --will-topic "will_topic" --will-payload "Will message" -m "" -d -V mqttv311
void MqttServer::readHeader(std::shared_ptr<MqttClient> client)
{
    client->header().clear();
    client->header().resize(2);
    boost::asio::async_read(client->socket(), boost::asio::buffer(client->header()),
        [this, client](boost::system::error_code ec, std::size_t length) {
            if (!ec && length > 0) {
                uint8_t packetType = (client->header()[0] & 0xF0) >> 4;
                std::cout << "Received MQTT Packet Type: " << static_cast<int>(packetType) << std::endl;
                processHeader(client, length);
                switch (packetType)
                {
                case 1:
                {
                    sendConnack(client);
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
                default:
                    break;
                }
                
                readPayload(client);
            }
            else {
                std::cerr << "Read error: " << ec.message() << std::endl;
                handleClientDisconnect(client);
            }
        });
}

void MqttServer::sendSuback(std::shared_ptr<MqttClient> client)
{
    uint16_t packetIdentifier = 1234;
    std::vector<uint8_t> qosLevels = { 0x00, 0x01 };
    // Construct SUBACK packet
    std::vector<uint8_t> subackPacket;
    subackPacket.push_back(0x90);  // Fixed Header
    subackPacket.push_back(packetIdentifier >> 8);  // MSB of Packet Identifier
    subackPacket.push_back(packetIdentifier & 0xFF);  // LSB of Packet Identifier
    subackPacket.insert(subackPacket.end(), qosLevels.begin(), qosLevels.end());  // QoS Levels
    subackPacket.push_back(0x00);

    // Send SUBACK packet
    boost::asio::async_write(client->socket(), boost::asio::buffer(subackPacket.data(), subackPacket.size()),
        [client](boost::system::error_code ec, std::size_t /*length*/) {
            if (ec) {
                std::cerr << "Write error to client: " << ec.message() << std::endl;
            }
            else {
                std::cout << "Send SUBACK to client\n";
            }
        });
    //boost::asio::write(client->socket(), boost::asio::buffer(subackPacket));
    //const char connackPacket[] = { 0x00 };
    //boost::asio::write(client->socket(), boost::asio::buffer(connackPacket, sizeof(connackPacket)));
}

void MqttServer::sendConnack(std::shared_ptr<MqttClient> client)
{
    try {
        const char connackPacket[] = { 0x20, 0x02, 0x00, 0x00 };
        boost::asio::write(client->socket(), boost::asio::buffer(connackPacket, sizeof(connackPacket)));
        std::cout << "Send CONNACK to client\n";
    }
    catch (const std::exception& e) {
        std::cerr << "Exception during Connack packet send: " << e.what() << std::endl;
    }
}

void MqttServer::readPayload(std::shared_ptr<MqttClient> client)
{
    client->payload().clear();
    // Resize the payload buffer to the expected length
    client->payload().resize(client->payloadLength());
    if (client->socket().is_open()) {
        boost::asio::async_read(client->socket(), boost::asio::buffer(client->payload(), client->payloadLength()),
            [this, client](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::cout << "Received buffer (raw): ";
                    for (std::size_t i = 0; i < length; ++i) {
                        std::cout << static_cast<int>(client->payload()[i]) << " ";
                    }
                    std::cout << std::endl;
                    processPayload(client, length);
                    readHeader(client);
                }
                else {
                    std::cerr << "Read error: " << ec.message() << std::endl;
                    handleClientDisconnect(client);
                }
            });
    }
}

void MqttServer::handleMqttMessage(std::shared_ptr<MqttClient> client, const std::string& message)
{
    
}

void MqttServer::handleMqttPublish(std::shared_ptr<MqttClient> sender, const std::string& topic, const std::string& publishMessage)
{
    std::vector<uint8_t> testpacket;
    testpacket.push_back(0x30);  // Packet Type: Publish with QoS 0
    testpacket.push_back(static_cast<uint8_t>(topic.size() >> 8));  // MSB of topic length
    testpacket.push_back(static_cast<uint8_t>(topic.size() & 0xFF));  // LSB of topic length
    // Append topic bytes to the packet
    for (char c : topic) {
        testpacket.push_back(static_cast<uint8_t>(c));
    }
    // Append payload bytes to the packet
    for (char c : publishMessage) {
        testpacket.push_back(static_cast<uint8_t>(c));
    }
    testpacket.push_back(0x00);

    //const char f[] = { 0x82, 0x0E, 0x00, 0x01, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x01 };
    // Send the Publish packet
    for (const auto& client : clients_) {
        if (client != sender) {
            for (int i = 0; i < client->subscribedTopics().size(); i++) {
                if (client->subscribedTopics()[i].topic == topic) {
                    boost::asio::async_write(client->socket(), boost::asio::buffer(testpacket.data(), testpacket.size()),
                        [client](boost::system::error_code ec, std::size_t /*length*/) {
                            if (ec) {
                                std::cerr << "Write error to client: " << ec.message() << std::endl;
                            }
                            else {
                                std::cout << "Message sent to client." << std::endl;
                            }
                        });
                }
            }
        }
    }
}

void MqttServer::handleClientDisconnect(std::shared_ptr<MqttClient> client)
{
    clients_.erase(client);
    std::cout << "Client disconnected." << std::endl;
}

void MqttServer::processHeader(std::shared_ptr<MqttClient> client, const std::size_t& length)
{
    // Extract payload length from the fixed header
    if (length >= 2) {
        client->payloadLength() = 0;
        uint8_t multiplier = 1;
        size_t index = 1;

        do {
            if (index >= length) {
                std::cerr << "Invalid Remaining Length encoding." << std::endl;
                // Handle error as needed
                return;
            }

            client->payloadLength() += (static_cast<uint8_t>(client->header()[index]) & 0x7F) * multiplier;
            multiplier *= 128;
            ++index;
        } while ((static_cast<uint8_t>(client->header()[index - 1]) & 0x80) != 0);
    }
    else {
        // Invalid header length
        std::cerr << "Invalid header length." << std::endl;
        // Handle error as needed
    }
}

void MqttServer::processPayload(std::shared_ptr<MqttClient> client, const std::size_t& length)
{
    if (length > 0) {
        // Process received data here
        std::vector<char> received_data(client->payload().begin(), client->payload().begin() + length);

        std::cout << "Received header: " << static_cast<int>(client->header()[0]) << std::endl;
        std::cout << "Received payload: ";
        for (char c : client->payload()) {
            std::cout << static_cast<int>(static_cast<unsigned char>(c)) << " ";
        }
        std::cout << std::endl;

        MqttParser parser(received_data);
        // Check if it's an MQTT Connect packet
        if (type == PacketType::CONNECT) {
            //received_data[0] = 0x10; // hardcode Connect because its not getting it from the header_ for some reason..
            std::cout << "Received MQTT CONNECT packet" << std::endl;
            connectPacket = parser.parseConnect();
            //std::string parserStr = parser->toString();
            //std::cout << parserStr << "\n";
        }
        else if (type == PacketType::SUBSCRIBE) {
            std::cout << "Received SUBSCRIBE packet\n";
            subscribePacket = parser.parseSubscription(received_data);
            client->subscribe(subscribePacket);
            //subscribePacket = parser.parseSubscription();
            std::cout << "Topic: " << subscribePacket.topic << " QoS: " << (int)subscribePacket.qosLevel << "\n";
            sendSuback(client);
            /*for (const auto& subscription : subscriptions) {
                std::cout << "Topic: " << subscription.topic << ", QoS: " << static_cast<int>(subscription.qosLevel) << std::endl;
            }*/

        }
        else if (type == PacketType::PUBLISH) {
            std::cout << "Received PUBLISH packet\n";
            publishPacket = parser.parsePublish(received_data);
            handleMqttPublish(client, "test", "hello");
        }
        else {
            std::cerr << "Unknown data: ";
            for (char c : received_data) {
                std::cerr << static_cast<int>(static_cast<unsigned char>(c)) << " ";
            }
            std::cerr << std::endl;
        }
    }
}
