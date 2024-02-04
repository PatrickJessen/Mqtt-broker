#include "MqttServer.h"
#include "MqttConnectParser.h"
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
            startAccept();  // Accept the next connection
        });
}
//mosquitto_pub -p 1883 -t test -m "hello world"
//mosquitto_pub -h localhost -p 1883 -t "test" -q 0 -u your_username -P your_password --will-topic "will_topic" --will-payload "Will message" -m "" -d -V mqttv311
void MqttServer::readHeader(std::shared_ptr<MqttClient> client)
{
    boost::asio::async_read(client->socket(), boost::asio::buffer(client->header()),
        [this, client](boost::system::error_code ec, std::size_t length) {
            if (!ec && length > 0) {
                sendSuback(client);
                uint8_t packetType = (client->header()[0] & 0xF0) >> 4;
                std::cout << "Received MQTT Packet Type: " << static_cast<int>(packetType) << std::endl;
                processHeader(client, length);
                if (packetType == 1) {
                    type = PacketType::CONNECT;
                }
                else if (packetType == 8) {
                    type = PacketType::SUBSCRIBE;
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

    // Send SUBACK packet
    boost::asio::write(client->socket(), boost::asio::buffer(subackPacket));
}

void MqttServer::readPayload(std::shared_ptr<MqttClient> client)
{
    client->payload().clear();
    // Resize the payload buffer to the expected length
    client->payload().resize(12);
    if (client->socket().is_open()) {
        // Async read operation
        boost::asio::async_read(client->socket(), boost::asio::buffer(client->payload(), 12),
            [this, client](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::cout << "Received buffer (raw): ";
                    for (std::size_t i = 0; i < length; ++i) {
                        std::cout << static_cast<int>(client->payload()[i]) << " ";
                    }
                    std::cout << std::endl;
                    processPayload(client, length);
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
    // Implement MQTT message handling logic here
        // Extract the topic and payload from the message and handle subscriptions

        // For illustration purposes, let's assume the message is "SUBSCRIBE topic1" or "UNSUBSCRIBE topic1"
    if (message.substr(0, 9) == "SUBSCRIBE") {
        std::string topicToSubscribe = message.substr(10);
        client->subscribe(topicToSubscribe);
        std::cout << "Client subscribed to: " << topicToSubscribe << std::endl;
    }
    else if (message.substr(0, 11) == "UNSUBSCRIBE") {
        std::string topicToUnsubscribe = message.substr(12);
        client->unsubscribe(topicToUnsubscribe);
        std::cout << "Client unsubscribed from: " << topicToUnsubscribe << std::endl;
    }
    else {
        // Handle other MQTT messages, e.g., publish messages to subscribed clients
        //handleMqttPublish(client, message);
    }
}

void MqttServer::handleMqttPublish(std::shared_ptr<MqttClient> sender, const std::string& publishMessage)
{
    // Implement logic to publish the message to subscribed clients
    for (const auto& client : clients_) {
        if (client != sender && client->subscribedTopics().count("test") > 0) {
            boost::asio::async_write(client->socket(), boost::asio::buffer(publishMessage),
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
                // Invalid length encoding
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

        // Check if it's an MQTT Connect packet
        if (type == PacketType::CONNECT) {
            //received_data[0] = 0x10; // hardcode Connect because its not getting it from the header_ for some reason..
            std::cout << "Received MQTT CONNECT packet" << std::endl;
            MqttConnectParser* parser = new MqttConnectParser(received_data);
            packet = parser->parse();
            std::string parserStr = parser->toString();
            std::cout << parserStr << "\n";
        }
        else if (type == PacketType::SUBSCRIBE) {
            std::vector<SubscriptionOptions> subscriptions;

            // Assuming payload contains one or more subscription entries
            for (std::size_t i = 0; i < client->payload().size(); ) {
                SubscriptionOptions subscription;

                // Extract topic length (2 bytes)
                std::size_t topicLength = (client->payload()[i] << 8) + client->payload()[i + 1];
                i += 2;

                // Extract topic string
                subscription.topic.assign(client->payload().begin() + i, client->payload().begin() + i + topicLength);
                i += topicLength;

                // Extract QoS level (1 byte)
                subscription.qosLevel = client->payload()[i];
                i++;

                // Add the subscription to the vector
                subscriptions.push_back(subscription);
            }

            // Now 'subscriptions' vector contains Subscription objects
            // Do whatever you need with the parsed subscription information
            for (const auto& subscription : subscriptions) {
                std::cout << "Topic: " << subscription.topic << ", QoS: " << static_cast<int>(subscription.qosLevel) << std::endl;
            }
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
