#include "MqttBrokerSession.h"

static bool firstConnect = true;

void MqttBrokerSession::ReadMessageAsync()
{
    
}

void MqttBrokerSession::Start()
{
    header_ = std::vector<char>(1024);
    ReadHeader();
}

void MqttBrokerSession::SendMessageAsync(const std::string& message)
{
    try
    {
        auto self(shared_from_this());
        async_write(socket_, buffer(message, message.size()),
            [this, self](boost::system::error_code ec, std::size_t length) {
                // Nothing to do after writing
            });
    }
    catch (const boost::system::system_error& e) {
        std::cerr << "Network Error: " << e.what() << "\n";
    }
    catch (const std::runtime_error& e) {
        std::cerr << "Runtime Error: " << e.what() << "\n";
    }
    catch (const std::exception& e) {
        std::cerr << "General Exception: " << e.what() << "\n";
    }
}

void MqttBrokerSession::IsAlive()
{
}

const bool MqttBrokerSession::GetIsAlive()
{
	return false;
}

void MqttBrokerSession::ReadPayload()
{
    payload_ = std::vector<char>(payload_length_);
    auto self(shared_from_this());
    async_read(socket_, buffer(payload_, payload_length_),
        [this, self](boost::system::error_code ec, std::size_t length) {
            if (!ec) {
                ProcessMessage(length);
                ReadHeader();
            }
            else {
                std::cerr << "Read payload error: " << ec.message() << std::endl;
            }
        });
}

void MqttBrokerSession::ReadHeader()
{
    auto self(shared_from_this());
    async_read(socket_, buffer(header_, 2),
        [this, self](boost::system::error_code ec, std::size_t length) {
            if (!ec) {
                ProcessHeader(length);
                ReadPayload();
            }
            else {
                std::cerr << "Read error: " << ec.message() << std::endl;
            }
        });
}

void MqttBrokerSession::ProcessMessage(std::size_t length)
{
    if (length > 0) {
        // Process received data here
        std::vector<char> received_data(payload_.begin(), payload_.begin() + length);

        // Check if it's an MQTT packet
        if (received_data.size() >= 2 && (received_data[0] & 0xF0) == 0x40) {
            // It's a PUBLISH packet
            std::cout << "Received MQTT PUBLISH packet" << std::endl;
            
            // Extract topic and payload
            std::string topic(received_data.begin() + 2, received_data.end());
            
            // Implement your logic to handle the received MQTT packet
            std::cout << "Topic: " << topic << std::endl;
        } else {
            std::cerr << "Unknown data: ";
            for (char c : received_data) {
                std::cerr << static_cast<int>(static_cast<unsigned char>(c)) << " ";
            }
            std::cerr << std::endl;
        }
    }
}

void MqttBrokerSession::ProcessHeader(std::size_t length)
{
    if (length == 2) {
        // Extract payload length from the fixed header
        payload_length_ = (static_cast<uint8_t>(header_[1]) & 0x7F);
    }
}
