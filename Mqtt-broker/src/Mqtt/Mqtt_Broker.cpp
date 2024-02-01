#include "Mqtt_Broker.h"
#include "MqttBrokerSession.h"

MqttBroker::MqttBroker(io_service& io_service, short port) 
    : acceptor_(io_service, ip::tcp::endpoint(ip::tcp::v4(), port)), socket_(io_service) {
    running = true;
    std::cout << "Mqtt-broker started!" << "\n";
    ListenForConnectionAsync();
}

void MqttBroker::Disconnect()
{
    running = false;
}

void MqttBroker::ListenForConnectionAsync()
{
    acceptor_.async_accept(socket_,
        [this](boost::system::error_code ec) {
            if (!ec) {
                std::cout << "New connection established." << std::endl;
                // Create a new session for the connection
                std::shared_ptr<ISession<std::string>> session = std::make_shared<MqttBrokerSession>(std::move(socket_));
                session->Start();
                //static_cast<MqttBrokerSession*>(session.get())->start();
                sessions.push_back(session);
                //std::make_shared<MqttBrokerSession>(std::move(socket_))->start();
            }

            ListenForConnectionAsync();
        });
}
