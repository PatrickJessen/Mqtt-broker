#pragma once
#include <iostream>
#include <vector>
#include <memory>
#include "../interfaces/IServer.h"
#include "boost/asio.hpp"

using namespace boost::asio;

class MqttBroker : public IServer
{
public:
    MqttBroker(io_service& io_service, short port);

private:
    virtual void Disconnect() override;
    virtual void ListenForConnectionAsync() override;

    ip::tcp::acceptor acceptor_;
    ip::tcp::socket socket_;
};