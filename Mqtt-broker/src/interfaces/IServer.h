#pragma once
#include <string>
#include <memory>
#include "ISession.h"

class IServer
{
public:
	virtual ~IServer() = default;
	virtual void Disconnect() = 0;
	virtual void DisconnectSession(const std::shared_ptr<ISession<std::string>>& session) = 0;
	virtual void ListenForConnectionAsync() = 0;
public:
	std::vector<std::shared_ptr<ISession<std::string>>> sessions;
	bool running;
};