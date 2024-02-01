#pragma once
#include <string>
#include <vector>

/// <summary>
/// 
/// </summary>
/// <typeparam name="T">Datatype to send and receive (Currently only handling string)</typeparam>
template <typename T>
class ISession
{
public:
	virtual ~ISession() = default;
	virtual void Start() = 0;
	virtual void SendMessageAsync(const std::string& message) = 0;
	virtual void ReadMessageAsync() = 0;
	/// <summary>
	/// Send read request to session
	/// If request was sucessfull, session is still alive
	/// Otherwise session has disconnected
	/// </summary>
	virtual void IsAlive() = 0;
	virtual const bool GetIsAlive() = 0;
};