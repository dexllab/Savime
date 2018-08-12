/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef DEFAULT_JOB_H
#define DEFAULT_JOB_H

#include <mutex>
#include <condition_variable>

#include "../core/include/session.h"
#include "../lib/protocol.h"

class DefaultSession : public Session,
                       public ConnectionListener,
                       public EngineListener {
  static int queryIdCounter;
  static int clientIdCounter;
  static std::mutex global_mutex;
  static std::mutex id_mutex;

  int _id;
  std::shared_ptr<DefaultSession> _this;
  std::condition_variable _conditionVar;
  int _associated_socket, _currentQuery, _currentClient;
  bool _serverJobHasBeenNotified = false, _active = true;
  bool _engineHasNotified = false;
  SessionManager *_sessionManager;
  QueryDataManagerPtr _queryDataManager;
  ConnectionDetailsPtr _currentConnection;
  ConfigurationManagerPtr _configurationManager;
  ConnectionManagerPtr _connectionManager;
  SystemLoggerPtr _systemLogger;
  EnginePtr _engine;
  ParserPtr _parser;
  OptimizerPtr _optimizer;
  MetadataManagerPtr _metadaManager;
  ServerState _serverState;

  int GetNextQueryId();
  int GetNextClientId();
  void SendMessage(MessageHeaderPtr header,
                   ConnectionDetailsPtr connectionDetails, char *content);
  void ReadHeader(ConnectionDetailsPtr connectionDetails,
                  MessageHeaderPtr messageHeader);
  void SendAck(ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header);
  void WaitAck(ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header);
  void Wait();
  void Terminate();

  void HandleConnectionRequest(ConnectionDetailsPtr connectionDetails,
                               MessageHeaderPtr header);
  void HandleCreateQueryResquest(ConnectionDetailsPtr connectionDetails,
                                 MessageHeaderPtr header);
  void HandleSendQueryText(ConnectionDetailsPtr connectionDetails,
                           MessageHeaderPtr header);
  void HandleSendQueryDone(ConnectionDetailsPtr connectionDetails,
                           MessageHeaderPtr header);
  void HandleSendParamRequest(ConnectionDetailsPtr connectionDetails,
                              MessageHeaderPtr header);
  void HandleSendParamData(ConnectionDetailsPtr connectionDetails,
                           MessageHeaderPtr header);
  void HandleSendParamDone(ConnectionDetailsPtr connectionDetails,
                           MessageHeaderPtr header);
  void HandleResultRequest(ConnectionDetailsPtr connectionDetails,
                           MessageHeaderPtr header);
  void HandleAck(ConnectionDetailsPtr connectionDetails,
                 MessageHeaderPtr header);
  void HandleCloseConnection(ConnectionDetailsPtr connectionDetails,
                             MessageHeaderPtr header);
  void HandleInvalid(ConnectionDetailsPtr connectionDetails,
                     MessageHeaderPtr header);
  void HandleShutdonwSignal(ConnectionDetailsPtr connectionDetails, 
                     MessageHeaderPtr header);
  void HandleMessage(ConnectionDetailsPtr connectionDetails);

public:
  DefaultSession(int id, ConnectionDetailsPtr details,
                 SessionManager *sessionManager,
                 ConnectionManagerPtr connectionManager,
                 ConfigurationManagerPtr configurationManager,
                 SystemLoggerPtr systemLogger, EnginePtr engine,
                 ParserPtr parser, OptimizerPtr optimizer,
                 MetadataManagerPtr metadaManager,
                 QueryDataManagerPtr queryDataManager) {
    _id = id;
    _serverState == WAIT_CONN;
    _currentConnection = details;
    _associated_socket = details->socket;
    _sessionManager = sessionManager;
    _configurationManager = configurationManager;
    _connectionManager = connectionManager;
    _systemLogger = systemLogger;
    _engine = engine;
    _parser = parser;
    _optimizer = optimizer;
    _metadaManager = metadaManager;
    _queryDataManager = queryDataManager;
  }

  void SetThisPtr(std::shared_ptr<DefaultSession> thisPtr);
  int GetId();
  int NotifyTextResponse(std::string text);
  int NotifyNewBlockReady(std::string blockName, int32_t file_descriptor,
                          int64_t size, bool isFirst, bool isLast);
  void NotifyWorkDone();
  ConnectionListenerPtr
  NotifyNewConnection(ConnectionDetailsPtr connectionDetails);
  void NotifyMessageArrival(ConnectionDetailsPtr connectionDetails);
  void Run();
};



#endif /* DEFAULT_JOB_H */

