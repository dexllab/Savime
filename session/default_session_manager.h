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
#ifndef JOB_MANAGER_DEFAULT_H
#define JOB_MANAGER_DEFAULT_H

#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "../core/include/session.h"
#include "../core/include/engine.h"


class DefaultSessionManager : public SessionManager {
  int _sessionIdCounter = 1;
  std::map<int, std::shared_ptr<std::thread>> _threads;
  std::map<int, std::shared_ptr<Session>> _runningSessions;
  std::map<int, std::shared_ptr<Session>> _stoppedSessions;
  std::shared_ptr<std::thread> _thread;

  ConnectionManagerPtr _connectionManager;
  std::mutex _mutex;
  std::mutex _thread_mutex;
  std::condition_variable _conditionVar;
  EnginePtr _engine;
  ParserPtr _parser;
  OptimizerPtr _optimizer;
  MetadataManagerPtr _metadaManager;
  QueryDataManagerPtr _queryDataManager;

public:
  DefaultSessionManager(ConfigurationManagerPtr configurationManager,
                        SystemLoggerPtr systemLogger,
                        ConnectionManagerPtr connectionManager,
                        EnginePtr engine, ParserPtr parser,
                        OptimizerPtr optimizer,
                        MetadataManagerPtr metadaManager,
                        QueryDataManagerPtr queryDataManager)
      :

        SessionManager(configurationManager, systemLogger) {
    _connectionManager = connectionManager;
    _metadaManager = metadaManager;
    _parser = parser;
    _optimizer = optimizer;
    _engine = engine;
    _queryDataManager = queryDataManager;
  }

  ConnectionListenerPtr
  NotifyNewConnection(ConnectionDetailsPtr connectionDetails);
  void NotifyMessageArrival(ConnectionDetailsPtr connectionDetails);
  void SetEngine(EnginePtr engine);
  void SetParser(ParserPtr parser);
  void SetOptmizer(OptimizerPtr optimizer);
  void SetMetadaManager(MetadataManagerPtr metadaManager);

  SavimeResult Start();
  SavimeResult EndSession(SessionPtr job);
  SavimeResult EndAllSessions();
  SavimeResult End();
};

#endif /* JOB_MANAGER_DEFAULT_H */

