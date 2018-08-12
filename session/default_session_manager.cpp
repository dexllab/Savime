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
#include <thread>
#include <mutex>

#include "../core/include/savime.h"
#include "default_session_manager.h"
#include "default_session.h"


ConnectionListenerPtr DefaultSessionManager::NotifyNewConnection(
    ConnectionDetailsPtr connectionDetails) {
  std::mutex mutex;
  std::unique_lock<std::mutex> locker(mutex);
  std::shared_ptr<DefaultSession> newSession;
  QueryDataManagerPtr queryDataManager = _queryDataManager->GetInstance();

  // Creating new session
  newSession = std::shared_ptr<DefaultSession>(new DefaultSession(
      _sessionIdCounter++, connectionDetails, this, _connectionManager,
      _configurationManager, _systemLogger, _engine, _parser, _optimizer,
      _metadaManager, queryDataManager));

  newSession->SetThisPtr(newSession);

  _thread_mutex.lock();

  _systemLogger->LogEvent(
      "Session Manager",
      "Creating session " + std::to_string(_sessionIdCounter - 1) +
          " with socket " + std::to_string(connectionDetails->socket));

  // Starting thread for new session
  _thread = std::shared_ptr<std::thread>(
      new std::thread(&DefaultSession::Run, newSession));

  return newSession.get();
}

void DefaultSessionManager::NotifyMessageArrival(
    ConnectionDetailsPtr connectionDetails){};

void DefaultSessionManager::SetEngine(EnginePtr engine) { _engine = engine; }

void DefaultSessionManager::SetParser(ParserPtr parser) { _parser = parser; }

void DefaultSessionManager::SetOptmizer(OptimizerPtr optimizer) {
  _optimizer = optimizer;
}

void DefaultSessionManager::SetMetadaManager(MetadataManagerPtr metadaManager) {
  _metadaManager = metadaManager;
}

SavimeResult DefaultSessionManager::Start() {
  _thread = NULL;
  _connectionManager->AddConnectionListener(this);
  return SAVIME_SUCCESS;
}

SavimeResult DefaultSessionManager::EndSession(SessionPtr job) {
  _thread->detach();
  _thread = NULL;
  _thread_mutex.unlock();
  _conditionVar.notify_all();
  return SAVIME_SUCCESS;
}

SavimeResult DefaultSessionManager::EndAllSessions() { return SAVIME_SUCCESS; }

SavimeResult DefaultSessionManager::End() { return SAVIME_SUCCESS; }