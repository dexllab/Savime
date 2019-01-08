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
#ifndef DEFAULT_CONNECTION_MANAGER_H
#define DEFAULT_CONNECTION_MANAGER_H

#include <list>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <vector>
#include <condition_variable>
#include "../core/include/connection_manager.h"

#define _2GB 2147483647
#define BUFSIZE 4096

using namespace std;

class DefaultConnectionManager : public ConnectionManager {

  list<int32_t> _sockets_to_close;
  unordered_map<int, ConnectionListenerPtr> _listeners;
  unordered_map<int32_t, ConnectionListenerPtr> _socket_listeners_map;
  vector<int32_t> _sockets;
  unordered_map<int32_t, list<int32_t>> _socket_listeners;
  shared_ptr<thread> _connections_thread;
  shared_ptr<thread> _messages_thread;
  mutex _mutex;
  condition_variable _conditionVar;
  int32_t _unix_socket;
  int32_t _tcp_socket;
  int32_t _max_pending_connections;
  int32_t _listeners_id;

  SavimeResult StartUnixMasterSocket();
  SavimeResult StartTCPMasterSocket();
  SavimeResult RunConnectionsLoop();
  SavimeResult RunMessagesLoop();
  SavimeResult NoLockAddConnectionListener(ConnectionListener *listener,
                                           int32_t socket);
  SavimeResult SplicedCopy(int32_t file, int32_t socket, size_t size,
                           int64_t *transferred);
  void removeConnection(int32_t socket);

public:
  DefaultConnectionManager(ConfigurationManagerPtr configurationManager,
                           SystemLoggerPtr systemLogger);
  SavimeResult Start() override;
  SavimeResult AddConnectionListener(ConnectionListener *listener) override;
  SavimeResult AddConnectionListener(ConnectionListener *listener, int socket) override;
  SavimeResult RemoveConnectionListener(ConnectionListener *listener) override;
  MessagePtr CreateMessage(ConnectionDetailsPtr connectionDetails) override;
  SavimeResult Send(MessagePtr message) override;
  SavimeResult Receive(MessagePtr message) override;
  SavimeResult Close(ConnectionDetailsPtr connectionDetails) override;
  SavimeResult Stop() override;
};


#endif /* DEFAULT_CONNECTION_MANAGER_H */

