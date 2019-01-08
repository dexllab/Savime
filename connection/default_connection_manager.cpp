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
#include <stdio.h>
#include <string.h>  
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>  
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/un.h>
#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <mutex>
#include <vector>
#include <memory>
#include <utility>
#include <condition_variable>
#include "default_connection_manager.h"
#include "../core/include/savime.h"
#include "../core/include/util.h"



using namespace std;

DefaultConnectionManager::DefaultConnectionManager(
    ConfigurationManagerPtr configurationManager, SystemLoggerPtr systemLogger)
    : ConnectionManager(std::move(configurationManager), std::move(systemLogger)) {
  _listeners_id = 0;
}

SavimeResult DefaultConnectionManager::StartUnixMasterSocket() {

  int opt = true;
  struct sockaddr_un serv_addr{};
  try {
    int serverId = _configurationManager->GetIntValue(SERVER_ID);
    std::string unixPath =
        _configurationManager->GetStringValue(SERVER_UNIX_PATH(serverId));

    // create a master socket
    if ((_unix_socket = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
      throw std::runtime_error("Socket creation failed: " +
                               std::string(strerror(errno)));
    }

    // set master socket to allow multiple connections
    if (setsockopt(_unix_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt,
                   sizeof(opt)) < 0 &&
        setsockopt(_unix_socket, SOL_SOCKET, SO_KEEPALIVE, (char *)&opt,
                   sizeof(opt)) < 0) {

      throw std::runtime_error("Socket configuration failed: " +
                               std::string(strerror(errno)));
    }

    // type of unix socket created
    serv_addr.sun_family = AF_UNIX;

    if (unixPath.empty()) {
      throw std::runtime_error("Unix Socket path not set");
    }

    // Creating socket file if it doesn't exist
    FILE *socketFile = fopen(unixPath.c_str(), "w");
    if (socketFile)
      fclose(socketFile);

    strcpy(serv_addr.sun_path, unixPath.c_str());

    // bind the socket to localhost port 8888
    if (unlink(serv_addr.sun_path) < 0 ||
        bind(_unix_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) <
            0) {
      throw std::runtime_error("Binding unix socket failed: " +
                               std::string(strerror(errno)));
    }

    return SAVIME_SUCCESS;
  } catch (std::runtime_error &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultConnectionManager::StartTCPMasterSocket() {

  int opt = true, port;
  struct sockaddr_in address{};

  try {
    int serverId = _configurationManager->GetIntValue(SERVER_ID);
    port = _configurationManager->GetIntValue(SERVER_PORT(serverId));

    if (port == 0) {
      throw std::runtime_error("No port number set");
    }

    // create a master socket
    if ((_tcp_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      throw std::runtime_error("Socket creation failed: " +
                               std::string(strerror(errno)));
    }

    // set master socket to allow multiple connections and enable keepalive
    if (setsockopt(_tcp_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt,
                   sizeof(opt)) < 0 &&
        setsockopt(_tcp_socket, SOL_SOCKET, SO_KEEPALIVE, (char *)&opt,
                   sizeof(opt)) < 0) {
      throw std::runtime_error("Socket configurantion failed: " +
                               std::string(strerror(errno)));
    }

    // type of socket created
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<uint16_t>(port));

    // bind the socket to localhost port
    if (bind(_tcp_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
      throw std::runtime_error("Binding socket failed: " +
                               std::string(strerror(errno)));
    }

    return SAVIME_SUCCESS;
  } catch (std::runtime_error &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultConnectionManager::RunConnectionsLoop() {
  try {
    // set of socket descriptors
    fd_set readfds;
    int new_socket, activity;
    int max_sd;

    auto * addrlen = (socklen_t *)malloc(sizeof(socklen_t));
    memset((char *)addrlen, 0, sizeof(socklen_t));
    auto * address = (sockaddr_in *)malloc(sizeof(sockaddr_in));
    memset((char *)address, 0, sizeof(sockaddr_in));
    _max_pending_connections =
        _configurationManager->GetIntValue(MAX_CONNECTIONS);
    
    if (StartTCPMasterSocket() == SAVIME_FAILURE) {
      throw std::runtime_error("Could not start tcp master socket!");
    }
    
    if (StartUnixMasterSocket() == SAVIME_FAILURE) {
      throw std::runtime_error("Could not start unix master socket!");
    }

    if (listen(_tcp_socket, _max_pending_connections) < 0) {
      throw std::runtime_error("Listening to tcp socket failed: " +
                               std::string(strerror(errno)));
    }

    if (listen(_unix_socket, _max_pending_connections) < 0) {
      throw std::runtime_error("Listening to unix socket failed: " +
                               std::string(strerror(errno)));
    }

    _systemLogger->LogEvent(this->_moduleName, "Waiting for connections.");

    while (true) {
      // Create Thread safe listeners
      std::list<ConnectionListenerPtr> threadSafeListeners;

      // clear the socket set
      FD_ZERO(&readfds);

      // add sockets to set
      FD_SET(_unix_socket, &readfds);
      FD_SET(_tcp_socket, &readfds);
      max_sd = _tcp_socket > _unix_socket ? _tcp_socket : _unix_socket;

      // wait indefinitely for an activity on one of the sockets
      activity = select(max_sd + 1, &readfds, nullptr, nullptr, nullptr);

      _mutex.lock();
      if ((activity < 0) && (errno != EINTR)) {
        throw std::runtime_error("Problem while waiting connections: " +
                                 std::string(strerror(errno)));
      }

      int active_socket = -1;

      if (FD_ISSET(_tcp_socket, &readfds)) {
        active_socket = _tcp_socket;
      }

      if (FD_ISSET(_unix_socket, &readfds)) {
        active_socket = _unix_socket;
      }

      // Incoming connection on a socket
      if (active_socket != -1 && _sockets.size() < _max_pending_connections) {
        if ((new_socket = accept(active_socket, (sockaddr *)address, addrlen)) <
            0) {
          throw std::runtime_error("Error during connection acceptance: " +
                                   std::string(strerror(errno)));
        }

        _systemLogger->LogEvent(this->_moduleName,
                                "New connection arrived. Socket:" +
                                    std::to_string(new_socket) + "  Address: " +
                                    std::string(inet_ntoa(address->sin_addr)));

        _sockets.push_back(new_socket);

        ConnectionDetailsPtr connDetails = make_shared<ConnectionDetails>();
        connDetails->socket = new_socket;
        connDetails->port = address->sin_port;
        connDetails->address = std::string(inet_ntoa(address->sin_addr));

        for (auto entry : _listeners) {
          threadSafeListeners.push_back(entry.second);
        }

        for (ConnectionListenerPtr &listener : threadSafeListeners) {
          _mutex.unlock();
          ConnectionListener *newConnectionListener =
              listener->NotifyNewConnection(connDetails);
          _mutex.lock();
          if (newConnectionListener)
            NoLockAddConnectionListener(newConnectionListener,
                                        connDetails->socket);
        }

        threadSafeListeners.clear();
      }
      _mutex.unlock();
    }

    free(address);
    free(addrlen);

    return SAVIME_SUCCESS;
  } catch (std::runtime_error &e) {
    _systemLogger->LogEvent(this->_moduleName,
                            std::string("Error: ") + e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultConnectionManager::RunMessagesLoop() {
  try {
    std::mutex lock;
    std::unique_lock<std::mutex> locker(lock);

    // set of socket descriptors
    fd_set readfds;
    int activity;
    int max_sd = 0;

    _max_pending_connections =
        _configurationManager->GetIntValue(MAX_CONNECTIONS);

    while (true) {
      struct timeval tv{};
      tv.tv_sec = 0;
      tv.tv_usec = 300;

      // Create thread safe listeners
      std::list<ConnectionListenerPtr> threadSafeListeners;

      // clear the socket set
      FD_ZERO(&readfds);

      if (_sockets.empty())
        _conditionVar.wait(locker);

      _mutex.lock();
      for (int i = 0; i < _sockets.size(); i++) {
        FD_SET(_sockets[i], &readfds);
        if (max_sd < _sockets[i])
          max_sd = _sockets[i];
      }
      _mutex.unlock();

      // wait for an activity on one of the sockets
      activity = select(max_sd + 1, &readfds, nullptr, nullptr, &tv);

      _mutex.lock();
      if ((activity < 0) && (errno != EINTR)) {
        throw std::runtime_error("Problem while waiting connections: " +
                                 std::string(strerror(errno)));
      }

      for (int i = 0; i < _sockets.size(); i++) {
        threadSafeListeners.clear();

        if (FD_ISSET(_sockets[i], &readfds)) {
          int bytes_available;
          ioctl(_sockets[i], FIONREAD, &bytes_available);

          ConnectionDetailsPtr connDetails = make_shared<ConnectionDetails>();
          connDetails->socket = _sockets[i];

          for (auto entry : _listeners) {
            threadSafeListeners.push_back(entry.second);
          }

          if (_socket_listeners.find(_sockets[i]) != _socket_listeners.end()) {
            for (int listerner_id : _socket_listeners[_sockets[i]]) {
              if (_socket_listeners_map.find(listerner_id) !=
                  _socket_listeners_map.end()) {
                threadSafeListeners.push_back(
                    _socket_listeners_map[listerner_id]);
              }
            }
          }

          for (ConnectionListenerPtr &listener : threadSafeListeners) {
            listener->NotifyMessageArrival(connDetails);
          }

          break;
        }
      }

      while (!_sockets_to_close.empty()) {
        close(_sockets_to_close.front());
        _sockets_to_close.pop_front();
      }
      _mutex.unlock();
    }

    return SAVIME_SUCCESS;

  } catch (std::runtime_error &e) {
    _systemLogger->LogEvent(this->_moduleName,
                            std::string("Error: ") + e.what());
    return SAVIME_FAILURE;
  }
}

void DefaultConnectionManager::removeConnection(int socket) {
  _mutex.lock();

  for (int i = 0; i < _sockets.size(); i++) {
    if (_sockets[i] == socket) {
      if (i != (_sockets.size() - 1)) {
        _sockets[i] = _sockets[_sockets.size() - 1];
      }
      _sockets.pop_back();

      _socket_listeners.erase(socket);
      break;
    }
  }

  _mutex.unlock();
}

SavimeResult DefaultConnectionManager::Start() {
  _connections_thread = std::make_shared<std::thread>(&DefaultConnectionManager::RunConnectionsLoop, this);
  _messages_thread = std::make_shared<std::thread>(&DefaultConnectionManager::RunMessagesLoop, this);
  return SAVIME_SUCCESS;
}

SavimeResult DefaultConnectionManager::NoLockAddConnectionListener(
    ConnectionListener *listener, int socket) {
  listener->SetListenerId(_listeners_id++);
  _socket_listeners_map[listener->GetListenerId()] = listener;

  if (_socket_listeners.find(socket) == _socket_listeners.end())
    _socket_listeners[socket] = std::list<int>();

  _socket_listeners[socket].push_back(listener->GetListenerId());

  _conditionVar.notify_all();
  return SAVIME_SUCCESS;
}

SavimeResult
DefaultConnectionManager::AddConnectionListener(ConnectionListener *listener) {
  _mutex.lock();
  listener->SetListenerId(_listeners_id++);
  _listeners[listener->GetListenerId()] = listener;
  _mutex.unlock();
  return SAVIME_SUCCESS;
}

SavimeResult
DefaultConnectionManager::AddConnectionListener(ConnectionListener *listener,
                                                int socket) {
  _mutex.lock();
  NoLockAddConnectionListener(listener, socket);
  _mutex.unlock();
  return SAVIME_SUCCESS;
}

SavimeResult DefaultConnectionManager::RemoveConnectionListener(
    ConnectionListener *listener) {
  _mutex.lock();
  _listeners.erase(static_cast<const int &>(listener->GetListenerId()));
  _socket_listeners_map.erase(static_cast<const int &>(listener->GetListenerId()));

  for (auto entry : _socket_listeners)
    entry.second.remove(listener->GetListenerId());

  _mutex.unlock();
  return SAVIME_SUCCESS;
}

SavimeResult DefaultConnectionManager::SplicedCopy(int file, int socket,
                                                   size_t size,
                                                   int64_t *transferred) {
  loff_t output_offset = 0;
  size_t total_transferred = 0;
  int transferred_bits;
  int pipe_descriptors[2];
  size_t buffer_size = BUFSIZE;

  try {
    if (pipe(pipe_descriptors) < 0) {
      throw std::runtime_error("Could not create pipes for splice operation: " +
                               std::string(strerror(errno)));
    }

    while (total_transferred < size) {
      size_t partial_transfer = 0;
      if ((size - total_transferred) < buffer_size)
        buffer_size = (size - total_transferred);

      while (partial_transfer < buffer_size) {
        size_t to_transfer = buffer_size - partial_transfer;
        transferred_bits = static_cast<int>(splice(socket, nullptr, pipe_descriptors[1], nullptr,
                                                   to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE));
        if (transferred_bits < 0) {
          throw std::runtime_error("Problem during splice operation: " +
                                   std::string(strerror(errno)));
        }

        partial_transfer += transferred_bits;
      }

      partial_transfer = 0;

      while (partial_transfer < buffer_size) {
        size_t to_transfer = buffer_size - partial_transfer;
        transferred_bits =
          static_cast<int>(splice(pipe_descriptors[0], nullptr, file, &output_offset, to_transfer,
                             SPLICE_F_MOVE | SPLICE_F_MORE));
        if (transferred_bits < 0) {
          throw std::runtime_error("Problem during splice operation: " +
                                   std::string(strerror(errno)));
        }
        partial_transfer += transferred_bits;
      }

      total_transferred += buffer_size;
    }

    *transferred = total_transferred;
    close(pipe_descriptors[0]);
    close(pipe_descriptors[1]);
    return SAVIME_SUCCESS;

  } catch (std::runtime_error& e) {
    close(pipe_descriptors[0]);
    close(pipe_descriptors[1]);
    _systemLogger->LogEvent(this->_moduleName,
                            std::string("Error: ") + e.what());
    return SAVIME_FAILURE;
  }
}

MessagePtr DefaultConnectionManager::CreateMessage(
    ConnectionDetailsPtr connectionDetails) {
  MessagePtr message = std::make_shared<Message>();
  message->connection_details = connectionDetails;
  return message;
}

SavimeResult DefaultConnectionManager::Send(MessagePtr messageHandle) {
  off64_t data_send = 1, total_data_send = 0;
  try {
    if (!messageHandle->payload->is_in_file) {
      while (data_send > 0 && total_data_send < messageHandle->payload->size) {
        off64_t to_read = messageHandle->payload->size - total_data_send;
        auto size = static_cast<size_t>(to_read > _2GB ? _2GB : to_read);

        data_send =
            send(messageHandle->connection_details->socket,
                 &messageHandle->payload->data[total_data_send], size, 0);

        if (data_send < 0) {
          throw std::runtime_error("Problem while sending data: " +
                                   std::string(strerror(errno)));
        }

        total_data_send += data_send;
      }

    } else {
      size_t offset = 0; // = total_data_send;
      while (data_send > -1 && total_data_send < messageHandle->payload->size) {

        data_send = sendfile64(
            messageHandle->connection_details->socket,
            messageHandle->payload->file_descriptor, (off64_t *)&offset,
            static_cast<size_t>(messageHandle->payload->size - total_data_send));

        if (data_send < 0) {
          throw std::runtime_error("Problem while sending data file: " +
                                   std::string(strerror(errno)));
        }

        total_data_send += data_send;
      }
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName,
                            std::string("Error: ") + e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultConnectionManager::Receive(MessagePtr messageHandle) {
  off64_t data_read = 1, total_data_read = 0;
  try {
    if (!messageHandle->payload->is_in_file) {
      while (data_read > 0 && total_data_read < messageHandle->payload->size) {
        auto to_read = static_cast<size_t>(messageHandle->payload->size - total_data_read);
        size_t size = to_read > _2GB ? _2GB : to_read;

        data_read = read(messageHandle->connection_details->socket,
                         &messageHandle->payload->data[total_data_read], size);

        if (data_read < 0) {
          throw std::runtime_error(
              "Problem while receiving data from socket " +
              std::to_string(messageHandle->connection_details->socket) + ": " +
              std::string(strerror(errno)));
        }

        total_data_read += data_read;
      }

      messageHandle->payload->size = total_data_read;
      return SAVIME_SUCCESS;
    } else {
      return SplicedCopy(messageHandle->payload->file_descriptor,
                         messageHandle->connection_details->socket,
                         static_cast<size_t>(messageHandle->payload->size),
                         &messageHandle->payload->size);
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName,
                            std::string("Error: ") + e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult
DefaultConnectionManager::Close(ConnectionDetailsPtr connectionDetails) {
  removeConnection(connectionDetails->socket);
  _mutex.lock();
  _sockets_to_close.push_front(connectionDetails->socket);
  _mutex.unlock();
  return SAVIME_SUCCESS;
}

SavimeResult DefaultConnectionManager::Stop() { return SAVIME_SUCCESS; }