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
#include <unistd.h>
#include <condition_variable>
#include <stdexcept>
#include <string.h> 
#include <chrono> 
#include "default_session.h"
#include "../lib/protocol.h"

#define SESSION_ID "Session "+std::to_string(_id)

using namespace std;
using namespace chrono;

SavimeTime t1, t2;

/*
 * Static declarations
 */
int DefaultSession::queryIdCounter = 0;
int DefaultSession::clientIdCounter = 0;
std::mutex DefaultSession::id_mutex;
std::mutex DefaultSession::global_mutex;

void DefaultSession::SetThisPtr(std::shared_ptr<DefaultSession> ownPtr) {
  _this = std::move(ownPtr);
}

int DefaultSession::GetId() { return _id; }

/*
 * Interface functions
 */
ConnectionListenerPtr
DefaultSession::NotifyNewConnection(ConnectionDetailsPtr connectionDetails) {
  return nullptr;
}

void DefaultSession::NotifyMessageArrival(
    ConnectionDetailsPtr connectionDetails) {
  if (_serverState != DONE && !_serverJobHasBeenNotified) {
    //_systemLogger->LogEvent(SESSION_ID, "Server notified.");
    _serverJobHasBeenNotified = true;

    if (_currentConnection == nullptr)
      _currentConnection = connectionDetails;
  }

  _conditionVar.notify_one();
}

int DefaultSession::NotifyTextResponse(std::string text) {
  // Session can only be notified by engine while its processing a query.
  if (_serverState == PROCESS_QUERY) {
    static MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();

    // convert string to char array
    char *ctext = (char *)malloc(sizeof(char) * text.length() + 1);
    if (ctext == nullptr) {
      _systemLogger->LogEvent(SESSION_ID, "Could not allocate more memory.");
      return SAVIME_FAILURE;
    }
    memcpy(ctext, text.c_str(), text.length() + 1);

    // Initialize header values
    init_header((*responseHeader), _currentClient, _currentQuery, 0,
                S_SEND_TEXT, strlen(ctext) + 1, nullptr, nullptr);

    // Send text result from engine
    SendMessage(responseHeader, _currentConnection, ctext);

    // Cleaning memory
    free(ctext);

    // Client must acknowledge that it received the text
    WaitAck(_currentConnection, responseHeader);

    return SAVIME_SUCCESS;
  } else {
    return SAVIME_FAILURE;
  }
}

int DefaultSession::NotifyNewBlockReady(std::string blockName,
                                        int32_t file_descriptor, int64_t size,
                                        bool isFirst, bool isLast) {
  // Server job can only be notified by engine while its processing a query
  if (_serverState == PROCESS_QUERY) {
#ifdef TIME
    GET_T1();
#endif

    MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();
    memset((char *)responseHeader.get(), 0, sizeof(MessageHeader));

    enum MessageType type;

    if (isFirst) {
      type = S_SEND_BIN_BLOCK_INITIAL;
    } else if (isLast) {
      type = S_SEND_BIN_BLOCK_FINAL;
    } else {
      type = S_SEND_BIN_BLOCK;
    }

    // Initialize header for block data
    init_header_block((*responseHeader), _currentClient, _currentQuery, 0, type,
                      size, blockName.c_str(), 0, nullptr, nullptr);
    SendMessage(responseHeader, _currentConnection, nullptr);

    // Last block might not contain any data.
    // It is sent to inform the block's ended.
    if (size != 0) {
      MessagePtr message = std::make_shared<Message>();
      PayloadPtr payload = std::make_shared<Payload>();
      message->connection_details = _currentConnection;
      message->payload = payload;
      message->payload->is_in_file = true;
      message->payload->file_descriptor = file_descriptor;
      message->payload->data = nullptr;
      message->payload->size = size;

      if (_connectionManager->Send(message) != SAVIME_SUCCESS) {
        _systemLogger->LogEvent(SESSION_ID,
                                "Error while sending engine data block!");
        return SAVIME_FAILURE;
      }
    }

    WaitAck(_currentConnection, responseHeader);

#ifdef TIME
    GET_T2();
    _systemLogger->LogEvent(SESSION_ID, "Send block took " +
                                            std::to_string(GET_DURATION()) +
                                            " ms.");
#endif

    return SAVIME_SUCCESS;
  } else {
    return SAVIME_FAILURE;
  }
}

void DefaultSession::NotifyWorkDone() {}

/*
 * Util functions
 */

int DefaultSession::GetNextQueryId() {
  DefaultSession::id_mutex.lock();
  int id = DefaultSession::queryIdCounter++;
  DefaultSession::id_mutex.unlock();
  return id;
}

int DefaultSession::GetNextClientId() {
  DefaultSession::id_mutex.lock();
  int id = DefaultSession::clientIdCounter++;
  DefaultSession::id_mutex.unlock();
  return id;
}

void DefaultSession::Terminate() {
  _active = false;

  if (_serverState != DONE) {
    _serverState = DONE;
    if (_connectionManager->Close(_currentConnection) != SAVIME_SUCCESS) {
      throw std::runtime_error("Error while closing connection.");
    }
  }

  _conditionVar.notify_one();
}

void DefaultSession::ReadHeader(ConnectionDetailsPtr connectionDetails,
                                MessageHeaderPtr messageHeader) {
  MessagePtr message = std::make_shared<Message>();
  PayloadPtr payload = std::make_shared<Payload>();
  message->connection_details = std::move(connectionDetails);
  payload->data = (char *)messageHeader.get();
  payload->size = sizeof(MessageHeader);
  message->payload = payload;

  if (_connectionManager->Receive(message) != SAVIME_SUCCESS) {
    throw std::runtime_error("Error while reading message header from client.");
  }

  if (message->payload->size == 0) {
    Terminate();
    messageHeader->type = TYPE_INVALID;
    return;
  }

  if (messageHeader->magic != MAGIC_NO) {
    throw std::runtime_error("Invalid header message.");
  }
}

void DefaultSession::SendMessage(MessageHeaderPtr header,
                                 ConnectionDetailsPtr connectionDetails,
                                 char *content) {
  MessagePtr message = std::make_shared<Message>();
  PayloadPtr payload = std::make_shared<Payload>();
  message->connection_details = std::move(connectionDetails);
  message->payload = payload;

  MessageHeader headerAux{};
  memset((char *)&headerAux, 0, sizeof(MessageHeader));
  strncpy(headerAux.block_name, header->block_name, sizeof(header->block_name));
  headerAux.block_num = header->block_num;
  headerAux.clientid = header->clientid;
  headerAux.key = header->key;
  headerAux.magic = header->magic;
  headerAux.msg_number = header->msg_number;
  headerAux.payload_length = header->payload_length;
  headerAux.protocol_version = header->protocol_version;
  headerAux.queryid = header->queryid;
  headerAux.total_length = header->total_length;
  headerAux.type = header->type;
  message->payload->data = (char *)&headerAux;
  message->payload->size = sizeof(MessageHeader);
  message->payload->is_in_file = false;

  // Send header first
  if (_connectionManager->Send(message) != SAVIME_SUCCESS) {
    throw std::runtime_error(
        "MessagePtr: " + std::to_string(header->msg_number) + " to client " +
        std::to_string(header->clientid) + " at socket " +
        std::to_string(_associated_socket) + " failed.");
  }

  // change payload to content
  payload->data = (char *)content;
  payload->size = header->payload_length;

  // Send content second (if content is null, nothing is send)
  if (payload->data != nullptr &&
      _connectionManager->Send(message) != SAVIME_SUCCESS) {
    throw std::runtime_error(
        "MessagePtr: " + std::to_string(header->msg_number) + " to client " +
        std::to_string(header->clientid) + " at socket " +
        std::to_string(_associated_socket) + " failed.");
  }
}

void DefaultSession::SendAck(ConnectionDetailsPtr connectionDetails,
                             MessageHeaderPtr header) {
  MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();
  init_header((*responseHeader), header->clientid, header->queryid,
              header->msg_number, S_ACK, 0, nullptr, nullptr);
  SendMessage(responseHeader, std::move(connectionDetails), nullptr);
}

void DefaultSession::WaitAck(ConnectionDetailsPtr connectionDetails,
                             MessageHeaderPtr header) {
  std::mutex lock;
  std::unique_lock<std::mutex> locker(lock);

  while (!_serverJobHasBeenNotified) {
    _conditionVar.wait(locker);
  }
  _serverJobHasBeenNotified = false;

  ReadHeader(std::move(connectionDetails), header);

  if (header->type != C_ACK) {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::Wait() {
  std::mutex lock;
  std::unique_lock<std::mutex> locker(lock);

  while (!_serverJobHasBeenNotified) {
    _conditionVar.wait(locker);
  }
  _serverJobHasBeenNotified = false;
}

/*
 *  PROTOCOL FUNCTIONS
 */
void DefaultSession::HandleConnectionRequest(
    ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header) {
  if (_serverState == WAIT_CONN) {
    //_systemLogger->LogEvent(SESSION_ID, "Received connection request.");

    header->clientid = GetNextClientId();
    _currentClient = header->clientid;
    int server_id = _configurationManager->GetIntValue(SERVER_ID);
    MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();
    init_header((*responseHeader), header->clientid, 0, header->msg_number + 1,
                S_CONNECTION_ACCEPT, 0, nullptr, nullptr);

    SendMessage(responseHeader, std::move(connectionDetails), nullptr);

    _serverState = WAIT_QUERY;
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleCreateQueryResquest(
    ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header) {
  MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();
  GET_T1_LOCAL();

  if (_serverState == WAIT_QUERY) {
    //_systemLogger->LogEvent(SESSION_ID, "Received query request.");

    if (header->clientid != _currentClient) {
      throw std::runtime_error("Invalid client.");
    }

    header->queryid = GetNextQueryId();
    _currentQuery = header->queryid;
    _queryDataManager->SetQueryId(_currentQuery);

    init_header((*responseHeader), header->clientid, header->queryid,
                header->msg_number + 1, S_QUERY_ACCEPT, 0, nullptr, nullptr);

    SendMessage(responseHeader, std::move(connectionDetails), nullptr);
    _serverState = RECEIVE_QUERY;
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleSendQueryText(ConnectionDetailsPtr connectionDetails,
                                         MessageHeaderPtr header) {
  if (_serverState == RECEIVE_QUERY) {
    //_systemLogger->LogEvent(SESSION_ID, "Receiving query text.");

    if (header->clientid != _currentClient) {
      throw std::runtime_error("Invalid client.");
    }

    if (header->queryid != _currentQuery) {
      throw std::runtime_error("Invalid query.");
    }

    int64_t max_buffer_size =
        _configurationManager->GetLongValue(MAX_TFX_BUFFER_SIZE);

    do {
      int64_t size = header->payload_length;

      if (size > max_buffer_size) {
        throw std::runtime_error("MessagePtr is larger than allowed buffer.");
      }

      char *buffer = (char *)malloc(sizeof(char) * size);

      if (buffer == nullptr) {
        throw std::runtime_error(
            "Could not allocate enough memory for processing request.");
      }

      MessagePtr message = std::make_shared<Message>();
      PayloadPtr payload = std::make_shared<Payload>();

      message->connection_details = connectionDetails;

      payload->data = buffer;
      payload->size = size;
      message->payload = payload;

      if (_connectionManager->Receive(message) != SAVIME_SUCCESS) {
        throw std::runtime_error("Failure while reading message from client.");
      }

      _queryDataManager->AddQueryTextPart(std::string(payload->data));
      free(buffer);

      SendAck(connectionDetails, header);
      Wait();

      ReadHeader(connectionDetails, header);

      if (header->type == C_SEND_QUERY_TXT) {
        continue;
      } else if (header->type == C_SEND_QUERY_DONE) {
        SendAck(connectionDetails, header);
        _serverState = WAIT_PARAM;
        break;
      } else {
        throw std::runtime_error(
            "Misbehaved client: Invalid connection request message.");
      }

    } while (true);
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleSendQueryDone(ConnectionDetailsPtr connectionDetails,
                                         MessageHeaderPtr header) {
  throw std::runtime_error(
      "Misbehaved client: Invalid connection request message.");
}

void DefaultSession::HandleSendParamRequest(
    ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header) {
  if (_serverState == WAIT_PARAM) {
    //_systemLogger->LogEvent(SESSION_ID, "Receiving param.");

    if (header->clientid != _currentClient) {
      throw std::runtime_error("Invalid client.");
    }

    if (header->queryid != _currentQuery) {
      throw std::runtime_error("Invalid query.");
    }

    SendAck(std::move(connectionDetails), header);
    _serverState = RECEIVE_PARAM;
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleSendParamData(ConnectionDetailsPtr connectionDetails,
                                         MessageHeaderPtr header) {
  if (_serverState == RECEIVE_PARAM) {
    if (header->clientid != _currentClient) {
      throw std::runtime_error("Invalid client.");
    }

    if (header->queryid != _currentQuery) {
      throw std::runtime_error("Invalid query.");
    }

    while (true) {
      MessagePtr messageHandle = std::make_shared<Message>();
      PayloadPtr payload = std::make_shared<Payload>();
      messageHandle->connection_details = _currentConnection;
      messageHandle->payload = payload;
      messageHandle->payload->data = nullptr;
      messageHandle->payload->size = header->payload_length;
      messageHandle->payload->file_descriptor =
          _queryDataManager->GetParamFile(header->block_name);
      messageHandle->payload->is_in_file = true;

      //_systemLogger->LogEvent(SESSION_ID, "Receiving param data block:
      //"+std::string(header->block_name)+".");
      if (_queryDataManager->RegisterTransferBuffer(header->payload_length) !=
          SAVIME_SUCCESS) {
        throw std::runtime_error("Insufficient space in transfer buffer: "
                                 "Increase transfer buffer max size.");
      }

      if (_connectionManager->Receive(messageHandle) != SAVIME_SUCCESS) {
        _queryDataManager->RemoveParamFile(header->block_name);
        throw std::runtime_error("Error while reading params");
      }
      //_systemLogger->LogEvent(SESSION_ID, "Receiving param data block2:
      //"+std::string(header->block_name)+".");

      // Send ack and wait for a new message
      SendAck(connectionDetails, header);
      Wait();

      // Read new message header
      ReadHeader(connectionDetails, header);

      if (header->type == C_SEND_PARAM_DATA) {
        continue;
      } else if (header->type == C_SEND_PARAM_DONE) {
        _serverState = WAIT_PARAM;
        SendAck(connectionDetails, header);
        break;
      } else {
        throw std::runtime_error(
            "Misbehaved client: Invalid connection request message.");
      }
    }
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleSendParamDone(ConnectionDetailsPtr connectionDetails,
                                         MessageHeaderPtr header) {
  throw std::runtime_error(
      "Misbehaved client: Invalid connection request message.");
}

void DefaultSession::HandleResultRequest(ConnectionDetailsPtr connectionDetails,
                                         MessageHeaderPtr header) {
  bool error = false;

  if (_serverState == WAIT_PARAM) {
    if (header->clientid != _currentClient) {
      throw std::runtime_error("Invalid client.");
    }

    if (header->queryid != _currentQuery) {
      throw std::runtime_error("Invalid query.");
    }

    MessageHeaderPtr responseHeader = std::make_shared<MessageHeader>();
    init_header((*responseHeader), header->clientid, header->queryid,
                header->msg_number + 1, S_SEND_START_RESPONSE, 0, nullptr, nullptr);
    SendMessage(responseHeader, connectionDetails, nullptr);

    WaitAck(connectionDetails, responseHeader);

    _serverState = PROCESS_QUERY;
    _systemLogger->LogEvent(SESSION_ID, "Processing query.");

    if (_parser->Parse(_queryDataManager) != SAVIME_SUCCESS ||
        _optimizer->Optimize(_queryDataManager) != SAVIME_SUCCESS ||
      _engine->Run(_queryDataManager, this) != SAVIME_SUCCESS) {
      error = true;
      NotifyTextResponse(_queryDataManager->GetErrorResponse());
    }

    for (auto param : _queryDataManager->GetParamsList()) {
      auto file = _queryDataManager->GetParamFile(param);
      auto filePath = _queryDataManager->GetParamFilePath(param);
      close(file);

      if (error)
        remove(filePath.c_str());
    }

    init_header((*responseHeader), header->clientid, header->queryid,
                header->msg_number, S_RESPONSE_END, 0, nullptr, nullptr);
    SendMessage(responseHeader, connectionDetails, nullptr);

    _queryDataManager->Release();
    _serverState = WAIT_QUERY;

    GET_T2_LOCAL();
    _systemLogger->LogEvent(SESSION_ID, "Total query processing time: " +
                                          std::to_string(GET_DURATION())+" ms");
  } else {
    throw std::runtime_error(
        "Misbehaved client: Invalid connection request message.");
  }
}

void DefaultSession::HandleAck(ConnectionDetailsPtr connectionDetails,
                               MessageHeaderPtr header) {
  throw std::runtime_error(
      "Misbehaved client: Invalid connection request message.");
}

void DefaultSession::HandleInvalid(ConnectionDetailsPtr connectionDetails,
                                   MessageHeaderPtr header) {
  throw std::runtime_error(
      "Misbehaved client: Invalid connection request message.");
}

void DefaultSession::HandleCloseConnection(
    ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header) {
  Terminate();
}

void DefaultSession::HandleShutdonwSignal(
    ConnectionDetailsPtr connectionDetails, MessageHeaderPtr header) {
  SendAck(std::move(connectionDetails), std::move(header));
  _systemLogger->LogEvent(SESSION_ID, "Client signalled Savime to shutdown.");
  _systemLogger->LogEvent(SESSION_ID, "Shutting down.");
  exit(EXIT_SUCCESS);
}

void DefaultSession::HandleMessage(ConnectionDetailsPtr connectionDetails) {
  try {
    MessageHeaderPtr header = std::make_shared<MessageHeader>();
    ReadHeader(connectionDetails, header);

    switch (header->type) {
    case C_CONNECTION_REQUEST:
      HandleConnectionRequest(connectionDetails, header);
      break;
    case C_CREATE_QUERY_REQUEST:
      HandleCreateQueryResquest(connectionDetails, header);
      break;
    case C_SEND_QUERY_TXT:
      HandleSendQueryText(connectionDetails, header);
      break;
    case C_SEND_QUERY_DONE:
      HandleSendQueryDone(connectionDetails, header);
      break;
    case C_SEND_PARAM_REQUEST:
      HandleSendParamRequest(connectionDetails, header);
      break;
    case C_SEND_PARAM_DATA:
      HandleSendParamData(connectionDetails, header);
      break;
    case C_SEND_PARAM_DONE:
      HandleResultRequest(connectionDetails, header);
      break;
    case C_RESULT_REQUEST:
      HandleResultRequest(connectionDetails, header);
      break;
    case C_CLOSE_CONNECTION:
      HandleCloseConnection(connectionDetails, header);
      break;
    case C_ACK:
      HandleAck(connectionDetails, header);
      break;
    case SHUTDOWN_SIGNAL:
      HandleShutdonwSignal(connectionDetails, header);
      break;
    }

  } catch (std::exception &e) {
    _systemLogger->LogEvent(SESSION_ID, e.what());

    try {
      _systemLogger->LogEvent(SESSION_ID, "Aborting connection.");
      Terminate();
    } catch (std::exception &subE) {
      _systemLogger->LogEvent(
          SESSION_ID, std::string("Problem while aborting: ") + subE.what());
    }
  }
}

/*
 *  RUN METHOD
 */
void DefaultSession::Run() {
  std::mutex lock;
  std::unique_lock<std::mutex> locker(lock);
  _serverState = WAIT_CONN;
  _systemLogger->LogEvent(SESSION_ID, "Session has started.");

  while (true) {
    // if _active flag is not set, exit main loop
    if (!_active)
      break;

    // Check if server is notified to avoid spurious notifications
    if (_serverJobHasBeenNotified) {
      _serverJobHasBeenNotified = false;
      HandleMessage(_currentConnection);
    }

    if (!_active)
      break;

    // wait until thread is awake
    _conditionVar.wait(locker);
  }

  _systemLogger->LogEvent(SESSION_ID, "Session has ended.");

  // cleaning up and notify session manager thread has finished
  _connectionManager->RemoveConnectionListener(this);
  _sessionManager->EndSession(this);
  _this = nullptr;
}