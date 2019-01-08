#include <utility>

#ifndef CONNECTION_MANAGER_H
#define CONNECTION_MANAGER_H
/*! \file */
#include "savime.h"
#include "../lib/protocol.h"

using namespace std;

class ConnectionListener;
typedef ConnectionListener* ConnectionListenerPtr;

/**
 * Stores connection info of a message used for transferring data with
 * ConnectionManager.
 */
struct ConnectionDetails {
  int32_t socket; /*!<Socket used in the communication.*/
  int32_t port;   /*!<Port used in the communication.*/
  string address; /*!<Address of the client.*/
};
typedef std::shared_ptr<ConnectionDetails>
    ConnectionDetailsPtr; //!< ConnectionDetails pointer.

/**
 * Stores data about the payload of message used for transferring data with
 * ConnectionManager.
 */
struct Payload {
  int32_t
      is_in_file; /*!<Flag specifying if data is in a file (true) or is the
                     memory buffer indicated in data (false).*/
  int32_t file_descriptor; /*!<Descriptor for the file where data should be
                              written to or read from depending on the
                              operation.*/
  int64_t
      size; /*!<Size in bytes of the file pointed by file_descriptor or the
               buffer pointed to by data.*/
  char *
      data; /*!<Pointer to the memory buffer in which data should be read from
               or written to depending on the operation.*/
};
typedef std::shared_ptr<Payload> PayloadPtr; //!< Payload pointer.

/**
 * Message stores info necessary for sending and receiving data with
 * ConnectionManager.
 */
struct Message {
  ConnectionDetailsPtr
      connection_details; /*!< Information about the connection to be usd for
                             transferring data.*/
  PayloadPtr payload;     /*!< Structure containing infomartion about the actual
                             data to be transferred.*/
};

typedef std::shared_ptr<Message> MessagePtr;

/**
 * ConnectionListener is an interface used to interact with the
 * ConnectionManager
 * class. A ConnectionListener can subscribe to a ConnectionManager to be
 * informed
 * whenever a new connection or message arrived.
 */
class ConnectionListener {
  int64_t _id; /*!< Id given by ConnectionManager when subscribing.*/

public:
  /**
  * Sets the Id of a ConnectionListener.
  * @param id a integer used as the listener's identifier.
  */
  void SetListenerId(int64_t id) { _id = id; }

  /**
  * Gets the listeners Id.
  * @return An integer containing the listener's identifier.
  */
  int64_t GetListenerId() { return _id; }

  /**
  * Notifies the listener a new connection has arrived.
  * @param connectionDetails a structure with the details of newly arrived
  * connection.
  * @return A pointer to a ConnectionListener that shall be notified for any new
  * message that arrives for the new connection.
  */
  virtual ConnectionListenerPtr
  NotifyNewConnection(ConnectionDetailsPtr connectionDetails) = 0;

  /**
  * Notifies the listener a new message has arrived.
  * @param connectionDetails a structure with the connection details for the
  * newly arrived message.
  */
  virtual void NotifyMessageArrival(ConnectionDetailsPtr connectionDetails) = 0;
};

/**
 * ConnectionManager is responsible for listening to incoming connections/
 * messages and to post ConnectionListeners. The ConnectionManager also offers
 * functions to send and receive data, either from a memory buffer or in a file.
 */
class ConnectionManager : public SavimeModule {

public:
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  ConnectionManager(ConfigurationManagerPtr configurationManager,
                    SystemLoggerPtr systemLogger)
      : SavimeModule("Connection Manager", std::move(configurationManager), systemLogger) {
  }

  /**
  * Starts the ConnectionManager instance.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Start() = 0;

  /**
  * Add a new listener to be posted whenever a new connection arrives.
  * @param listener is the listener to be posted when new connections arrive.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult
  AddConnectionListener(ConnectionListenerPtr listener) = 0;

  /**
  * Add a new listener to be posted whenever a new message arrives on the
  * socket.
  * @param listener is the listener to be posted when new messages arrive.
  * @param socket is the socket for which listener subscribes to.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult AddConnectionListener(ConnectionListenerPtr listener,
                                             int32_t socket) = 0;

  /**
  * Unsubscribe a listener from posts of new connections and new messages.
  * @param listener is the listener to unsubscribed.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult
  RemoveConnectionListener(ConnectionListenerPtr listener) = 0;

  /**
  * Creates a new message with the given connection details.
  * @param connectionDetails is the struct containing information about the
  * message connection.
  * @return a MessagePtr with the newly created message.
  */
  virtual MessagePtr CreateMessage(ConnectionDetailsPtr connectionDetails) = 0;

  /**
  * Sends data according to what is specified in the message parameter.
  * @param MessagePtr contains all details about the message that is to be sent.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Send(MessagePtr message) = 0;

  /**
  * Receives data according to what is specified in the message parameter.
  * @param MessagePtr contains all details about the message that is to be
  * received.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Receive(MessagePtr message) = 0;

  /**
  * Closes a connection and stops listening to the underlying socket.
  * @param connectionDetails contains the socket to be closed.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Close(ConnectionDetailsPtr connectionDetails) = 0;

  /**
  * Stops the ConnectionManager .
  * @param connectionDetails contains the socket to be closed.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Stop() = 0;
};
typedef std::shared_ptr<ConnectionManager>
    ConnectionManagerPtr; //!< C

#endif


