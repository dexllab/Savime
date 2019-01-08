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
#ifndef PROTOCOL_H
#define PROTOCOL_H
/*! \file */
#include <cstring>
#include <netdb.h>
#include <memory>

#define MAGIC_NO 0x42
#define NAME_LENGTH 256
#define SCHEMA_IDENTIFIER_CHAR '#'

/**
 * MessageType contains the codes for types of messages in the protocol
 * of communication between the client and the server.
 */
enum MessageType {
  // Clients Messages
  C_CONNECTION_REQUEST,   /*!<Code for client messages with connection requests.
                             */
  C_CREATE_QUERY_REQUEST, /*!<Code for client messages with query creation
                             requests. */
  C_SEND_QUERY_TXT,  /*!<Code for client messages with parts of query texts. */
  C_SEND_QUERY_DONE, /*!<Code for client messages for signalling the end of
                        query text transfer. */
  C_SEND_PARAM_REQUEST, /*!<Code for client messages with requests for sending
                           param. */
  C_SEND_PARAM_DATA,  /*!<Code for client messages with query parameter data. */
  C_SEND_PARAM_DONE,  /*!<Code for client messages for signalling the end of a
                         parameter data transfer. */
  C_RESULT_REQUEST,   /*!<Code for client messages for asking the server query
                         response. */
  C_CLOSE_CONNECTION, /*!<Code for client messages for closing connection. */
  C_ACK,              /*!<Code for client messages with an acknoledgement. */
  // Server Messages
  S_CONNECTION_ACCEPT, /*!<Code for server messages indicating connection
                          acceptance. */
  S_QUERY_ACCEPT, /*!<Code for server messages indicating query acceptance. */
  S_PARAM_ACCEPT, /*!<Code for server messages indicating param acceptance. */
  S_SEND_START_RESPONSE, /*!<Code for server messages indicating that the server
                            started to send the query response. */
  S_SEND_TEXT,   /*!<Code for server messages containing textual query response.
                    */
  S_SEND_SCHEMA, /*!<Code for server messages containing result schema. */
  S_SEND_BIN_BLOCK_INITIAL, /*!<Code for server messages containing the initial
                               block of a responde schema. */
  S_SEND_BIN_BLOCK,         /*!<Code for server messages containing a block of a
                               responde schema. */
  S_SEND_BIN_BLOCK_FINAL,   /*!<Code for server messages containing the final
                               block of a responde schema. */
  S_RESPONSE_END, /*!<Code for server messages indicating the end of a query
                     response. */
  S_ACK,          /*!<Code for server messages with an acknoledgement.*/
  // Invalid
  SHUTDOWN_SIGNAL, /*!<Code for signaling SAVIME server shutdown.*/
  TYPE_INVALID /*!<Code indicating invalid messages.*/
};

/**
 * Server State contains codes for the status of the server during a query
 * processing.
 */
enum ServerState {
  WAIT_CONN,           /*!<Idle server job waiting a connection.*/
  WAIT_QUERY,          /*!<Connected server job waiting a query.*/
  RECEIVE_QUERY,       /*!<Server job is in process of receiveing query text.*/
  WAIT_PARAM,          /*!<Server job has received the query and is waiting for
                          parameters.*/
  RECEIVE_PARAM,       /*!<Server job is in process of receiving params.*/
  PROCESS_QUERY,       /*!<Server job is processing a query.*/
  SEND_RESPONSE,       /*!<Server job is sending the response text.*/
  SEND_RESPONSE_BLOCK, /*!<Server job is sending a response block..*/
  WAIT_ACK,            /*!<Server job is waiting an acknoledgement.*/
  INVALID,             /*!<Invalid server job state.*/
  DONE /*!<Server job has finished and is thread is exiting soon.*/
};

/**
 * MessageHeader is the struct containing the basic info transferred
 * between the client and the server.
 */
struct __attribute__((__packed__)) MessageHeader {
  char magic; /*!<Magic number for message header validation. It must be 0x42.*/
  char protocol_version; /*!<Protocol version, reserved for future use. Default
                            is 0x01.*/
  int key;               /*!<Reserved for future use.*/
  int msg_number;        /*!<Number of message exchanged during communicatio.*/
  int clientid;          /*!<Server attributed client id.*/
  int queryid;           /*!<Server attributed query id.*/
  enum MessageType type; /*!<Code for message type.*/
  size_t total_length;   /*!<Total length of the message: Header+payload..*/
  size_t
      payload_length; /*!<Size of the payload that follows the message header.*/
  char block_name[NAME_LENGTH]; /*!<Used for block transfer messages. C string
                                   with the block name.*/
  int block_num; /*!<Used for block transfer messages. Server attributed block
                    number being transfered.*/
};
typedef std::shared_ptr<MessageHeader> MessageHeaderPtr;

inline void init_header(MessageHeader &x, int c, int q, int m,
                        enum MessageType t, size_t l, const char *h,
                        const char *s) {
  x.magic = 0x42;
  x.protocol_version = 0x01;
  x.key = 0x00;
  x.clientid = c;
  x.queryid = q;
  x.msg_number = m;
  x.type = t;
  x.payload_length = l;
  x.total_length = l + sizeof(MessageHeader);
}

inline void init_header_block(MessageHeader &x, int c, int q, int m,
                              enum MessageType t, size_t l, const char *n,
                              int b, const char *h, const char *s) {
  init_header(x, c, q, m, t, l, h, s);
  size_t len = strlen(n) + 1;
  len = (len < NAME_LENGTH) ? len : NAME_LENGTH;
  memcpy(x.block_name, n, len);
  x.block_num = b;
}

// RDMA stuff
enum {
  use_rdma = 0,
  max_writers = 4,
  max_backlog = 10,
  max_file_name = 16,
  max_buffer_len = 1 << 28,
};

enum RdmaOp { R_SERVER_RECV, R_SERVER_SEND };

struct __attribute__((__packed__)) request {
  int query_id;
  int client_id;
  RdmaOp op;
};

struct __attribute__((__packed__)) remote_region {
  uint64_t addr;
  uint32_t rkey;
};

#endif
