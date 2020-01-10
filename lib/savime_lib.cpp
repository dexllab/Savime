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
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <list>
#include <fstream>
#include <sys/types.h>
#include <arpa/inet.h>   
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <sys/un.h>
#include <vector>
#include <../lib/protocol.h>
#include <../lib/savime_lib.h>

#define __BUFSIZE 4096
#define __PATHSIZE 1024
#define __TMP_FILES_PATH "/dev/shm/"
#define __INVALID_SOCKET -1
#define ASSERT_RETURN(R)                                                       \
  if (R == SAV_FAILURE)                                                        \
  return SAV_FAILURE
#define ASSERT_RETURN_READ_BLOCKS(R)                                           \
  if (R == SAV_FAILURE)                                                        \
  return SAV_ERROR_READING_BLOCKS
#define ASSERT_CONN_OPEN(C)                                                    \
  if (!C.opened)                                                               \
  return SAV_FAILURE

char __UNIX_SOCKET_ADDRESS[__BUFSIZE] = "/tmp/savime-socket";

std::vector<std::string> split(const std::string &str, const char &ch) {
  std::string next;
  std::vector<std::string> result;

  for (std::string::const_iterator it = str.begin(); it != str.end(); it++) {
    if (*it == ch) {
      if (!next.empty()) {
        result.push_back(next);
        next.clear();
      }
    } else {
      next += *it;
    }
  }

  if (!next.empty())
    result.push_back(next);

  return result;
}

std::ifstream::pos_type get_file_size(const char *filename) {
  std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
  return in.tellg();
}

// UTIL FUNCTIONS
int savime_connect(const char *unix_socket_address) {
  struct sockaddr_in serv_addr{};
  struct sockaddr_un serv_addr_un{};
  struct hostent *server;

  int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("Error opening socket");
    return __INVALID_SOCKET;
  }

  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr_un.sun_family = AF_UNIX;

  strcpy(serv_addr_un.sun_path, unix_socket_address);

  if (connect(sockfd, (struct sockaddr *)&serv_addr_un, sizeof(serv_addr_un)) <
      0) {
    perror("Error connecting");
    return __INVALID_SOCKET;
  }

  return sockfd;
}

int savime_connect(int portno, const char *address) {
  // int sockfd, portno, n;
  struct sockaddr_in serv_addr{};
  struct sockaddr_un serv_addr_un{};
  struct hostent *server;

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);

  if (sockfd < 0) {
    perror("Error opening socket");
    return SAV_FAILURE;
  }

  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;

  serv_addr.sin_port = htons(portno);
  serv_addr.sin_addr.s_addr = inet_addr(address);

  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("Error connecting");
    return SAV_FAILURE;
  }

  return sockfd;
}

int savime_receive(int socket, char *buffer, size_t size) {
  off64_t total_received = 0, to_receive = size;

  while (to_receive > 0) {
    off64_t received = recv(socket, &buffer[total_received], size, 0);

    if (received == 0)
      break;

    if (received < 0) {
      perror("Error receiving data.");
      return SAV_FAILURE;
    }

    total_received += received;
    to_receive -= received;
  }

  return SAV_SUCCESS;
}

int savime_receive(int socket, int file, size_t size) {
  off_t input_offset = 0, output_offset = 0;
  off_t len;
  size_t total_transferred = 0;
  int transferred_bits;
  int pipe_descriptors[2];
  size_t buffer_size = __BUFSIZE;

  if (pipe(pipe_descriptors) < 0) {
    perror("Error creating pipes.");
    return SAV_FAILURE;
  }

  while (total_transferred < size) {
    size_t partial_transfer = 0;
    if ((size - total_transferred) < buffer_size)
      buffer_size = (size - total_transferred);

    while (partial_transfer < buffer_size) {
      size_t to_transfer = buffer_size - partial_transfer;
      transferred_bits = splice(socket, NULL, pipe_descriptors[1], NULL,
                                to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE);

      if (transferred_bits < 0) {
        perror("Problem during splice operation.");
        return SAV_FAILURE;
      }
      partial_transfer += transferred_bits;
    }

    partial_transfer = 0;

    while (partial_transfer < buffer_size) {
      size_t to_transfer = buffer_size - partial_transfer;
      transferred_bits = splice(pipe_descriptors[0], NULL, file, NULL,
                                to_transfer, SPLICE_F_MOVE | SPLICE_F_MORE);
      if (transferred_bits < 0) {
        perror("Problem during splice operation.");
        return SAV_FAILURE;
      }
      partial_transfer += transferred_bits;
    }

    total_transferred += buffer_size;
  }

  close(pipe_descriptors[0]);
  close(pipe_descriptors[1]);

  return SAV_SUCCESS;
}

int savime_send(int socket, const char *buffer, size_t size) {
  off64_t total_send = 0, to_send = size;

  while (to_send > 0) {
    size_t send = write(socket, &buffer[total_send], to_send);

    if (send < 0) {
      perror("Error while sending data.");
      return SAV_FAILURE;
    }

    total_send += send;
    to_send -= send;
  }

  return SAV_SUCCESS;
}

int savime_send(int socket, int file, size_t size) {
  ssize_t total_send = 0, to_send = size;

  while (to_send > 0) {
    ssize_t send = sendfile64(socket, file, (off64_t *)&total_send, to_send);

    if (send < 0) {
      perror("Error while sending data from file.");
      return SAV_FAILURE;
    }

    to_send -= send;
  }

  return SAV_SUCCESS;
}

int savime_wait_ack(int socket) {
  MessageHeader header;
  return savime_receive(socket, (char *)&header, sizeof(MessageHeader));
}

int savime_get_appendable_file(QueryResultHandle &result_handle,
                               char *block_name) {
  std::string sblock_name(block_name);

  if (result_handle.descriptors.find(sblock_name) !=
      result_handle.descriptors.end()) {
    return result_handle.descriptors[sblock_name];
  }

  char path[__PATHSIZE] = "\0";
  strncat(path, __TMP_FILES_PATH, strlen(__TMP_FILES_PATH));
  strncat(path, block_name, strlen(block_name));
  remove(path);

  int file = open(path, O_CREAT | O_RDWR, 0666);
  if (file < 0) {
    perror(path);
    return SAV_FAILURE;
  }

  result_handle.descriptors[sblock_name] = file;
  result_handle.files[sblock_name] = path;
  return file;
}

int send_query(SavimeConn &connection, const char *query) {
  ASSERT_CONN_OPEN(connection);

  int ret, query_length = strlen(query);

  // Create header
  MessageHeader header;
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_CREATE_QUERY_REQUEST, 0, NULL,
              NULL);

  // Send message and wait response
  ret =
      savime_send(connection.socketfd, (char *)&header, sizeof(MessageHeader));
  ASSERT_RETURN(ret);

  ret = savime_receive(connection.socketfd, (char *)&header,
                       sizeof(MessageHeader));
  ASSERT_RETURN(ret);

  // Storing queryid returned by server
  connection.queryid = header.queryid;

  // Send query
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_SEND_QUERY_TXT, query_length + 1,
              NULL, NULL);

  ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
  ASSERT_RETURN(ret);
  ret = savime_send(connection.socketfd, query, query_length + 1);
  ASSERT_RETURN(ret);
  ret = savime_wait_ack(connection.socketfd);
  ASSERT_RETURN(ret);

  // Send query done signal
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_SEND_QUERY_DONE, 0, NULL, NULL);

  ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
  ASSERT_RETURN(ret);

  ret = savime_wait_ack(connection.socketfd);
  ASSERT_RETURN(ret);

  return SAV_SUCCESS;
}

int send_query_params(SavimeConn &connection, FileBufferSet file_buffer_set) {
  ASSERT_CONN_OPEN(connection);

  // send param data
  MessageHeader header;
  int ret;

  for (int i = 0; i < file_buffer_set.num_files; i++) {
    // Send Param Request
    init_header(header, connection.clientid, connection.queryid,
                connection.message_count++, C_SEND_PARAM_REQUEST, 0, NULL,
                NULL);

    ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
    ASSERT_RETURN(ret);

    ret = savime_wait_ack(connection.socketfd);
    ASSERT_RETURN(ret);

    int file = open(file_buffer_set.files[i], O_RDONLY);
    if (file < 0) {
      perror("Could not open file");
      init_header(header, connection.clientid, connection.queryid,
                  connection.message_count++, C_CLOSE_CONNECTION, 0, NULL,
                  NULL);

      ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
      return SAV_FAILURE;
    }

    // Send Param Data
    init_header_block(header, connection.clientid, connection.queryid,
                      connection.message_count++, C_SEND_PARAM_DATA,
                      file_buffer_set.file_sizes[i],
                      file_buffer_set.set_name[i], 1, NULL, NULL);

    ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
    ASSERT_RETURN(ret);

    ret = savime_send(connection.socketfd, file, file_buffer_set.file_sizes[i]);
    close(file);
    ASSERT_RETURN(ret);

    ret = savime_wait_ack(connection.socketfd);
    ASSERT_RETURN(ret);

    // Send param done
    init_header(header, connection.clientid, connection.queryid,
                connection.message_count++, C_SEND_PARAM_DONE, 0, NULL, NULL);

    ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
    ASSERT_RETURN(ret);
    ret = savime_wait_ack(connection.socketfd);
    ASSERT_RETURN(ret);
  }

  return SAV_SUCCESS;
}

int send_result_request(SavimeConn &connection) {
  ASSERT_CONN_OPEN(connection);

  // Create header
  MessageHeader header;
  int ret;

  // Request result from server
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_RESULT_REQUEST, 0, NULL, NULL);

  ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
  ASSERT_RETURN(ret);

  ret = savime_wait_ack(connection.socketfd);
  ASSERT_RETURN(ret);

  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_ACK, 0, NULL, NULL);

  ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
  ASSERT_RETURN(ret);

  return SAV_SUCCESS;
}

int receive_query(SavimeConn &connection, QueryResultHandle &result_handle) {
  ASSERT_CONN_OPEN(connection);

  // Create header
  MessageHeader header;
  int ret;

  // Starting processing query result
  ret = savime_receive(connection.socketfd, (char *)&header,
                       sizeof(MessageHeader));
  ASSERT_RETURN(ret);

  result_handle.response_text = (char *)malloc(header.payload_length);
  ret = savime_receive(connection.socketfd, result_handle.response_text,
                       header.payload_length);
  ASSERT_RETURN(ret);

  result_handle.is_schema = (header.type == S_SEND_SCHEMA) ? 1 : 0;
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_ACK, 0, NULL, NULL);

  ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
  ASSERT_RETURN(ret);

  return SAV_SUCCESS;
}

int receive_query_data(SavimeConn &connection,
                       QueryResultHandle &result_handle) {
  ASSERT_CONN_OPEN(connection);

  // Create header
  MessageHeader header;
  int ret;

  // Reading response
  while (true) {
    ret = savime_receive(connection.socketfd, (char *)&header,
                         sizeof(MessageHeader));
    ASSERT_RETURN(ret);

    if (header.type == S_RESPONSE_END) {
      break;
    }

    if (header.payload_length != 0) {
      int file = savime_get_appendable_file(result_handle, header.block_name);
      ret = savime_receive(connection.socketfd, file, header.payload_length);
      ASSERT_RETURN(ret);
    }

    // Send ACK
    init_header(header, connection.clientid, connection.queryid,
                connection.message_count++, C_ACK, 0, NULL, NULL);

    ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
    ASSERT_RETURN(ret);
  }

  return SAV_SUCCESS;
}

int receive_response_end(SavimeConn &connection) {
  MessageHeader header;
  return savime_receive(connection.socketfd, (char *)&header,
                        sizeof(MessageHeader));
}

int has_file_parameters(const char *query, FileBufferSet *file_buffer_set) {
  size_t i = 0, position = 0;
  std::string text(query);
  std::list<std::string> files;

  while (true) {
    size_t start = text.find("\"", position);
    if (start == std::string::npos)
      break;
    size_t end = text.find("\"", start + 1);
    if (end == std::string::npos)
      break;
    position = end + 1;

    std::string param = text.substr(start + 1, end - start - 1);
    if (param.at(0) == '@') {
      files.push_back(param);
    }
  }

  file_buffer_set->num_files = files.size();
  file_buffer_set->files = (char **)malloc(sizeof(char *) * files.size());
  file_buffer_set->file_sizes = (size_t *)malloc(sizeof(size_t) * files.size());
  file_buffer_set->set_name = (char **)malloc(sizeof(char *) * files.size());

  for (auto &f : files) {
    file_buffer_set->files[i] = (char *)malloc(sizeof(char) * (f.length() + 1));
    strncpy(file_buffer_set->files[i], &f.c_str()[1], f.length());
    file_buffer_set->file_sizes[i] = get_file_size(file_buffer_set->files[i]);
    std::string param_name = f;
    file_buffer_set->set_name[i] =
        (char *)malloc(sizeof(char) * (param_name.length() + 1));
    strncpy(file_buffer_set->set_name[i], param_name.c_str(),
            param_name.length() + 1);
    i++;
  }

  return files.size();
}

/*return 0 if there are no blocks left and 1 otherwise. Returns -1 on error*/
int read_query_block(SavimeConn &connection, QueryResultHandle &result_handle) {
  ASSERT_CONN_OPEN(connection);

  // Create header
  MessageHeader header;
  int ret;

  for (auto fileEntry : result_handle.files) {
    if (remove(fileEntry.second.c_str()) == -1)
      perror("Could not remove file");
  }
  result_handle.descriptors.clear();
  result_handle.files.clear();

  for (int i = 0; i < result_handle.schema.size(); i++) {
    ret = savime_receive(connection.socketfd, (char *)&header,
                         sizeof(MessageHeader));
    ASSERT_RETURN_READ_BLOCKS(ret);

    if (header.type == S_RESPONSE_END) {
      return SAV_NO_MORE_BLOCKS;
    }

    if (header.type == S_SEND_TEXT) {
      result_handle.response_text = (char *)malloc(header.payload_length);
      ret = savime_receive(connection.socketfd, result_handle.response_text,
                           header.payload_length);
      ASSERT_RETURN_READ_BLOCKS(ret);

      init_header(header, connection.clientid, connection.queryid,
                  connection.message_count++, C_ACK, 0, NULL, NULL);

      ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
      ASSERT_RETURN_READ_BLOCKS(ret);

      return SAV_ERROR_RESPONSE_BLOCKS;
    } else {
      if (header.payload_length != 0) {
        int file = savime_get_appendable_file(result_handle, header.block_name);
        ret = savime_receive(connection.socketfd, file, header.payload_length);
        ASSERT_RETURN_READ_BLOCKS(ret);
      }

      // Send ACK
      init_header(header, connection.clientid, connection.queryid,
                  connection.message_count++, C_ACK, 0, NULL, NULL);

      ret = savime_send(connection.socketfd, (char *)&header, sizeof(header));
      ASSERT_RETURN_READ_BLOCKS(ret);
    }
  }

  return SAV_BLOCKS_LEFT;
}

int parse_schema(QueryResultHandle &result_handle) {
  if (result_handle.response_text[0] == SCHEMA_IDENTIFIER_CHAR) {
    result_handle.is_schema = 1;
    std::string schema_string(&result_handle.response_text[1]);
    std::vector<std::string> schema_elements = split(schema_string, '|');

    for (auto schema_element : schema_elements) {
      SavimeDataElement dataElement;
      std::vector<std::string> schema_detais = split(schema_element, ',');
      dataElement.name = schema_detais[0];
      dataElement.is_dimension = schema_detais[1].c_str()[0] == 'd' ? 1 : 0;
      std::vector<std::string> type = split(schema_detais[2], ':');

      if (!strcmp(type[0].c_str(), "char")) {
        dataElement.type = SAV_CHAR;
      } else if (!strcmp(type[0].c_str(), "int8")) {
        dataElement.type = SAV_INT8;
      } else if (!strcmp(type[0].c_str(), "int16")) {
        dataElement.type = SAV_INT16;
      } else if (!strcmp(type[0].c_str(), "int32") ||
                 !strcmp(type[0].c_str(), "int")) {
        dataElement.type = SAV_INT32;
      } else if (!strcmp(type[0].c_str(), "int64") ||
                 !strcmp(type[0].c_str(), "long")) {
        dataElement.type = SAV_INT64;
      } else if (!strcmp(type[0].c_str(), "uint8")) {
        dataElement.type = SAV_UINT8;
      } else if (!strcmp(type[0].c_str(), "uint16")) {
        dataElement.type = SAV_UINT16;
      } else if (!strcmp(type[0].c_str(), "uint32")) {
        dataElement.type = SAV_UINT32;
      } else if (!strcmp(type[0].c_str(), "uint64")) {
        dataElement.type = SAV_UINT64;
      } else if (!strcmp(type[0].c_str(), "float")) {
        dataElement.type = SAV_FLOAT;
      } else if (!strcmp(type[0].c_str(), "double")) {
        dataElement.type = SAV_DOUBLE;
      }
      if (type.size() > 1) {
        dataElement.type.length = strtol(type[1].c_str(), NULL, 10);
      } else {
        dataElement.type.length = 1;
      }

      result_handle.schema[dataElement.name] = dataElement;
    }
  } else {
    result_handle.is_schema = 0;
  }

  return SAV_SUCCESS;
}

void dispose_file_buffer_set(FileBufferSet *file_buffer_set) {
  for (int i = 0; i < file_buffer_set->num_files; i++) {
    free(file_buffer_set->files[i]);
    free(file_buffer_set->set_name[i]);
  }

  free(file_buffer_set->files);
  free(file_buffer_set->file_sizes);
  free(file_buffer_set->set_name);
}

void ___savime_change_default_unix_socket(char *address) {
  int32_t len = strlen(address);

  if (len < __BUFSIZE) {
    strcpy(__UNIX_SOCKET_ADDRESS, address);
  }
}

// LIB FUNCTIONS
SavimeConn open_connection(int port, const char *address) {
#define ASSERT_RETURN_OPEN_CONN(R, C)                                          \
  if (R == __INVALID_SOCKET) {                                                 \
    C.opened = false;                                                          \
    return C;                                                                  \
  }

  SavimeConn connection;
  MessageHeader header;
  connection.message_count = 0;
  int ret;

  if (address[0] == '\0') {
    connection.socketfd = savime_connect(__UNIX_SOCKET_ADDRESS);
  } else {
    connection.socketfd = savime_connect(port, address);
  }

  ASSERT_RETURN_OPEN_CONN(connection.socketfd, connection);

  init_header(header, 0, 0, connection.message_count++, C_CONNECTION_REQUEST, 0,
              NULL, NULL);

  ret =
      savime_send(connection.socketfd, (char *)&header, sizeof(MessageHeader));
  ASSERT_RETURN_OPEN_CONN(ret, connection);

  ret = savime_receive(connection.socketfd, (char *)&header,
                       sizeof(MessageHeader));
  ASSERT_RETURN_OPEN_CONN(ret, connection);
  connection.clientid = header.clientid;

  /*Setting connection status to open.*/
  connection.opened = true;

  return connection;
}

QueryResultHandle execute(SavimeConn &connection, const char *query) {

#define RETURN_UNSUCCESSFUL(Q)                                                 \
  {                                                                            \
    Q.successful = false;                                                      \
    return Q;                                                                  \
  }

  MessageHeader header;
  FileBufferSet file_buffer_set;
  QueryResultHandle result_handle;
  result_handle.successful = true;

  if (!connection.opened)
    RETURN_UNSUCCESSFUL(result_handle);

  // Send query
  if (!send_query(connection, query))
    RETURN_UNSUCCESSFUL(result_handle);

  if (has_file_parameters(query, &file_buffer_set)) {
    // printf(RED "---Sending params  %s\n" RESET, connection.status().c_str());
    if (!send_query_params(connection, file_buffer_set))
      RETURN_UNSUCCESSFUL(result_handle);
  }

  dispose_file_buffer_set(&file_buffer_set);

  // printf(RED"---Sending result request  %s\n" RESET,
  // connection.status().c_str());
  // Send result request
  if (!send_result_request(connection))
    RETURN_UNSUCCESSFUL(result_handle);

  // printf(RED "---Receive query response  %s\n" RESET,
  // connection.status().c_str());
  // Send query response
  if (!receive_query(connection, result_handle))
    RETURN_UNSUCCESSFUL(result_handle);

  // printf(RED "---Parsing schema %s\n" RESET, connection.status().c_str());
  // Parsing schema from text response
  parse_schema(result_handle);

  // if there is no more data, read end response
  if (result_handle.is_schema == 0) {
    // printf(RED "---Receive end of response  %s\n" RESET,
    // connection.status().c_str());
    if (!receive_response_end(connection))
      RETURN_UNSUCCESSFUL(result_handle);
  }

  return result_handle;
}

void dispose_query_handle(QueryResultHandle &queryHandle) {
  for (auto entry : queryHandle.descriptors)
    close(entry.second);

  for (auto entry : queryHandle.files)
    remove(entry.second.c_str());
}

void close_connection(SavimeConn &connection) {
  if(!connection.opened)
    return;
  
  MessageHeader header;
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, C_CLOSE_CONNECTION, 0, NULL, NULL);

  savime_send(connection.socketfd, (char *)&header, sizeof(header));
  connection.opened = false;
}

void shutdown_savime(SavimeConn &connection) {
  if(!connection.opened)
    return;
  
  MessageHeader header;
  init_header(header, connection.clientid, connection.queryid,
              connection.message_count++, SHUTDOWN_SIGNAL, 0, NULL, NULL);
  savime_send(connection.socketfd, (char *)&header, sizeof(header));
  savime_wait_ack(connection.socketfd);
  connection.opened = false;  
}
