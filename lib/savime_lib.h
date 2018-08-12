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
#ifndef SAVIME_LIB_H
#define SAVIME_LIB_H

#include <map>
#include <netdb.h>

#define SAV_FAILURE 0
#define SAV_SUCCESS 1
#define SAV_NO_MORE_BLOCKS 0
#define SAV_BLOCKS_LEFT 1
#define SAV_ERROR_READING_BLOCKS -1
#define SAV_ERROR_RESPONSE_BLOCKS -2

/**
 * Struct storing data about the current connection.
 */
 struct SavimeConn {
  int socketfd;      /*!<File descriptor for the socket. */
  int clientid;      /*!<Client identifier. */
  int queryid;       /*!<Query identifier. */
  int message_count; /*!<Message counter for message numbering. */
  bool opened;       /*!<Flag indicating if connection is still opened. */

  SavimeConn() {
    socketfd = 0;
    clientid = 0;
    queryid = 0;
    message_count = 0;
    opened = false;
  }

  std::string status() {
    return "s=" + std::to_string(socketfd) + " c=" + std::to_string(clientid) +
           " q=" + std::to_string(queryid) + " m=" +
           std::to_string(message_count) + " o=" + std::to_string(opened);
  }
};

/**
 * Enum with possible data types for data elements in a TAR.
 */
enum SavimeEnumType {
  SAV_CHAR,
  SAV_INT8,
  SAV_INT16,
  SAV_INT32,
  SAV_INT64,
  SAV_UINT8,
  SAV_UINT16,
  SAV_UINT32,
  SAV_UINT64,
  SAV_FLOAT,
  SAV_DOUBLE,
  INVALID_TYPE
};


/**
 * Struct with data defining a data type.
 */
struct SavimeType {
  SavimeEnumType type; /*!<Type of the data element.*/
  int length;          /*!<Determine if its a scalar or vector. */

  inline SavimeType &operator=(SavimeEnumType arg) {
    length = 1;
    type = arg;
    return *this;
  }

  inline bool operator==(SavimeEnumType arg) { return type == arg; }

  inline bool operator!=(SavimeEnumType arg) { return type != arg; }

  inline bool operator==(SavimeType arg) {
    return type == arg.type && length == arg.length;
  }

  inline bool operator!=(SavimeType arg) {
    return type != arg.type || length == arg.length;
  }
};

/**
 * Struct with metadata for a data elements in a TAR.
 */
typedef struct {
  std::string name;  /*!<Data element name. */
  bool is_dimension; /*!<Flag indicating if it is a dimension or not. */
  SavimeType type;   /*!<Type of the data element.*/
} SavimeDataElement;

/**
 * Struct with metadata for a data elements in a TAR.
 */
struct QueryResultHandle {
  char *response_text; /*!<String with the response text. */
  bool is_schema;      /*!<Flag indicating if its a schema response. */
  bool successful; /*!<Flag indicating if query has been executed (even with an
                   * error has returned
                   * from the server). */
  std::map<std::string, int>
      descriptors; /*!<File descriptor for each data element. */
  std::map<std::string, std::string>
      files; /*!<File path for each data element. */
  std::map<std::string, SavimeDataElement>
      schema; /*!<Metadata info for each data element. */
};

/**
 * Struct storing data of a block response storing in memory buffers.
 */
struct BufferSet {
  char **buffers;    /*!<Vector of strings with buffer's pointers. */
  char **set_name;   /*!<Vector of strings with names for blocks. */
  int *buffer_sizes; /*!<Vector of ints with buffer sizes. */
  int num_buffers;   /*!<Number of buffers present in the response. */
};

/**
 * Struct storing data of a block response storing in files.
 */
struct FileBufferSet {
  char **files;       /*!<Vector of strings with file's paths. */
  char **set_name;    /*!<Vector of strings with names for blocks. */
  size_t *file_sizes; /*!<Vector of ints with file sizes. */
  int num_files;      /*!<Number of files present in the response. */
};

void ___savime_change_default_unix_socket(char *address);

/**
* Checks if there are files defined as parameters in the query to be sent to
* server.
* @param query is the string containing the query text.
* @param  file_buffer_set is a struct where file's info are ketp.
* @return An integer with number of file parameters present in the query.
*/
int has_file_parameters(char *query, FileBufferSet *file_buffer_set);

/**
* Creates a connection between the client and savime server.
* @param port must be set to the value of the port which savime's server is
* attached to.
* @param  address is a string used to define the savime's server address.
* @return SavimeConn a savime connection to be used to identify the connection.
*/
SavimeConn open_connection(int port, const char *address);

/**
* Executes a savime query.
* @param connection is the handler for an open server connection.
* @param query is the text for the query to be executed.
* @return a QueryResultHandle for the query result.
*/
QueryResultHandle execute(SavimeConn &connection, const char *query);

/**
* Reads a query block.
* @param connection is the handler for an open server connection.
* @param a QueryResultHandle for the query result.
* @return An integer with number of file parameters present in the query.
*/
int read_query_block(SavimeConn &connection, QueryResultHandle &result_handle);

/**
* Free all memory used by the QueryResultHandle.
* @param queryHandle is the QueryResultHandle to be disposed.
*/
void dipose_query_handle(QueryResultHandle &queryHandle);

/**
* Closes a connection with the savime server.
* @param conn is the SavimeConn to be closed.
*/
void close_connection(SavimeConn &conn);

/**
* Signal Savime server to shutdown.
* @param conn is the SavimeConn used to signal the server. Connection is closed
* afterwards.
*/
void shutdown_savime(SavimeConn &conn);

#endif /* SAVIME_LIB_H */
