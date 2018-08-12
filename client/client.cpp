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
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <sys/un.h>
#include <sys/mman.h> 
#include <getopt.h>
#include <fstream>
#include <sys/stat.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <list>
#include <algorithm>
#include <../core/include/symbols.h>
#include <../lib/protocol.h>
#include <../lib/savime_lib.h>

#define _BUFFER 50000
#define _BASE_WIDTH 19

size_t print_header(QueryResultHandle handle) {
  std::stringstream header;
  std::list<std::string> el;

  for (auto entry : handle.schema) {
    if (entry.second.is_dimension)
      el.push_back(entry.first);
  }

  for (auto entry : handle.schema) {
    if (!entry.second.is_dimension)
      el.push_back(entry.first);
  }

  for (auto entry : el) {
    int32_t len = handle.schema[entry].type.length;

    if (handle.schema[entry].type == SAV_CHAR) {
      header << "|" << std::right << std::setw(std::max(_BASE_WIDTH, len))
             << std::setfill(' ') << entry;
    } else {
      header << "|" << std::right << std::setw(_BASE_WIDTH * len + len)
             << std::setfill(' ') << entry;
    }
  }

  header << "|";

  std::string sheader = header.str();

  std::cout << std::string(sheader.length(), '-') << std::endl;
  std::cout << sheader << std::endl;
  std::cout << std::string(sheader.length(), '-') << std::endl;

  return sheader.length();
}

void print_block(QueryResultHandle &handle, size_t totalLineWidth) {
  int64_t entry_count = 0, minimal_count = 0;
  std::map<std::string, char *> buf_map;
  for (auto entry : handle.schema) {
    std::string name = entry.first;
    SavimeDataElement dataElement = entry.second;

    struct stat s;
    fstat(handle.descriptors[name], &s);

    buf_map[name] = (char *)mmap(0, s.st_size, PROT_READ, MAP_SHARED,
                                 handle.descriptors[name], 0);

    if (buf_map[name] == MAP_FAILED) {
      close(handle.descriptors[entry.first]);
      perror("Error while mapping file");
      return;
    }

    switch (dataElement.type.type) {
    case SAV_CHAR:
      entry_count = s.st_size / (sizeof(int8_t) * dataElement.type.length);
      break;
    case SAV_INT8:
      entry_count = s.st_size / (sizeof(int8_t) * dataElement.type.length);
      break;
    case SAV_INT16:
      entry_count = s.st_size / (sizeof(int16_t) * dataElement.type.length);
      break;
    case SAV_INT32:
      entry_count = s.st_size / (sizeof(int32_t) * dataElement.type.length);
      break;
    case SAV_INT64:
      entry_count = s.st_size / (sizeof(int64_t) * dataElement.type.length);
      break;
    case SAV_UINT8:
      entry_count = s.st_size / (sizeof(uint8_t) * dataElement.type.length);
      break;
    case SAV_UINT16:
      entry_count = s.st_size / (sizeof(uint16_t) * dataElement.type.length);
      break;
    case SAV_UINT32:
      entry_count = s.st_size / (sizeof(uint32_t) * dataElement.type.length);
      break;
    case SAV_UINT64:
      entry_count = s.st_size / (sizeof(uint64_t) * dataElement.type.length);
      break;
    case SAV_FLOAT:
      entry_count = s.st_size / (sizeof(float) * dataElement.type.length);
      break;
    case SAV_DOUBLE:
      entry_count = s.st_size / (sizeof(double) * dataElement.type.length);
      break;
    }

    if (minimal_count == 0 || entry_count < minimal_count) {
      minimal_count = entry_count;
    }
  }

  entry_count = minimal_count;

  std::list<std::string> el;

  for (auto entry : handle.schema) {
    if (entry.second.is_dimension)
      el.push_back(entry.first);
  }

  for (auto entry : handle.schema) {
    if (!entry.second.is_dimension)
      el.push_back(entry.first);
  }

#define PRINT_NUMERICAL_COLUMN(TYPE, ELEMENT, INDEX, LEN)                      \
  std::cout << "|";                                                            \
  for (int32_t _i = 0; _i < LEN; _i++) {                                       \
    std::cout << std::right << std::setw(_BASE_WIDTH) << std::setfill(' ')     \
              << std::fixed << std::showpoint << std::setprecision(4)          \
              << ((TYPE *)buf_map[ELEMENT])[INDEX * LEN + _i];                 \
    if (_i < LEN - 1 && LEN > 1)                                               \
      std::cout << "|";                                                        \
    else                                                                       \
      std::cout << " ";                                                        \
  }

#define PRINT_STRING_COLUMN(TYPE, ELEMENT, INDEX, LEN)                         \
  std::cout << "|";                                                            \
  for (int32_t _i = 0; _i < _BASE_WIDTH - LEN; _i++) {                         \
    std::cout << ' ';                                                          \
  }                                                                            \
  for (int32_t _i = 0; _i < LEN; _i++) {                                       \
    std::cout << ((char *)buf_map[ELEMENT])[INDEX * LEN + _i];                 \
  }

  for (int64_t i = 0; i < entry_count; i++) {
    for (auto element : el) {
      SavimeDataElement dataElement = handle.schema[element];

      switch (dataElement.type.type) {
      case SAV_CHAR:
        PRINT_STRING_COLUMN(char, element, i, dataElement.type.length);
        break;
      case SAV_INT8:
        PRINT_NUMERICAL_COLUMN(int8_t, element, i, dataElement.type.length);
        break;
      case SAV_INT16:
        PRINT_NUMERICAL_COLUMN(int16_t, element, i, dataElement.type.length);
        break;
      case SAV_INT32:
        PRINT_NUMERICAL_COLUMN(int32_t, element, i, dataElement.type.length);
        break;
      case SAV_INT64:
        PRINT_NUMERICAL_COLUMN(int64_t, element, i, dataElement.type.length);
        break;
      case SAV_UINT8:
        PRINT_NUMERICAL_COLUMN(uint8_t, element, i, dataElement.type.length);
        break;
      case SAV_UINT16:
        PRINT_NUMERICAL_COLUMN(uint16_t, element, i, dataElement.type.length);
        break;
      case SAV_UINT32:
        PRINT_NUMERICAL_COLUMN(uint32_t, element, i, dataElement.type.length);
        break;
      case SAV_UINT64:
        PRINT_NUMERICAL_COLUMN(uint64_t, element, i, dataElement.type.length);
        break;
      case SAV_FLOAT:
        PRINT_NUMERICAL_COLUMN(float, element, i, dataElement.type.length);
        break;
      case SAV_DOUBLE:
        PRINT_NUMERICAL_COLUMN(double, element, i, dataElement.type.length);
        break;
      }
    }

    std::cout << "|" << std::endl;
  }

  std::cout << std::string(totalLineWidth, '-') << std::endl;
}

void process_query_response(SavimeConn &con, QueryResultHandle &handle,
                            bool printResult) {
  if (handle.is_schema == 0) {
    printf("%s\n", handle.response_text);
    read_query_block(con, handle);
  } else {
    size_t totalLineWidth = print_header(handle);
    while (true) {
      int ret = read_query_block(con, handle);

      if (ret == SAV_ERROR_RESPONSE_BLOCKS) {
        printf("%s\n", handle.response_text);
        break;
      }

      if (ret == SAV_NO_MORE_BLOCKS) {
        break;
      }

      if (ret == SAV_ERROR_READING_BLOCKS) {
        perror("Problem while receiving data from the server.\n");
        break;
      }

      if (printResult)
        print_block(handle, totalLineWidth);

      for (auto entry : handle.descriptors) {
        close(entry.second);
      }
      handle.descriptors.clear();
    }
  }
}

void parser_args(int argc, char *argv[], char* query, char *address, 
                 char *file, bool &print_flag, bool &shutdown_signal) {
#define MIN(X, Y) (X < Y) ? X : Y
  
  char c;
  while ((c = getopt (argc, argv, "c:f:pk")) != -1){
    
    switch (c) {
      case 'c': {
        size_t str_size = strlen(optarg)+1;
        memcpy(address, optarg, MIN(str_size, _BUFFER));
        break;
      }
      case 'f': {
        size_t str_size = strlen(optarg)+1;
        memcpy(file, optarg, MIN(str_size, _BUFFER));
        break;
      }
      case 'p': {
        print_flag = true;
        ;
        break;
      }
      case 'k': {
        shutdown_signal = true;
        ;
        break;
      }
      default:
        break;
    }
  } 
  
  for (int index = optind; index < argc; index++){
    size_t str_size = strlen(argv[index])+1;
    memcpy(query, argv[index], MIN(str_size, _BUFFER));
    break;
  }
}

int main(int argc, char *argv[]) {
#define EMPTY ""
  char query[_BUFFER] = EMPTY;
  char file[_BUFFER] = EMPTY;
  char address[_BUFFER] = EMPTY;
  bool print_flag = false;
  bool shutdown_signal = false;
  SavimeConn con;
  QueryResultHandle handle;
  
  parser_args(argc, argv, query, address, file, print_flag, shutdown_signal);
  
  if(!strcmp(address, EMPTY)){
    con = open_connection(0, "");
  } else {
    std::string str_connection = std::string(address);
    const auto c = str_connection.find(_COLON);
    std::string host = str_connection.substr(0, c);
    int32_t port = atoi(str_connection.substr(c + 1).c_str());
    con = open_connection(port, host.c_str());
  }
  
  if (!con.opened) {
    printf("Could not connect to the server.\n");
    return 0;
  }
  
  if(shutdown_signal){
    shutdown_savime(con);
    return 0;
  }
  
  if (strcmp(file, EMPTY)) {
    std::string line;
    std::ifstream infile(file);
    
    while (std::getline(infile, line)){
      handle = execute(con, line.c_str());

      if (!handle.successful) {
        printf("Fatal error during query execution.\n");
        return 0;
      }

      process_query_response(con, handle, true);
      dipose_query_handle(handle);
    }
    infile.close();
    
  } else if (strcmp(query, EMPTY)) {
    handle = execute(con, query);
    
    if (!handle.successful) {
      printf("Fatal error during query execution.\n");
      return 0;
    }

    process_query_response(con, handle, !print_flag);
    dipose_query_handle(handle);
  } else {
     while (true) {
      memset(query, sizeof(char), '\0');
      printf("query> ");
      fgets(query, _BUFFER, stdin);
      handle = execute(con, query);

      if (!handle.successful) {
        printf("Fatal error during query execution.\n");
        return 0;
      }

      process_query_response(con, handle, true);
      dipose_query_handle(handle);
    }
  }

  close_connection(con);
}