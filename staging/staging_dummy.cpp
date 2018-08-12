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
*    ALLAN MATHEUS				JANUARY 2018
*/

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <thread>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <stdio.h>
#include <fcntl.h> 
#include <unistd.h>
#include "staging.h"
#include "../lib/savime_lib.h"
#include "../core/include/symbols.h"
#include "../core/include/util.h"

#define ONE_SEC 1000000
#define FIRST_SERVER 1
#define LAST_SERVER 0
int32_t server_count = 0;
std::shared_ptr<staging::communicator> global_comm = NULL;
std::mutex comm_mutex;

inline char *to_char_ptr(const std::string &str) { return (char *)str.data(); }

staging::server::server(const std::string &address, std::size_t num_threads) {
  comm_mutex.lock();
  server_count++;

  if (server_count == FIRST_SERVER) {
    _comm = global_comm = std::make_shared<communicator>(address, num_threads);
  } else {
    _comm = global_comm;
  }
  comm_mutex.unlock();
}

void staging::server::finalize() {
  comm_mutex.lock();
  server_count--;

  if (server_count == LAST_SERVER) {
    global_comm->_keep_working = false;
    for (auto t = begin(global_comm->_thrds); t != end(global_comm->_thrds);
         ++t) {
      t->join();
    }

    global_comm = NULL;
  }
  comm_mutex.unlock();
  _comm = NULL;
}

staging::server::~server() { finalize(); }

std::string staging::server::run_savime(const std::string &query) {
#define EMPTY ""
  savime_query newQuery{query, ""};
  _comm->_queries_mtx.lock();
  _comm->_queries.push_back(newQuery);
  _comm->_queries_mtx.unlock();
  return EMPTY;
}

std::string staging::server::run_savime_sync(const std::string &query) {
#define EMPTY ""
  _comm->run_savime_sync(query);
  return EMPTY;
}

void staging::server::sync() { _comm->sync(); }

staging::dataset::dataset(const std::string &name, const std::string &type,
                          staging::server &st)
    : _created{false}, _name{name}, _type{type}, _comm{st._comm} {
  // std::cerr << "dataset(" << _name << ") ctor\n";
}

staging::dataset::~dataset() {
  // std::cerr << "dataset(" << _name << ") dtor\n";
}

void staging::dataset::write(char *buf, std::size_t len) {
  auto c = _get_comm();
  c->create_dataset(_name, _type, buf, len);
}

std::string staging::dataset::savime_response() {
  throw std::logic_error("Not Implemented\n");
}

std::shared_ptr<staging::communicator> staging::dataset::_get_comm() {
  if (auto c = _comm.lock()) {
    return c;
  }
  throw std::runtime_error("missing communicator");
}

staging::communicator::communicator(const std::string &address,
                                    std::size_t num_threads) {
  const auto c = address.find(_COLON);
  _host = address.substr(0, c);
  _service = address.substr(c + 1);
  std::thread t{&staging::communicator::worker, this};
  _thrds.push_back(std::move(t));
}

staging::communicator::~communicator() {}

void staging::communicator::create_dataset(const std::string &name,
                                           const std::string &type, char *buf,
                                           std::size_t len) {
#define SHM "/dev/shm/"

  int64_t writtenBytes = 0;
  std::stringstream q;
  std::string path = generateUniqueFileName(SHM, 1);

  int fd = open(path.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
  if (fd == -1) {
    std::cerr << "Could not open temp file for dataset: " << strerror(errno)
              << std::endl;
    return;
  }

  while (writtenBytes < len) {
    int64_t written = write(fd, &buf[writtenBytes], len - writtenBytes);
    if (written == -1) {
      std::cerr << "Error during writing temp file for dataset: "
                << strerror(errno) << std::endl;
      close(fd);
      return;
    }
    writtenBytes += written;
  }
  close(fd);

  // Creating create dataset command
  q << "create_dataset(\"" << name << ':' << type << "\", \"@" << path
    << "\");";
  savime_query new_query{q.str(), path};

  // Adding to list of commands to be sent
  _queries_mtx.lock();
  _queries.push_back(new_query);
  _queries_mtx.unlock();
}

std::string staging::communicator::run_savime(const std::string &query) {}

std::string staging::communicator::run_savime_sync(const std::string &query) {
  std::string ret = "";
  SavimeConn conn = open_connection(atoi(_service.c_str()), _host.c_str());  
  if(!conn.opened)
      return ret;
  QueryResultHandle handle = execute(conn, query.c_str());
  ret = handle.response_text;
  dipose_query_handle(handle);
  close_connection(conn);
  return ret;
}

void staging::communicator::sync() {}

void staging::communicator::worker() {
#define MAX_QUERIES_PER_BATCH 1000
#define FIRST_QUERY(X) X == 0
#define BATCH_QUERY_BEGIN "batch("
#define BATCH_QUERY_SEPARATOR ","
#define BATCH_QUERY_SEMICOLON ';'
#define BATCH_QUERY_END ");"
  _idle = true;
  _keep_working = true;

  while (true) {
    std::stringstream batch_query;
    std::vector<savime_query> queries;

    // Getting all queries put in the list
    _queries_mtx.lock();

    // Checking if keepWorking flag was set to false
    bool keep_working = _keep_working;
    if (!keep_working && _queries.size() == 0) {
      _queries_mtx.unlock();
      break;
    }

    // for(auto q : _queries)
    while (_queries.size() > 0 && queries.size() < MAX_QUERIES_PER_BATCH) {
      queries.push_back(_queries.front());
      _queries.pop_front();
    }
    _queries_mtx.unlock();

    // If no query has been sent, wait a sec and try again
    if (queries.size() == 0) {
      usleep(ONE_SEC);
      continue;
    }

    // Creating batch query
    batch_query << BATCH_QUERY_BEGIN;
    for (int32_t i = 0; i < queries.size(); i++) {
      std::string q = queries[i].q;
      q = remove_str(q, BATCH_QUERY_SEMICOLON);

      if (FIRST_QUERY(i)) {
        batch_query << q;
      } else {
        batch_query << BATCH_QUERY_SEPARATOR << q;
      }
    }
    batch_query << BATCH_QUERY_END;

    SavimeConn conn = open_connection(atoi(_service.c_str()), _host.c_str());
    
    if(!conn.opened)
      continue;
      
    QueryResultHandle handle = execute(conn, batch_query.str().c_str());
    dipose_query_handle(handle);
    close_connection(conn);

    for (int32_t i = 0; i < queries.size(); i++) {
      std::string file = queries[i].file;
      if (!file.empty()) {
        remove(file.c_str());
      }
    }
  }

  _queries_mtx.lock();
  for (auto q : _queries) {
    if (!q.file.empty()) {
      remove(q.file.c_str());
    }
  }
  _queries_mtx.unlock();

  _idle = true;
}

staging::dataset_writer::dataset_writer() {}

staging::dataset_writer::dataset_writer(const std::string &_name,
                                        const std::string &_type, char *_buf,
                                        std::size_t _len)
    : name{_name}, type{_type}, buf{_buf}, len{_len} {}

void staging::dataset_writer::run(char *host, char *service) {}

std::ostream &operator<<(std::ostream &o, const staging::response &r) {
  switch (r.status) {
  case staging::result::ok:
    o << "OK";
    break;

  case staging::result::err:
    o << "ERR";
    break;
  }
}
