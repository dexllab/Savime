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
#include "rdma_utils.h"
#include "../lib/savime_lib.h"
#include "../core/include/util.h"
#include "../lib/savime_lib.h"

#define ONE_SEC 1000000
#define FIRST_SERVER 1
#define LAST_SERVER 0
int32_t server_count = 0;
std::shared_ptr<staging::communicator> global_comm = NULL;
std::mutex comm_mutex;

inline void init_dataset_request(staging::request& req, staging::operation op,
        const std::string& dataset, const std::string& type, size_t n)
{
    if (dataset.size() >= staging::MAX_DATASET_NAME_LEN
            && type.size() >= staging::MAX_TYPE_NAME_LEN) {
        throw std::length_error("cannot make a create_dataset request");
    }

    memset(&req, 0, sizeof(req));

    req.op = op;
    req.data.dataset.size = n;
    memcpy(req.data.dataset.name, dataset.c_str(), dataset.size());
    memcpy(req.data.dataset.type, type.c_str(), type.size());
}


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

staging::server::~server() {finalize();}

std::string staging::server::run_savime(const std::string &query) {
#define EMPTY ""
  savime_query newQuery{query, "", false, NULL};
  _comm->_queries_mtx.lock();
  _comm->_queries.push_back(newQuery);
  _comm->_queries_mtx.unlock();
  return EMPTY;
}

std::string staging::server::run_savime_sync(const std::string &query) {
#define EMPTY ""
  savime_query newQuery{query, "", false, NULL};
  _comm->_queries_mtx.lock();
  _comm->_queries.push_back(newQuery);
  _comm->_queries_mtx.unlock();
  return EMPTY;
}

void staging::server::sync() { _comm->sync(); }

staging::dataset::dataset(const std::string &name, const std::string &type,
                          staging::server &st)
    : _created{false}, _name{name}, _type{type}, _comm{st._comm} {}

staging::dataset::~dataset() {}

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
  std::copy(_host.begin(), _host.end(), _chost);
  _service = address.substr(c + 1);
  std::copy(_service.begin(), _service.end(), _cservice);
  std::thread t{&staging::communicator::worker, this};
  _thrds.push_back(std::move(t));
}

staging::communicator::~communicator() {}

void staging::communicator::create_dataset(const std::string &name,
                                           const std::string &type, char *buf,
                                           std::size_t len) {
#define SHM "/dev/shm/"

   auto w = std::make_shared<dataset_writer>(name, type, buf, len);
   savime_query new_query{"", "", true, w};

  // Adding to list of commands to be sent
  _queries_mtx.lock();
  _queries.push_back(new_query);
  _queries_mtx.unlock();
}

std::string staging::communicator::run_savime(const std::string &query) {
  staging::request req;
  memset(&req, 0, sizeof(req));
  req.op = staging::operation::run_savime;
  req.data.query.size = query.size();
  return send_query(_chost, _cservice, req, query);
}

std::string staging::communicator::run_savime_sync(const std::string &query) {
  return run_savime(query);
}

void staging::communicator::sync() {
  std::unique_lock<std::mutex> lock(_mutex);
  
  while(!_idle) {
    _condVar.wait(lock);
  }
}

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
    std::vector<savime_query> create_dataset_queries;

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
      auto q = _queries.front();
      if(q.is_create_dataset){
        create_dataset_queries.push_back(q);  
      } else {
        queries.push_back(q);
      }
      _queries.pop_front();
    }
    
    // If no query has been sent, wait a sec and try again
    if (queries.size() == 0 && create_dataset_queries.size() == 0) {
      _idle = true;
      _condVar.notify_all();
      _queries_mtx.unlock();
      usleep(ONE_SEC);
      continue;
    } else {
      _idle = false;
    }
    
    _queries_mtx.unlock();
    
    if(create_dataset_queries.size() > 0){
      for (int32_t i = 0; i < create_dataset_queries.size(); i++) {
        create_dataset_queries[i].writer->run(_chost, _cservice);
      }
    }

    if(queries.size() > 0){
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
      run_savime(batch_query.str());
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

void staging::dataset_writer::run(char *host, char *service) {
  staging::request req;
  auto op = staging::operation::create_dataset;
  init_dataset_request(req, op, name, type, len);
  write_rdma(host, service, req, buf, len);   
}

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