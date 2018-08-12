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
#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <stdexcept>

#include <netdb.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>



#define TEST_NZ(x) do { if ( (x)) { throw std::runtime_error(strerror(errno)); } } while (0)
#define TEST_Z(x)  do { if (!(x)) { throw std::runtime_error(strerror(errno)); } } while (0)

#define CQ_SIZE 20
#define WR_SEND_SIZE 10
#define WR_RECV_SIZE 10
#define WR_SEND_SGE 1
#define WR_RECV_SGE 1

void rc_server_loop(const char *port);
void rc_client_loop(const char *host, const char *port, void *context);

void rc_disconnect(struct rdma_cm_id *id);

struct ibv_pd *rc_get_pd();

class rdma_rc_loop {
public:
    virtual void run() = 0;
};

struct default_rdma_rc_loop {
protected:
    struct rdma_event_channel *m_ec;

    void event_loop(bool exit_on_disconnect);
};

class rdma_rc_client_loop :
        public rdma_rc_loop, protected default_rdma_rc_loop {
public:
    rdma_rc_client_loop(const char *host, const char *port, void *context);
    ~rdma_rc_client_loop();
    void run();

private:
    struct addrinfo *m_addr;
    struct rdma_cm_id *m_conn;
    struct rdma_conn_param m_cm_params;
};

class rdma_rc_server_loop :
        public rdma_rc_loop, protected default_rdma_rc_loop {
public:
    rdma_rc_server_loop(const char *port);
    ~rdma_rc_server_loop();
    void run();

private:
    struct sockaddr_in m_addr;
    struct rdma_cm_id *m_listener;
};

#endif /* RDMA_COMMON_H */
