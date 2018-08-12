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

#include <chrono>
#include <sstream>
#include <iostream>
#include <string>
#include <stdexcept>
#include <thread>

#include <cstdio>
#include <cstring>
#include <cerrno>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include "rdmap.h"
#include "log.h"

using namespace rdma;

memory_region::memory_region() : _buf{nullptr}, _len{0}, _mr{nullptr}
{ debug("mr default ctor"); }

memory_region::memory_region(void *buf, size_t len)
    : _buf{buf}, _len{len}, _mr{nullptr}
{ debug("mr ctor"); }

memory_region::memory_region(memory_region&& mr)
    : _buf{mr._buf}, _len{mr._len}, _mr{mr._mr}
{
    debug("mr move ctor");
    mr._buf = nullptr;
    mr._len = 0;
    mr._mr = nullptr;
}

memory_region::~memory_region()
{
    debug("mr dtor");
    _buf = nullptr;
    _len = 0;
    if (_mr) {
        rdma_dereg_mr(_mr);
    }
}

memory_region& memory_region::operator=(memory_region&& mr)
{
    debug("mr move operator=");
    if (this != &mr) {
        if (_mr) {
            rdma_dereg_mr(_mr);
        }
        _buf = mr._buf;
        _len = mr._len;
        _mr = mr._mr;
        mr._buf = nullptr;
        mr._len = 0;
        mr._mr = nullptr;
    }
    return *this;
}

endpoint::endpoint() : _conn{false}, _blocking{true}, _id{nullptr}
{ debug("ep default ctor"); }

endpoint::endpoint(struct rdma_cm_id *id, bool conn)
    : _conn{conn}, _blocking{true}, _id{id}
{ debug("ep ctor"); }

endpoint::endpoint(endpoint&& ep)
    : _conn{ep._conn}, _blocking{ep._blocking}, _id{ep._id}
{
    debug("ep move ctor");
    ep._conn = false;
    ep._blocking = true;
    ep._id = nullptr;
}

endpoint::~endpoint()
{
    debug("ep dtor");
    if (_conn) {
        disconnect();
    }
    if (_id) {
        rdma_destroy_ep(_id);
    }
}

endpoint& endpoint::operator=(endpoint&& ep)
{
    debug("ep move operator=");
    if (this != &ep) {
        if (_conn) {
            disconnect();
        }
        if (_id) {
            rdma_destroy_ep(_id);
        }
        _conn = ep._conn;
        _blocking = ep._blocking;
        _id = ep._id;
        ep._conn = false;
        ep._blocking = true;
        ep._id = nullptr;
    }
    return *this;
}

void endpoint::create(char *host, char *service, struct ibv_qp_init_attr *attr,
        struct rdma_addrinfo *hints, bool blocking)
{
    debug("ep create");
    struct rdma_addrinfo *res;
    if (attr) {
        attr->qp_context = _id;
    }
    if (rdma_getaddrinfo(host, service, hints, &res)) {
        throw addrinfo_error(std::string("rdma_getaddrinfo: ")
                + strerror(errno));
    }
    if (rdma_create_ep(&_id, res, nullptr, attr)) {
        throw endpoint_error(std::string("rdma_create_ep: ")
                + strerror(errno));
    }
    _blocking = blocking;
    if (!blocking) {
        auto flags = fcntl(_id->channel->fd, F_GETFL);
        auto rc = fcntl(_id->channel->fd, F_SETFL, flags | O_NONBLOCK);
        if (rc < 0) {
            throw std::runtime_error(std::string("fcntl (set O_NONBLOCK): ")
                    + strerror(errno));
        }
    }
    rdma_freeaddrinfo(res);
}

void endpoint::query_qp(struct ibv_qp_attr *qp_attr, int attr_mask,
        struct ibv_qp_init_attr *init_attr)
{
    debug("ep query_qp");
    if (ibv_query_qp(_id->qp, qp_attr, attr_mask, init_attr)) {
        throw std::runtime_error(std::string("ibv_query_qp: ")
                + strerror(errno));
    }
}

void endpoint::listen(int backlog)
{
    debug("ep listen");
    if (rdma_listen(_id, backlog)) {
        throw listen_error(std::string("rdma_listen: ") + strerror(errno));
    }
}

void poll_ev(int fd)
{
    struct pollfd pfd = {
        //.fd = _id->channel->fd,
        .fd = fd,
        .events = POLLIN,
        .revents = 0,
    };
    int rc;
    do {
        rc = poll(&pfd, 1, 300);
    } while (rc == 0);
    if (rc < 0) {
        throw std::runtime_error(std::string("poll failed: ")
                + strerror(errno));
    }
}

endpoint endpoint::get_request()
{
    debug("ep get_request");
    struct rdma_cm_id *new_id;
    if (!_blocking) {
        poll_ev(_id->channel->fd);
    }
    if (rdma_get_request(_id, &new_id)) {
        throw connection_request_error(std::string("rdma_get_request: ")
                + strerror(errno));
    }
    //endpoint ep;
    //ep._id = new_id;
    //ep._conn = true;
    //return ep;
    endpoint ep(new_id, true);
    return ep;
}

void endpoint::accept()
{
    debug("ep accept");
    if (rdma_accept(_id, nullptr)) {
        throw accept_error(std::string("rdma_accept: ") + strerror(errno));
    }
}

void endpoint::connect(struct rdma_conn_param *param, int retry)
{
    std::chrono::seconds s(1);
    debug("ep connect");
    int i;
    for (i = 0; i < retry; ++i) {
        if (rdma_connect(_id, param) == 0) {
            _conn = true;
            break;
        }
        std::this_thread::sleep_for(s);
        std::cerr << "Retrying to connect\n";
    }
    if (i == retry) {
        throw connection_error(std::string("rdma_connect: ")
                + strerror(errno));
    }
}

void endpoint::disconnect()
{
    debug("ep disconnect");
    if (rdma_disconnect(_id)) {
        throw connection_error(std::string("rdma_disconnect: ")
                + strerror(errno));
    }
    _conn = false;
}

memory_region endpoint::reg_memory(void *buf, size_t len, mr_type type)
{
    debug("ep reg_memory");
    memory_region m{buf, len};
    switch (type) {
    case mr_type::message:
        debug("ep reg_memory for message");
        m._mr = rdma_reg_msgs(_id, m._buf, m._len);
        break;
    case mr_type::allow_read:
        debug("ep reg_memory for remote read");
        m._mr = rdma_reg_read(_id, m._buf, m._len);
        break;
    case mr_type::allow_write:
        debug("ep reg_memory for remote write");
        m._mr = rdma_reg_write(_id, m._buf, m._len);
        break;
    case mr_type::inline_data:
        debug("ep reg_memory: skip, using inline");
        break;
    default:
        throw std::logic_error("invalid option");
    }
    if (!m._mr) {
        throw registration_error(std::string("rdma_reg_memory: ")
                + strerror(errno));
    }
    return m;
}

void endpoint::post_send(memory_region& mr, void *ctx, int flags)
{
    debug("ep post_send");
    if (rdma_post_send(_id, ctx, mr._buf, mr._len, mr._mr, flags)) {
        throw send_error(std::string("rdma_post_send: ") + strerror(errno));
    }
}

void endpoint::post_recv(memory_region& mr, void *ctx)
{
    debug("ep post_recv");
    if (rdma_post_recv(_id, ctx, mr._buf, mr._len, mr._mr)) {
        throw recv_error(std::string("rdma_post_recv: ") + strerror(errno));
    }
}

void endpoint::post_write(memory_region& mr, uint64_t raddr, uint32_t rkey,
        void *ctx, int flags)
{
    debug("ep post_write");
    if (rdma_post_write(_id, ctx, mr._buf, mr._len, mr._mr, flags, raddr, rkey)) {
        throw remote_write_error(std::string("rdma_post_write: ")
                + strerror(errno));
    }
}

void endpoint::post_read(memory_region& mr, uint64_t raddr, uint32_t rkey,
        void *ctx, int flags)
{
    debug("ep post_read");
    if (rdma_post_read(_id, ctx, mr._buf, mr._len, mr._mr, flags, raddr, rkey)) {
        throw remote_read_error(std::string("rdma_post_read: ")
                + strerror(errno));
    }
}

uint64_t endpoint::wait_send()
{
    debug("ep wait_send");
    struct ibv_wc wc;
    memset(&wc, 0, sizeof(wc));
    if (!_blocking) {
        poll_ev(_id->channel->fd);
    }
    int rt = rdma_get_send_comp(_id, &wc);
    if (rt == -1) {
        throw completion_error(std::string("rdma_get_send_comp: ")
                + strerror(errno));
    }
    if (wc.status != IBV_WC_SUCCESS) {
        std::stringstream msg;
        msg << "status: " << wc.status << " msg: "
            << ibv_wc_status_str(wc.status) << '\n';
        throw completion_error(msg.str());
    }
    debug("ep finished_send");
    return wc.wr_id;
}

uint64_t endpoint::wait_recv()
{
    debug("ep wait recv");
    struct ibv_wc wc;
    memset(&wc, 0, sizeof(wc));
    if (!_blocking) {
        poll_ev(_id->channel->fd);
    }
    int rt = rdma_get_recv_comp(_id, &wc);
    if (rt == -1) {
        throw completion_error(std::string("rdma_get_recv_comp: ")
                + strerror(errno));
    }
    if (wc.status != IBV_WC_SUCCESS) {
        std::stringstream msg;
        msg << "status: " << wc.status << " msg: "
            << ibv_wc_status_str(wc.status) << '\n';
        throw completion_error(msg.str());
    }
    debug("ep finished recv");
    return wc.wr_id;
}
