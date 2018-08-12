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

#include <vector>
#include <iostream>
#include "rdma_utils.h"

// Client side

inline void init_conn_attr(struct ibv_qp_init_attr *attr,
    struct rdma_addrinfo *hints, struct rdma_conn_param *param)
{
    memset(attr, 0, sizeof(*attr));
    attr->cap.max_send_wr = attr->cap.max_recv_wr = max_writers + 1;
    attr->cap.max_send_sge = attr->cap.max_recv_sge = 1;
    attr->sq_sig_all = 0;

    memset(hints, 0, sizeof(*hints));
    hints->ai_port_space = RDMA_PS_TCP;

    memset(param, 0, sizeof(*param));
    //param.initiator_depth = param.responder_resources = max_writers + 1;
    param->retry_count = 7;
    param->rnr_retry_count = 7;
}

void write_rdma(char *host, char *service, staging::request& req, char *buf,
        std::size_t size)
{
    struct ibv_qp_init_attr attr;
    struct rdma_addrinfo hints;
    struct rdma_conn_param param;

    init_conn_attr(&attr, &hints, &param);

    rdma::endpoint ep;
    ep.create(host, service, &attr, &hints);
    ep.connect(&param);

    staging::response res;
    auto res_mr = ep.reg_memory(&res, sizeof(res));

    send_buffer(ep, req, buf, size);

    ep.post_recv(res_mr);
    ep.wait_recv(); // wait response
    //std::cerr << "GOT RESPONSE: " << res << '\n';

    if (res.status == staging::result::err) {
        throw std::runtime_error("staging error");
    }
}

void send_buffer(rdma::endpoint& ep, staging::request& req, char *buf,
        std::size_t n)
{
    char sync = '\0';
    auto sync_mr = ep.reg_memory(&sync, sizeof(sync));

    auto req_mr = ep.reg_memory(&req, sizeof(req));
    ep.post_send(req_mr, nullptr, IBV_SEND_SIGNALED);

    size_t buflen = 0;
    auto buflen_mr = ep.reg_memory(&buflen, sizeof(buflen));

    remote_region remote;
    memset(&remote, 0, sizeof(remote));
    auto remote_mr = ep.reg_memory(&remote, sizeof(remote));

    ep.wait_send(); // wait req

    //std::cerr << "request sent\n";

    std::size_t writers = 0;
    std::list<rdma::memory_region> mrs;

    for (size_t sent = 0; sent < n; sent += buflen) {
        size_t rem = n - sent;
        buflen = rem > max_buffer_len ? max_buffer_len : rem;

        //std::cerr << "sent: " << sent << "   sending: " << buflen <<  "   total: " << n << '\n';

        ep.post_recv(remote_mr);
        ep.post_send(buflen_mr, nullptr, IBV_SEND_SIGNALED);
        ep.wait_send(); // wait buflen

        auto mr = ep.reg_memory(&buf[sent], buflen);
        ep.wait_recv(); // wait remote_mr
        //std::cerr << "got region raddr: " << remote.addr << "   rkey: " << remote.rkey << '\n';

#if 1
        if (writers < max_writers - 1 && !(sent + buflen == n)) {
            ep.post_write(mr, remote.addr, remote.rkey);
            ++writers;
            //std::cerr << "[debug] writers: " << writers << " ...\n";

        } else {
            ep.post_write(mr, remote.addr, remote.rkey, nullptr,
                    IBV_SEND_SIGNALED);
            ++writers;
            //std::cerr << "[debug] writers: " << writers << " ... waiting ...\n";
            ep.wait_send();
            writers = 0;
        }
#else
        ep.post_write(mr, remote.addr, remote.rkey, nullptr,
                IBV_SEND_SIGNALED);
        //std::cerr << "waiting ...\n";
        ep.wait_send();
#endif

        mrs.push_back(std::move(mr));
    }

    ep.post_send(sync_mr, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait sync
}

std::string send_query(char *host, char *service, staging::request& req,
        const std::string& query)
{
    struct ibv_qp_init_attr attr;
    struct rdma_addrinfo hints;
    struct rdma_conn_param param;

    init_conn_attr(&attr, &hints, &param);
    attr.sq_sig_all = 0;

    rdma::endpoint ep;
    ep.create(host, service, &attr, &hints);
    ep.connect(&param);

    remote_region remote;
    memset(&remote, 0, sizeof(remote));
    auto remote_mr = ep.reg_memory(&remote, sizeof(remote));
    ep.post_recv(remote_mr);

    auto req_mr = ep.reg_memory(&req, sizeof(staging::request));

    ep.post_send(req_mr, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait request

    ep.wait_recv(); // wait remote
    
    auto query_mr = ep.reg_memory((char *)query.data(), query.size());
    ep.post_write(query_mr, remote.addr, remote.rkey, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait query

    char sync = '\0';
    auto sync_mr = ep.reg_memory(&sync, sizeof(sync));
    staging::response res;
    auto res_mr = ep.reg_memory(&res, sizeof(res));

    ep.post_recv(res_mr); // antecipated

    ep.post_send(sync_mr, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait sync

    ep.wait_recv(); // wait response

    //std::cerr << "GOT RESPONSE: " << res << '\n';

    if (res.status == staging::result::err) {
        throw std::runtime_error("staging error");
    }

    return "";
}

// Server side

std::string recv_query(rdma::endpoint& ep, size_t size)
{
    //std::cerr << "################ recv_query of size: " << size << "\n";

    remote_region remote;
    memset(&remote, 0, sizeof(remote));
    auto remote_mr = ep.reg_memory(&remote, sizeof(remote));

    std::vector<char> query(size);
    auto query_mr = ep.reg_memory((char *)query.data(), size,
            rdma::mr_type::allow_write);

    remote.addr = (uint64_t)(uintptr_t)query_mr._mr->addr;
    remote.rkey = query_mr._mr->rkey;

    char sync = '\0';
    auto sync_mr = ep.reg_memory(&sync, sizeof(sync));

    ep.post_recv(sync_mr); // antecipated

    ep.post_send(remote_mr, nullptr, IBV_SEND_SIGNALED);
    //std::cerr << "################ recv_query sending remote\n";

    ep.wait_send(); // wait remote
    //std::cerr << "################ recv_query remote sent\n";

    ep.wait_recv(); // wait sync

    //query[query.size() -1] = '\0';

    std::string str_query(static_cast<char *>(query.data()), query.size());

    //std::cerr << "################ recv_query '" << str_query << "'\n";

    return str_query;
}

void recv_buffer(rdma::endpoint& ep, char *buf, size_t size)
{
    char sync = '\0';
    auto sync_mr = ep.reg_memory(&sync, sizeof(sync));

    size_t buflen = 0;
    auto buflen_mr = ep.reg_memory(&buflen, sizeof(buflen));

    remote_region remote;
    auto remote_mr = ep.reg_memory(&remote, sizeof(remote));

    std::list<rdma::memory_region> mrs;

    for (size_t registred = 0; registred < size; registred += buflen) {
        ep.post_recv(buflen_mr);
        ep.wait_recv(); // wait buflen

        //std::cerr << "reg mem region of " << buflen << " bytes\n";
        auto mr = ep.reg_memory(&buf[registred], buflen,
                rdma::mr_type::allow_write);

        remote.addr = (uint64_t)(uintptr_t)mr._mr->addr;
        remote.rkey = mr._mr->rkey;
        mrs.push_back(std::move(mr));

        if (registred + buflen >= size) { // last iteration
            ep.post_recv(sync_mr); // antecipated
        }

        ep.post_send(remote_mr, nullptr, IBV_SEND_SIGNALED);
        ep.wait_send(); // wait remote region
        //std::cerr << "sent region raddr: " << remote.addr << "   rkey: " << remote.rkey << '\n';
    }

    ep.wait_recv(); // wait sync
    //std::cerr << "GOT SYNC\n";
}
