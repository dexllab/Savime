#ifndef SAVIME_STAGING_RDMA_UTILS_H
#define SAVIME_STAGING_RDMA_UTILS_H

#include "../lib/protocol.h"
#include "rdmap/rdmap.h"
#include "staging.h"

void write_rdma(char *host, char *service, staging::request& req,
        char *buf = nullptr, std::size_t size = 0);

void send_buffer(rdma::endpoint& ep, staging::request& req, char *buf,
        std::size_t n);

std::string send_query(char *host, char *service, staging::request& req,
        const std::string& query);

std::string recv_query(rdma::endpoint& ep, size_t size);

void recv_buffer(rdma::endpoint& ep, char *buf, size_t size);

#endif // SAVIME_STAGING_RDMA_UTILS_H
