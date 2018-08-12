#ifndef RDMAP_H
#define RDMAP_H

#include <memory>
#include <stdexcept>
#include <string>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

namespace rdma {

/**
 * enum class mr_type - Defines the permissions for the memory region access.
 */
enum class mr_type {
    inline_data, // no registration
    message,     // only send/recv
    allow_read,  // remote peer can read from this
    allow_write  // remote peer can write to this
};

/**
 * struct memory_region - Useful wrapper for dealing with buffers used in RDMA
 * communication.
 *
 * Note: A memory region must be associated with the PD (protection domain) of
 * an specified RDMA endpoint.  Use rdma::endpoint::reg_memory() for create
 * memory regions correctly.
 */
struct memory_region {
    void *_buf;
    size_t _len;
    struct ibv_mr *_mr;

    memory_region();
    memory_region(void *buf, size_t len);
    memory_region(memory_region&& mr);
    ~memory_region();

    memory_region& operator=(memory_region&& mr);
};

/**
 * class endpoint - A RDMA socket-like endpoint.
 *
 * for servers ::
 *
 *     struct rdma_addrinfo hints;
 *     struct ibv_qp_init_attr init_attr;
 *     memset(&hints, 0, sizeof(hints));
 *     hints.ai_flags = RAI_PASSIVE;
 *     hints.ai_port_space = RDMA_PS_TCP;
 *     memset(&init_attr, 0, sizeof(init_attr));
 *     init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = 1;
 *     init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1;
 *     init_attr.sq_sig_all = 1;
 *     rdma::endpoint ep;
 *     ep.create("127.0.0.1", "1234", &init_attr, &hints);
 *     ep.listen(10);
 *     auto peer = ep.get_request();
 *     // maybe peer.post_recv something
 *     peer.accept();
 *
 * for clients ::
 *
 *     struct rdma_addrinfo hints;
 *     struct ibv_qp_init_attr attr;
 *     memset(&attr, 0, sizeof(attr));
 *     attr.cap.max_send_wr = attr.cap.max_recv_wr = 1;
 *     attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
 *     attr.sq_sig_all = 1;
 *     memset(&hints, 0, sizeof(hints));
 *     hints.ai_port_space = RDMA_PS_TCP;
 *     rdma::endpoint ep;
 *     ep.create("127.0.0.1", "3221", &attr, &hints);
 *     // maybe ep.post_recv something
 *     ep.connect();
 *
 */
class endpoint {
public:
    endpoint();
    endpoint(struct rdma_cm_id *id, bool conn);
    endpoint(endpoint&& ep);
    ~endpoint();

    endpoint& operator=(endpoint&& ep);

    /**
     * endpoint::create() - Actually create the endpoint.
     */
    void create(char *host, char *service, struct ibv_qp_init_attr *attr,
            struct rdma_addrinfo *hints, bool blocking = true);

    /**
     * endpoint::query_qp() - Query parameters of the QP associated with this
     * endpoint.
     *
     * Note: The endpoint must be created (See create()).
     */
    void query_qp(struct ibv_qp_attr *qp_attr, int attr_mask,
            struct ibv_qp_init_attr *init_attr);

    /**
     * endpoint::reg_memory() - Registry a memory region allowing a specific
     * type of operation.
     *
     * See rdma::memory_region and rdma::mr_type.
     */
    memory_region reg_memory(void *buf, size_t len,
            mr_type type = mr_type::message);

    void listen(int backlog);
    endpoint get_request();
    void accept();

    void connect(struct rdma_conn_param *param = nullptr, int retry = 5);
    void disconnect();

    void post_send(memory_region& mr, void *ctx = nullptr, int flags = 0);
    void post_recv(memory_region& mr, void *ctx = nullptr);

    void post_write(memory_region& mr, uint64_t raddr, uint32_t rkey,
            void *ctx = nullptr, int flags = 0);
    void post_read(memory_region& mr, uint64_t raddr, uint32_t rkey,
            void *ctx = nullptr, int flags = 0);

    /**
     * endpoint::wait_send() - Blocks until the next send completion.
     *
     * Waits (blocks) for the next completion from the send completion queue
     * associated with this endpoint.  The completion can be of the following
     * types: send, write or read.
     *
     * Returns the id associated with the posted operation.
     */
    uint64_t wait_send();

    /**
     * endpoint::wait_recv() - Blocks until the next recv completion.
     *
     * Waits (blocks) for the next completion from the recv completion queue
     * associated with this endpoint.  The completion can only be of the recv
     * type.
     *
     * Returns the id associated with the posted operation.
     */
    uint64_t wait_recv();

private:
    bool _conn;
    bool _blocking;
    struct rdma_cm_id *_id;
};

class rdma_error : public std::runtime_error {
public:
    rdma_error(const std::string& s) : std::runtime_error(s) { }
};

class completion_error : public rdma_error {
public:
    completion_error(const std::string& s) : rdma_error(s) { }
};

class addrinfo_error : public rdma_error {
public:
    addrinfo_error(const std::string& s) : rdma_error(s) { }
};

class endpoint_error : public rdma_error {
public:
    endpoint_error(const std::string& s) : rdma_error(s) { }
};

class listen_error : public rdma_error {
public:
    listen_error(const std::string& s) : rdma_error(s) { }
};

class connection_request_error : public rdma_error {
public:
    connection_request_error(const std::string& s) : rdma_error(s) { }
};

class accept_error : public rdma_error {
public:
    accept_error(const std::string& s) : rdma_error(s) { }
};

class connection_error : public rdma_error {
public:
    connection_error(const std::string& s) : rdma_error(s) { }
};

class registration_error : public rdma_error {
public:
    registration_error(const std::string& s) : rdma_error(s) { }
};

class post_error : public rdma_error {
public:
    post_error(const std::string& s) : rdma_error(s) { }
};

class send_error : public post_error {
public:
    send_error(const std::string& s) : post_error(s) { }
};

class recv_error : public post_error {
public:
    recv_error(const std::string& s) : post_error(s) { }
};

class remote_write_error : public post_error {
public:
    remote_write_error(const std::string& s) : post_error(s) { }
};

class remote_read_error : public post_error {
public:
    remote_read_error(const std::string& s) : post_error(s) { }
};

} // rdma

#endif // RDMAP_H
