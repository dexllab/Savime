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
#include <thread>

#include "rdmap.h"
#include "common.h"

const int TIMEOUT_IN_MS = 500;

struct context {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    //pthread_t cq_poller_thread;
    std::thread cq_poller_thread;
};

static struct context *s_ctx = NULL;
static pre_conn_cb_fn s_on_pre_conn_cb = NULL;
static connect_cb_fn s_on_connect_cb = NULL;
static completion_cb_fn s_on_completion_cb = NULL;
static disconnect_cb_fn s_on_disconnect_cb = NULL;

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void event_loop(struct rdma_event_channel *ec,
        int exit_on_disconnect);
//static void *poll_cq(void *);
static void poll_cq();

void build_connection(struct rdma_cm_id *id)
{
    struct ibv_qp_init_attr qp_attr;
    build_context(id->verbs);
    build_qp_attr(&qp_attr);
    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));
}

void build_context(struct ibv_context *verbs)
{
    if (s_ctx) {
        if (s_ctx->ctx != verbs) {
            throw std::runtime_error(
                    "cannot handle events in more than one context.");
        }
        return;
    }
    s_ctx = (struct context *)malloc(sizeof(struct context));
    s_ctx->ctx = verbs;
    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, CQ_SIZE, NULL,
                s_ctx->comp_channel, 0));	/* cqe=10 is arbitrary */
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
    //TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
    s_ctx->cq_poller_thread = std::thread(poll_cq);
}

void build_params(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));
    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7;	/* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));
    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;
    qp_attr->cap.max_send_wr = WR_SEND_SIZE;
    qp_attr->cap.max_recv_wr = WR_RECV_SIZE;
    qp_attr->cap.max_send_sge = WR_SEND_SGE;
    qp_attr->cap.max_recv_sge = WR_RECV_SGE;
}

void default_rdma_rc_loop::event_loop(bool exit_on_disconnect)
{
    struct rdma_cm_event *event = NULL;
    struct rdma_conn_param cm_params;
    build_params(&cm_params);
    while (rdma_get_cm_event(m_ec, &event) == 0) {
        struct rdma_cm_event event_copy;
        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);
        if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) {
            build_connection(event_copy.id);
            if (s_on_pre_conn_cb) {
                s_on_pre_conn_cb(event_copy.id);
            }
            TEST_NZ(rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS));
        } else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
            TEST_NZ(rdma_connect(event_copy.id, &cm_params));
        } else if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            build_connection(event_copy.id);
            if (s_on_pre_conn_cb) {
                s_on_pre_conn_cb(event_copy.id);
            }
            TEST_NZ(rdma_accept(event_copy.id, &cm_params));
        } else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) {
            if (s_on_connect_cb) {
                s_on_connect_cb(event_copy.id);
            }
        } else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) {
            rdma_destroy_qp(event_copy.id);
            if (s_on_disconnect_cb) {
                s_on_disconnect_cb(event_copy.id);
            }
            rdma_destroy_id(event_copy.id);
            if (exit_on_disconnect) {
                break;
            }
        } else {
            throw std::runtime_error("unknown event");
        }
    }
}

void poll_cq()
{
    void *ctx = NULL;
    struct ibv_cq *cq;
    struct ibv_wc wc;
    // int nev = 0, max_ev = CQ_SIZE - 1;
    while (1) {
        /*
           if (nev == max_ev) {
           nev = 0;
           ibv_ack_cq_events(cq, max_ev);
           }
           */
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        // nev++;
        TEST_NZ(ibv_req_notify_cq(cq, 0));
        while (ibv_poll_cq(cq, 1, &wc)) {
            if (wc.status == IBV_WC_SUCCESS) {
                s_on_completion_cb(&wc);
            } else {
                fprintf(stderr, "status: %d\n", wc.status);
                throw std::runtime_error(
                        "poll_cq: status is not IBV_WC_SUCCESS");
            }
        }
    }
    // ibv_ack_cq_events(cq, nev);
    //return NULL;
}

void rc_init(pre_conn_cb_fn pc, connect_cb_fn conn, completion_cb_fn comp,
        disconnect_cb_fn disc)
{
    s_on_pre_conn_cb = pc;
    s_on_connect_cb = conn;
    s_on_completion_cb = comp;
    s_on_disconnect_cb = disc;
}

rdma_rc_client_loop::rdma_rc_client_loop(const char *host, const char *port,
        void *context)
{
    TEST_NZ(getaddrinfo(host, port, NULL, &m_addr));
    TEST_Z(m_ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(m_ec, &m_conn, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_resolve_addr(m_conn, NULL, m_addr->ai_addr,
                TIMEOUT_IN_MS));
    freeaddrinfo(m_addr);
    m_conn->context = context;
    build_params(&m_cm_params);
}

rdma_rc_client_loop::~rdma_rc_client_loop()
{
    // rdma_destroy_id(m_conn);
    rdma_destroy_event_channel(m_ec);
}

void rdma_rc_client_loop::run()
{
    event_loop(true); // exit on disconnect
}

void rc_client_loop(const char *host, const char *port, void *context)
{
    rdma_rc_client_loop loop(host, port, context);
    loop.run();
}

rdma_rc_server_loop::rdma_rc_server_loop(const char *port)
{
    memset(&m_addr, 0, sizeof(m_addr));
    m_addr.sin_family = AF_INET;
    m_addr.sin_port = htons(atoi(port));
    TEST_Z(m_ec = rdma_create_event_channel());
    TEST_NZ(rdma_create_id(m_ec, &m_listener, NULL, RDMA_PS_TCP));
    TEST_NZ(rdma_bind_addr(m_listener, (struct sockaddr *)&m_addr));
    TEST_NZ(rdma_listen(m_listener, 10)); /* backlog=10 is arbitrary */
}

rdma_rc_server_loop::~rdma_rc_server_loop()
{
    rdma_destroy_id(m_listener);
    rdma_destroy_event_channel(m_ec);
}

void rdma_rc_server_loop::run()
{
    event_loop(false); // don't exit on disconnect
}

void rc_server_loop(const char *port)
{
    rdma_rc_server_loop loop(port);
    loop.run();
}

void rc_disconnect(struct rdma_cm_id *id)
{
    rdma_disconnect(id);
}

struct ibv_pd *rc_get_pd()
{
    return s_ctx->pd;
}
