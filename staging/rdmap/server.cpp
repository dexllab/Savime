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
#include <mutex>
#include <string>

#include <fcntl.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
//#include <pthread.h>

#include "common.h"
#include "messages.h"
#include "rdmap.h"

#ifdef MAX_PEERS
static uint64_t s_peer_counter = 0;
//static pthread_mutex_t s_peer_counter_mutex;
static std::mutex s_peer_counter_mutex;
#endif /* MAX_PEERS */

struct conn_context {
    struct msg_context mctx;
    char *buffer;
    int current_mr;
    struct ibv_mr *buffer_mr[2];
    int fd;
    uint32_t buflen;
    size_t file_size;
    size_t bytes_recv;
    char file_name[MAX_FILE_NAME];
};

static void on_pre_conn(struct rdma_cm_id *id)
{
    int mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    struct conn_context *ctx =
        (struct conn_context *) malloc(sizeof(struct conn_context));
    //printf("creating context\n");
    if (ctx == NULL) {
        perror("malloc");
        throw std::runtime_error("context malloc failed");
    }
    ctx->buffer_mr[0] = NULL;
    ctx->buffer_mr[1] = NULL;
    ctx->current_mr = 0;
    ctx->bytes_recv = 0;
    id->context = ctx;
    TEST_NZ(posix_memalign((void **) &ctx->mctx.msg, sysconf(_SC_PAGESIZE),
                sizeof(*ctx->mctx.msg)));
    TEST_Z(ctx->mctx.msg_mr = ibv_reg_mr(rc_get_pd(), ctx->mctx.msg,
                sizeof(*ctx->mctx.msg),
                mr_access));
    TEST_NZ(posix_memalign
            ((void **) &ctx->mctx.req, sysconf(_SC_PAGESIZE),
             sizeof(*ctx->mctx.req)));
    TEST_Z(ctx->mctx.req_mr =
            ibv_reg_mr(rc_get_pd(), ctx->mctx.req, sizeof(*ctx->mctx.req),
                mr_access));
    ctx->mctx.msg->id = MSG_INVALID;
    ctx->mctx.req->id = MSG_INVALID;
    post_recv_req(id);		// recv REQ
}

static size_t calc_chunk_size(size_t file_size, size_t bytes_recv)
{
    size_t size = file_size - bytes_recv;
    return (size > BUFFER_SIZE) ? BUFFER_SIZE : size;
}

static void handle_request(struct rdma_cm_id *id)
{
    size_t buflen;
    int file_flags = O_RDWR | O_CREAT | O_EXCL;
    int file_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    int mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    struct conn_context *ctx = (struct conn_context *) id->context;
    memcpy(ctx->file_name, ctx->mctx.req->file_name, MAX_FILE_NAME);
    ctx->file_name[MAX_FILE_NAME - 1] = '\0';
    ctx->file_size = ctx->mctx.req->file_size;
    //printf("opening file %s, size %zu\n", ctx->file_name, ctx->file_size);
    ctx->fd = open(ctx->file_name, file_flags, file_mode);
    if (ctx->fd == -1) {
        perror("open");
        throw std::runtime_error("open() failed");
    }
    if (ftruncate(ctx->fd, (off_t) ctx->file_size) == -1) {
        perror("ftruncate");
        throw std::runtime_error("ftruncate() failed");
    }
    ctx->buffer = (char *)mmap(NULL, ctx->file_size, PROT_READ | PROT_WRITE,
            MAP_SHARED, ctx->fd, 0);
    if (ctx->buffer == NULL) {
        perror("mmap");
        throw std::runtime_error("mmap() failed");
    }
    buflen = calc_chunk_size(ctx->file_size, ctx->bytes_recv);
    //printf("reg %d -> %zu..%zu\n", ctx->current_mr, ctx->bytes_recv, buflen);
    TEST_Z(ctx->buffer_mr[ctx->current_mr] = ibv_reg_mr(rc_get_pd(),
                &ctx->buffer[ctx->
                bytes_recv],
                buflen,
                mr_access));
    //printf("received REQ, sending READY\n");
    ctx->mctx.msg->id = MSG_READY;
    ctx->mctx.msg->data.mreg.addr =
        (uintptr_t) ctx->buffer_mr[ctx->current_mr]->addr;
    ctx->mctx.msg->data.mreg.rkey = ctx->buffer_mr[ctx->current_mr]->rkey;
    post_send_msg(id);
}

static void handle_data(struct rdma_cm_id *id)
{
    int done = 0;
    struct conn_context *ctx = (struct conn_context *) id->context;
    ctx->buflen = ntohl(ctx->mctx.msg->data.sync.buflen);
    done = ctx->bytes_recv == ctx->file_size;
    //printf("received: %" PRIu32 " bytes\n", ctx->buflen);
    ctx->mctx.msg->id = !done ? MSG_READY : MSG_DONE;
    ctx->current_mr = !ctx->current_mr;
    if (ctx->buffer_mr[ctx->current_mr] != NULL) {
        ctx->mctx.msg->data.mreg.addr =
            (uintptr_t) ctx->buffer_mr[ctx->current_mr]->addr;
        ctx->mctx.msg->data.mreg.rkey =
            ctx->buffer_mr[ctx->current_mr]->rkey;
    }
    post_send_msg(id);
    if (done) {
        //printf("dereg %d\n", !ctx->current_mr);
        ibv_dereg_mr(ctx->buffer_mr[!ctx->current_mr]);
    }
}

static void on_completion(struct ibv_wc *wc)
{
    struct rdma_cm_id *id = (struct rdma_cm_id *) (uintptr_t) wc->wr_id;
    struct conn_context *ctx = (struct conn_context *) id->context;
    int mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    if (wc->opcode == IBV_WC_RECV) {
        if (ctx->mctx.req->id == MSG_REQ) {
            handle_request(id);
            ctx->mctx.req->id = MSG_INVALID;
        } else if (ctx->mctx.msg->id == MSG_SYNC) {
            handle_data(id);
        } else {
            printf("?recv?\n");
        }
    } else if (wc->opcode == IBV_WC_SEND) {
        if (ctx->mctx.msg->id == MSG_READY) {
            size_t buflen;
            //printf("sent READY, waiting SYNC\n");
            post_recv_msg(id);
            buflen = calc_chunk_size(ctx->file_size, ctx->bytes_recv);
            ctx->bytes_recv += buflen;
            buflen = calc_chunk_size(ctx->file_size, ctx->bytes_recv);
            if (buflen) {	// reg the next buffer
                //printf("reg %d -> %zu..%zu\n", !ctx->current_mr, ctx->bytes_recv, ctx->bytes_recv + buflen);
                ctx->buffer_mr[!ctx->current_mr] = ibv_reg_mr(rc_get_pd(),
                        &ctx->buffer[ctx->bytes_recv], buflen, mr_access);
                TEST_Z(ctx->buffer_mr[!ctx->current_mr]);
            }
        } else if (ctx->mctx.msg->id == MSG_DONE) {
            //printf("sent DONE\n");
            munmap(ctx->buffer, ctx->file_size);
            close(ctx->fd);
        } else {
            printf("?send?\n");
        }
    } else {
        printf("?opcode?\n");
    }
}

static void on_disconnect(struct rdma_cm_id *id)
{
    struct conn_context *ctx = (struct conn_context *)id->context;
    ibv_dereg_mr(ctx->mctx.msg_mr);
    ibv_dereg_mr(ctx->mctx.req_mr);
    free(ctx->mctx.msg);
    free(ctx->mctx.req);
    free(ctx);
#ifdef MAX_PEERS
    {
        //pthread_mutex_lock(&s_peer_counter_mutex);
        std::lock_guard<std::mutex> guard(s_peer_counter_mutex);
        s_peer_counter += 1;
        if (s_peer_counter == MAX_PEERS) {
            exit(0);
        }
        //pthread_mutex_unlock(&s_peer_counter_mutex);
    }
#endif /* MAX_PEERS */
    //printf("finished transferring %s\n", ctx->file_name);
}

int run_rdma_server(const char *host, int port)
{
    (void)host;
    std::string p = std::to_string(port);
    rc_init(on_pre_conn, NULL, on_completion, on_disconnect);
#ifdef MAX_PEERS
    //pthread_mutex_init(&s_peer_counter_mutex, NULL);
#endif /* MAX_PEERS */
    try {
        rc_server_loop(p.c_str());
    } catch (std::runtime_error& e) {
        return -1;
    }
    return 0;
}

#if 0
int main(int argc, char **argv)
{
    rc_init(on_pre_conn, NULL, on_completion, on_disconnect);
    printf("waiting for connections. interrupt (^C) to exit.\n");
    pthread_mutex_init(&s_peer_counter_mutex, NULL);
    rc_server_loop(DEFAULT_PORT);
    return 0;
}
#endif
