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
#include <string>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <inttypes.h>
#include <libgen.h>

#include "common.h"
#include "messages.h"
#include "rdmap.h"

struct client_context {
    struct msg_context mctx;
    char *buffer;
    int current_mr;
    struct ibv_mr *buffer_mr[2];
    uint64_t peer_addr;
    uint32_t peer_rkey;
    int fd;
    uint32_t buflen;
    size_t file_size;
    size_t bytes_sent;
    const char *file_name;
};

static void send_request(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *) id->context;
    ctx->mctx.req->id = MSG_REQ;
    ctx->mctx.req->file_size = ctx->file_size;
    memset(ctx->mctx.req->file_name, '\0', MAX_FILE_NAME);
    strcpy(ctx->mctx.req->file_name, ctx->file_name);
    post_send_req(id);
}

static void send_sync(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *) id->context;
    ctx->mctx.msg->id = MSG_SYNC;
    ctx->mctx.msg->data.sync.buflen = htonl(ctx->buflen);
    post_send_msg(id);
}

static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
    struct ibv_sge sge;
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct client_context *ctx = (struct client_context *) id->context;
    ctx->buflen = len;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t) id;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.rkey = ctx->peer_rkey;
    wr.wr.rdma.remote_addr = ctx->peer_addr;
    wr.num_sge = 0;
    wr.sg_list = NULL;
    if (len) {
        wr.num_sge = 1;
        wr.sg_list = &sge;
        sge.length = len;
        sge.lkey = ctx->buffer_mr[ctx->current_mr]->lkey;
        sge.addr = (uintptr_t) & ctx->buffer[ctx->bytes_sent];
        TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
    } else {
        send_sync(id);
    }
}

static ssize_t calc_chunk_size(size_t file_size, size_t bytes_sent)
{
    ssize_t size = file_size - bytes_sent;
    return (size > BUFFER_SIZE) ? BUFFER_SIZE : size;
}

static void send_next_chunk(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *) id->context;
    ssize_t size = calc_chunk_size(ctx->file_size, ctx->bytes_sent);
    if (size < 0) {
        throw std::runtime_error("too many bytes sent\n");
    }
    write_remote(id, size);
    ctx->bytes_sent += size;
}

static void on_pre_conn(struct rdma_cm_id *id)
{
    int rt;
    int mr_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
    struct client_context *ctx = (struct client_context *) id->context;
    ctx->buffer = (char *)mmap(NULL, ctx->file_size,
            PROT_READ | PROT_WRITE, MAP_SHARED, ctx->fd, 0);
    if (ctx->buffer == MAP_FAILED) {
        perror("mmap");
        throw std::runtime_error("mapping failed");
    }
    rt = posix_memalign((void **) &ctx->mctx.msg, sysconf(_SC_PAGESIZE),
            sizeof(*ctx->mctx.msg));
    if (rt == ENOMEM) {
        throw std::runtime_error("cannot allocate aligned memory");
    }
    TEST_Z(ctx->mctx.msg_mr = ibv_reg_mr(rc_get_pd(),
                ctx->mctx.msg,
                sizeof(*ctx->mctx.msg),
                mr_access));
    rt = posix_memalign((void **) &ctx->mctx.req, sysconf(_SC_PAGESIZE),
            sizeof(*ctx->mctx.req));
    if (rt == ENOMEM) {
        throw std::runtime_error("cannot allocate aligned memory");
    }
    TEST_Z(ctx->mctx.req_mr =
            ibv_reg_mr(rc_get_pd(), ctx->mctx.req, sizeof(*ctx->mctx.req),
                mr_access));
    ctx->mctx.msg->id = MSG_INVALID;
    ctx->mctx.req->id = MSG_INVALID;
    ctx->bytes_sent = 0;
    ctx->current_mr = 0;
}

static void on_connect(struct rdma_cm_id *id)
{
    send_request(id);
}

static void on_completion(struct ibv_wc *wc)
{
    struct rdma_cm_id *id = (struct rdma_cm_id *) (uintptr_t) (wc->wr_id);
    struct client_context *ctx = (struct client_context *) id->context;
    if (wc->opcode & IBV_WC_RECV) {
        if (ctx->mctx.msg->id == MSG_READY) {
            ssize_t size;
            ctx->peer_addr = ctx->mctx.msg->data.mreg.addr;
            ctx->peer_rkey = ctx->mctx.msg->data.mreg.rkey;
            //printf("received READY, sending chunk\n");
            send_next_chunk(id);
            size = calc_chunk_size(ctx->file_size, ctx->bytes_sent);
            if (size < 0) {
                throw std::runtime_error("too many bytes sent\n");
            }
            if (size) {
                //printf("reg %d -> %zu..%zu\n", !ctx->current_mr, ctx->bytes_sent, ctx->bytes_sent + size);
                ctx->buffer_mr[!ctx->current_mr] = ibv_reg_mr(rc_get_pd(),
                        &ctx->
                        buffer[ctx->
                        bytes_sent],
                        size,
                        IBV_ACCESS_LOCAL_WRITE);
                TEST_Z(ctx->buffer_mr[!ctx->current_mr]);
            }
        } else if (ctx->mctx.msg->id == MSG_DONE) {
            //printf("received DONE, disconnecting\n");
            rc_disconnect(id);
        } else {
            printf("?recv?\n");
        }
    } else if (wc->opcode == IBV_WC_RDMA_WRITE) {
        //printf("sent %" PRIu32 " bytes [%zu of %zu], sending SYNC\n", ctx->buflen, ctx->bytes_sent, ctx->file_size);
        send_sync(id);
    } else if (wc->opcode == IBV_WC_SEND) {
        ssize_t size = calc_chunk_size(ctx->file_size, ctx->bytes_sent);
        if (size < 0) {
            throw std::runtime_error("too many bytes sent\n");
        }
        if (ctx->mctx.req->id == MSG_REQ) {
            //printf("sent REQ, waiting READY, " "need to send %zd bytes\n", size);
            post_recv_msg(id);
            if (size) {
                //printf("reg %d -> %zu..%zu\n", ctx->current_mr, ctx->bytes_sent, ctx->bytes_sent + size);
                ctx->buffer_mr[ctx->current_mr] = ibv_reg_mr(rc_get_pd(),
                        &ctx->
                        buffer[ctx->
                        bytes_sent],
                        size,
                        IBV_ACCESS_LOCAL_WRITE);
                TEST_Z(ctx->buffer_mr[ctx->current_mr]);
            }
            ctx->mctx.req->id = MSG_INVALID;
        } else if (ctx->mctx.msg->id == MSG_SYNC) {
            //printf("sent SYNC, waiting next READY (or DONE)\n");
            post_recv_msg(id);
            if (ctx->buflen) {
                //printf("dereg [%d]\n", ctx->current_mr);
                ibv_dereg_mr(ctx->buffer_mr[ctx->current_mr]);
            }
            ctx->current_mr = !ctx->current_mr;
        } else {
            printf("?send?\n");
        }
    } else {
        printf("?opcode?\n");
    }
}

int run_rdma_client(const char *host, int port, int fd, size_t size)
{
    int rt = 0;
    struct client_context ctx;
    std::string p = std::to_string(port);
    ctx.fd = fd;
    ctx.buflen = 0;
    ctx.file_size = size;
    /* ctx.file_name = basename(path); */
    rc_init(on_pre_conn, on_connect, on_completion, NULL);
    try {
        rc_client_loop(host, p.c_str(), &ctx);
    } catch (std::runtime_error& e) {
        rt = -1;
    }
    ibv_dereg_mr(ctx.mctx.msg_mr);
    ibv_dereg_mr(ctx.mctx.req_mr);
    free(ctx.mctx.msg);
    free(ctx.mctx.req);
    close(ctx.fd);
    return rt;
}

#if 0
int main(int argc, char **argv)
{
    struct stat sb;
    struct client_context ctx;
    //printf("bufsize: %" PRIu32 "\n", BUFFER_SIZE);
    if (argc != 3) {
        fprintf(stderr, "usage: %s <server-address> <file-name>\n",
                argv[0]);
        return 1;
    }
    ctx.file_name = basename(argv[2]);
    ctx.fd = open(argv[2], O_RDWR);
    if (ctx.fd == -1) {
        fprintf(stderr, "unable to open input file \"%s\"\n",
                ctx.file_name);
        return 1;
    }
    if (fstat(ctx.fd, &sb) == -1) {
        perror("fstat");
        return 1;
    }
    ctx.buflen = 0;
    ctx.file_size = (size_t) sb.st_size;
    rc_init(on_pre_conn, on_connect, on_completion, NULL);
    rc_client_loop(argv[1], DEFAULT_PORT, &ctx);
    ibv_dereg_mr(ctx.mctx.msg_mr);
    ibv_dereg_mr(ctx.mctx.req_mr);
    free(ctx.mctx.msg);
    free(ctx.mctx.req);
    close(ctx.fd);
    return 0;
}
#endif
