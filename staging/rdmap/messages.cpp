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
#include <errno.h>
#include <stdio.h>

#include "common.h"
#include "messages.h"

static void basic_post_recv(struct rdma_cm_id *id, void *buf, size_t size,
        struct ibv_mr *mr)
{
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    sge.addr = (uintptr_t)buf;
    sge.length = size;
    sge.lkey = mr->lkey;
    TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
}

static void basic_post_send(struct rdma_cm_id *id, void *buf, size_t size,
        struct ibv_mr *mr)
{
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uintptr_t)id;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;
    sge.addr = (uintptr_t)buf;
    sge.length = size;
    sge.lkey = mr->lkey;
    TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
}

void post_recv_msg(struct rdma_cm_id *id)
{
    struct msg_context *ctx = (struct msg_context *)id->context;
    basic_post_recv(id, ctx->msg, sizeof(*ctx->msg), ctx->msg_mr);
}

void post_send_msg(struct rdma_cm_id *id)
{
    struct msg_context *ctx = (struct msg_context *)id->context;
    basic_post_send(id, ctx->msg, sizeof(*ctx->msg), ctx->msg_mr);
}

void post_recv_req(struct rdma_cm_id *id)
{
    struct msg_context *ctx = (struct msg_context *)id->context;
    basic_post_recv(id, ctx->req, sizeof(*ctx->req), ctx->req_mr);
}

void post_send_req(struct rdma_cm_id *id)
{
    struct msg_context *ctx = (struct msg_context *)id->context;
    basic_post_send(id, ctx->req, sizeof(*ctx->req), ctx->req_mr);
}
