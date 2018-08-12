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
#ifndef RDMA_MESSAGES_H
#define RDMA_MESSAGES_H

#define MAX_FILE_NAME 256
#define DEFAULT_PORT "3221"
#define BUFFER_SIZE ((uint32_t)1 << 27)

enum msg_id {
    MSG_INVALID = 0,
    MSG_REQ,
    MSG_READY,
    MSG_SYNC,
    MSG_DONE
};

struct msg_context {
    struct message *msg;
    struct ibv_mr *msg_mr;
    struct request *req;
    struct ibv_mr *req_mr;
};

struct message {
    int id;
    union {
        struct {
            uint64_t addr;
            uint32_t rkey;
        } mreg;
        struct {
            uint32_t buflen;
        } sync;
    } data;
} __attribute__((packed));

struct request {
    int id;
    size_t file_size;
    char file_name[MAX_FILE_NAME];
} __attribute__((packed));

void post_recv_msg(struct rdma_cm_id *id);

void post_send_msg(struct rdma_cm_id *id);

void post_recv_req(struct rdma_cm_id *id);

void post_send_req(struct rdma_cm_id *id);

#endif /* RDMA_MESSAGES_H */
