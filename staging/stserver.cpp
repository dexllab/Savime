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

#define _GNU_SOURCE
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <list>
#include <queue>
#include <thread>
#include <mutex>
#include <stdexcept>
#include <string>
#include "../mapped_memory/mapped_memory.h"
#include "../rdmap/rdmap.h"
#include "../lib/protocol.h"
#include "../lib/savime_lib.h"
#include "rdma_utils.h"
#include "staging.h"

constexpr std::size_t NUM_JOBS{42};

constexpr std::size_t WORKERS{4};
constexpr std::size_t SAVIME_PORT{65000};
constexpr char *SAVIME_HOST{"127.0.0.1"};
constexpr char *STAGING_PORT{"3221"};
constexpr char *STAGING_HOST{"10.1.18.1"};
constexpr char *LOCAL_MEM_DATASET_PATH{"/home/bomberman/data/mem/staging"};
constexpr char *LOCAL_DISK_DATASET_PATH{"/home/bomberman/data/staging"};

inline std::string make_path_to_dataset(const std::string& prefix,
        const std::string& name)
{
    //std::cerr << "getting path for " << name << '\n';
    std::stringstream p;
    p << prefix << "/" << name;
    return p.str();
}

inline std::string make_create_dataset_query(const std::string& name,
        const std::string& type, const std::string& path)
{
    std::stringstream q;

    q << "create_dataset(\""
        << name << ':' << type
        << "\", \"@" << path << "\");";

    return q.str();
}

static void run_rdma_server(char *host, char *service);

static void handle_connection(rdma::endpoint ep);

static void handle_create_dataset(rdma::endpoint& ep, staging::request& req);

static void handle_run_savime(rdma::endpoint& ep, staging::request& req);

static void execute_query(SavimeConn& con, const std::string& query,
        bool retry = true);

static void database_saver_worker();

struct savime_conn {
    SavimeConn conn;

    savime_conn(const char *host, int port) : conn{open_connection(port, host)}
    { }

    ~savime_conn() {
        close_connection(conn);
    }
};

struct database_saver {
    std::string name;
    std::string type;
    std::string path;

    database_saver()
    { }

    database_saver(const std::string& _name, const std::string& _type,
            const std::string& _path)
        : name{_name}, type{_type}, path{_path}
    { }

    void run()
    {
        savime_conn c{SAVIME_HOST, SAVIME_PORT};
        auto query = make_create_dataset_query(name, type, path);

        try {
            execute_query(c.conn, query);

        } catch (std::exception& e) {
            std::cerr << "ERROR: " << e.what() << '\n';
        }

        //unlink(path.c_str()); // TODO uncomment this
    }
};

static std::mutex g_database_savers_mtx;
static std::queue<database_saver> g_database_savers;

static std::mutex g_saved_databases_mtx;
static std::size_t g_saved_databases = 0;

int main(int argc, char **argv)
{
    for (std::size_t i = 0; i < WORKERS; ++i) {
        std::thread t{database_saver_worker};
        pthread_setname_np(t.native_handle(), "[sender]");
        t.detach();
    }

    try {
        run_rdma_server(STAGING_HOST, STAGING_PORT);

    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << '\n';
        return 1;
    }

    return 0;
}

static void run_rdma_server(char *host, char *service)
{
    struct rdma_addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;

    struct ibv_qp_init_attr init_attr;
    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = init_attr.cap.max_recv_wr = max_writers + 1;
    init_attr.cap.max_send_sge = init_attr.cap.max_recv_sge = 1;
    init_attr.sq_sig_all = 0;

    rdma::endpoint ep;
    ep.create(host, service, &init_attr, &hints, false);
    ep.listen(max_backlog);

    for (;;) {
        auto peer = ep.get_request();
        std::thread thrd(handle_connection, std::move(peer));
        pthread_setname_np(thrd.native_handle(), "[peer handler]");
        thrd.detach();
    }
}

static void handle_connection(rdma::endpoint ep)
{
    staging::request req;
    memset(&req, 0, sizeof(req));
    auto req_mr = ep.reg_memory(&req, sizeof(req));

    ep.post_recv(req_mr);
    ep.accept();
    ep.wait_recv(); // wait req

    //staging::response res;

    //try {
        switch (req.op) {
        case staging::operation::create_dataset:
            //std::cerr << "=== start request (CREATE_DATASET)\n";
            handle_create_dataset(ep, req);
            break;

        case staging::operation::run_savime:
            //std::cerr << "=== start request (RUN_SAVIME)\n";
            handle_run_savime(ep, req);
            break;

        default:
            throw std::runtime_error("unknown request operation");
        }

    //     res.status = staging::result::ok;

    // } catch (const std::runtime_error& e) {
    //     std::cerr << "rdma error: " << e.what() << '\n';
    //     res.status = staging::result::err;
    // }

    //std::cerr << "=== end of request\n";

    // auto res_mr = ep.reg_memory(&res, sizeof(res));
    // ep.post_send(res_mr, nullptr, IBV_SEND_SIGNALED);
    // ep.wait_send(); // wait response
}

std::string create_file(const std::string& name, std::size_t size)
{
    std::string path;

    try {
        path = make_path_to_dataset(LOCAL_MEM_DATASET_PATH, name);
        do_truncate(path, size);

    } catch (...) {
        path = make_path_to_dataset(LOCAL_DISK_DATASET_PATH, name);
        do_truncate(path, size);
    }

    return path;
}

// static void database_saver(const std::string& name, const std::string& type,
//         const std::string& path)
// {
//     savime_conn c{SAVIME_HOST, SAVIME_PORT};
//     auto query = make_create_dataset_query(name, type, path);
// 
//     try {
//         execute_query(c.conn, query);
// 
//     } catch (std::exception& e) {
//         std::cerr << "ERROR: " << e.what() << '\n';
//     }
// 
//     unlink(path.c_str());
// }

static void handle_create_dataset(rdma::endpoint& ep, staging::request& req)
{
    staging::response res;

    req.data.dataset.name[staging::MAX_DATASET_NAME_LEN - 1] = '\0';
    auto path = create_file(req.data.dataset.name, req.data.dataset.size);
    mapped_memory out{path};

    try {
        //std::cerr << "Got request CREATE\n";

        //std::cerr << "mapping '" << path << "' to addr: '" << (void *)out.get() << "'\n";
        recv_buffer(ep, out.get(), req.data.dataset.size);

        res.status = staging::result::ok;

    } catch (const std::runtime_error& e) {
        std::cerr << "rdma error: " << e.what() << '\n';
        res.status = staging::result::err;
    }

    auto res_mr = ep.reg_memory(&res, sizeof(res));
    ep.post_send(res_mr, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait response

    auto name = std::string(req.data.dataset.name);
    auto type = std::string(req.data.dataset.type);

    //std::thread t{database_saver, name, type, path};
    //t.detach();

    {
        database_saver ds{name, type, path};
        std::lock_guard<std::mutex> guard{g_database_savers_mtx};
        g_database_savers.push(ds);
    }
}

static void savime_runner(const std::string& query)
{
    savime_conn c{SAVIME_HOST, SAVIME_PORT};

    try {
        execute_query(c.conn, query);

    } catch (std::exception& e) {
        std::cerr << "ERROR: " << e.what() << '\n';
    }
}

static void handle_run_savime(rdma::endpoint& ep, staging::request& req)
{
    staging::response res;

    try {
        auto query = recv_query(ep, req.data.query.size);
        res.status = staging::result::ok;

        //std::thread t{savime_runner, query};
        //t.detach();

        savime_runner(query);

    } catch (const std::runtime_error& e) {
        std::cerr << "rdma error: " << e.what() << '\n';
        res.status = staging::result::err;
    }

    auto res_mr = ep.reg_memory(&res, sizeof(res));
    ep.post_send(res_mr, nullptr, IBV_SEND_SIGNALED);
    ep.wait_send(); // wait response
}

static void execute_query(SavimeConn& con, const std::string& query, bool retry)
{
    auto handle = execute(con, (char *)query.c_str());
    if (handle.is_schema) {
        // TODO: Handle errors for schema responses.
        while (read_query_block(con, handle))
            ;
    } else {
        const std::string success{"Query executed successfully"};
        const std::string response{handle.response_text};
        if (success != response) {
            if (retry) {
                std::cerr << "Retrying query execution";
                return execute_query(con, query, false);
            }
            throw std::runtime_error("SAVIME query failed");
        }
        read_query_block(con, handle);
    }
    dipose_query_handle(handle);
}

static void database_saver_worker()
{
    std::chrono::milliseconds d{500};

    for (;;) {
        bool has_job = false;;
        database_saver s;

        {
            std::lock_guard<std::mutex> guard{g_database_savers_mtx};
            if (!g_database_savers.empty()) {
                //std::cerr << "SIZE: " << g_database_savers.size() << '\n';
                has_job = true;
                s = g_database_savers.front();
                g_database_savers.pop();
            }
        }

        if (has_job) {
            s.run();

            {
                std::lock_guard<std::mutex> guard{g_saved_databases_mtx};
                g_saved_databases += 1;
            }
        }

        std::this_thread::sleep_for(d);

        {
            std::lock_guard<std::mutex> guard{g_saved_databases_mtx};
            if (g_saved_databases >= NUM_JOBS) {
                exit(0);
            }
        }
    }
}
