#ifndef SAVIME_STAGING_H
#define SAVIME_STAGING_H

#include <condition_variable>
#include <iostream>
#include <list>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include "chann.h"
#include "../lib/savime_lib.h"


#define STRBUFFER 100

namespace staging {

    constexpr std::size_t MAX_DATASET_NAME_LEN = 64;
    constexpr std::size_t MAX_TYPE_NAME_LEN = 16;

    enum class operation {
        create_dataset,
        run_savime,
    };

    struct __attribute__ ((__packed__)) request {
        operation op;

        union {
            struct {
                size_t size;
                char name[MAX_DATASET_NAME_LEN];
                char type[MAX_TYPE_NAME_LEN];
            } dataset;

            struct {
                size_t size;
            } query;
        } data;
    };

    enum class result {
        err,
        ok,
    };

    struct __attribute__ ((__packed__)) response {
        result status;
    };
       
    struct dataset_writer {
        std::string name;
        std::string type;
        char *buf;
        std::size_t len;
         
        dataset_writer();
        dataset_writer(const std::string& _name,
                const std::string& _type, char *_buf, std::size_t _len);

        void run(char *host, char *service);
    };
    
    struct savime_query {
        std::string q;
        std::string file;
        bool is_create_dataset;
        std::shared_ptr<dataset_writer> writer;
    };

    class communicator {
    public:   
        SavimeConn _conn;
        std::string _host;
        std::string _service;
        char _chost[STRBUFFER];
        char _cservice[STRBUFFER];
        std::list<std::thread> _thrds;
        std::list<savime_query> _queries;
        
        std::atomic<bool> _keep_working;
        std::atomic<bool> _idle;
        std::mutex _queries_mtx;
        std::mutex _mutex;
        std::condition_variable _condVar;
        chann<dataset_writer> _writers;

        communicator(const std::string& address, std::size_t num_threads);

        ~communicator();

        void create_dataset(const std::string& name,
                const std::string& type, char *buf, std::size_t len);

        std::string run_savime(const std::string& query);
        
        std::string run_savime_sync(const std::string& query);

        /**
         * Joins the communication threads if any.
         */
        void sync();
        
        void worker();
        
        bool isIdle(){return _idle;}
    };

    class server {
        
    public:
        server(const std::string& address, std::size_t num_threads = 1);

        ~server();

        /**
         * Executes a query on SAVIME.
         *
         * Returns the response_text of QueryResultHandle.
         *
         * Note: Currently only the create_tar and load_subtar queries are
         * allowed.
         */
        std::string run_savime(const std::string& query);
      
        /**
         * Executes a query on SAVIME synchronously.
         *
         * Returns the response_text of QueryResultHandle.
         *
         * Note: Currently only the create_tar and load_subtar queries are
         * allowed.
         */
        std::string run_savime_sync(const std::string& query);
        /**
         * Synchronizes with the staging server, not savime.  Now the data are
         * sent and you can reuse your buffers.
         */
        void sync();
        
        /**
        * Synchronizes with the staging server and savime.
        */
        void finalize();

    private:
        friend class dataset;
        std::shared_ptr<communicator> _comm = nullptr;
    };

    class dataset {
    public:

        /**
         * Constructs a dataset object to represent a dataset on savime.
         *
         * \name The name of the dataset.
         * \type The name of the type, like int or double.
         * \st The staging server where the dataset must be created.
         */
        dataset(const std::string& name, const std::string& type, server& st);

        ~dataset();

        /**
         * Writes the len bytes of buf to a (possibly in-memory) temporary file
         * in the staging server.  This method returns immediately, the
         * communication happens in another thread.
         *
         * See staging::server::sync() for wait on this operation.
         *
         * \buf A pointer to a buffer of memory, like the return of malloc().
         * \len The size (in bytes) of the buf.
         */
        void write(char *buf, std::size_t len);

        /**
         * NOT IMPLEMENTED YET!
         *
         * Gets the SAVIME response for the create_dataset query of this
         * dataset.
         *
         * Returns the response_text of QueryResultHandle.  An empty
         * string means a not finished query execution.
         */
        std::string savime_response();

    private:
        bool _created;
        bool _idle;
        std::string _name;
        std::string _type;
        std::weak_ptr<communicator> _comm;
        std::list<std::thread> _thrds;
        std::shared_ptr<communicator> _get_comm();
    };
    
   

} // namespace staging

std::ostream& operator<<(std::ostream& o, const staging::response& r);

#endif // SAVIME_STAGING_H
