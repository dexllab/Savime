#ifndef CONFIG_MANAGER_H
#define CONFIG_MANAGER_H
/*! \file */
#include <string>
#include <memory>

#define CONFIG_FILE "config_file"

#define SERVER_ID "server_id"
#define NUM_SERVERS "num_servers"
#define SERVER_ADDRESS(x) "server" + std::to_string(x) + "_address"
#define SERVER_PORT(x) "server" + std::to_string(x) + "_port"
#define SERVER_UNIX_PATH(x) "server" + std::to_string(x) + "_unix_path"
#define RDMA_ADDRESS(x) "rdma" + std::to_string(x) + "_address"
#define RDMA_PORT(x) "rdma" + std::to_string(x) + "_port"

#define LOG_FILE "log_file"
#define LOG_TRACE_FILE "log_trace_file"
#define HUGE_TBL_THRESHOLD "huge_tbl_threshold"
#define HUGE_TBL_SIZE "huge_tbl_size"
#define SUBDIRS_NUM "subdirs_num"
#define SHM_STORAGE_DIR "shm_storage_dir"
#define SEC_STORAGE_DIR "sec_storage_dir"

#define DAEMON_MODE "daemon_mode"
#define MAX_CONNECTIONS "max_pending_connections"
#define MAX_TFX_BUFFER_SIZE "max_buffer_size"
#define MAX_STORAGE_SIZE "max_storage"
#define MAX_THREADS "max_num_threads"
#define MAX_PARA_SUBTARS "max_subtars_processed_in_parallel"
#define MAX_THREADS_ENGINE "max_num_threads_engine"
#define REORIENT_PARTITION_SIZE "reorient_partition_size"

#define DEFAULT_TARS "default_tars"
#define WORK_PER_THREAD "work_per_thread"
#define ITERATOR_MODE_ENABLED "iterator_mode"
#define FREE_BUFFERED_SUBTARS "free_buffered_subtars"
#define MAX_AGGREGATE_BUFFER "max_aggregate_buffer"
#define MAX_SPLIT_LEN "max_split_len"
#define CATALYST_EXECUTABLE "catalyst_exe"

#define OPERATOR(x) "op_exists_" + std::string(x)
#define OPERATOR_PARSE_STRING(x) "op_parse_string_" + std::string(x)
#define OPERATOR_SCHEMA_INFER_STRING(x) "op_infer_string_" + std::string(x)
#define OPERATOR_ADDRESS(x) "op_address_" + std::string(x)

#define AGGREGATION_FUNCTION(x) "aggr_exists_" + std::string(x)
#define NUMERICAL_FUNCTION(x) "numfunc_exists_" + std::string(x)
#define NUMERICAL_FUNCTION_PARAMS(x) "numfunc_params_" + std::string(x)
#define NUMERICAL_FUNCTION_ADDRESS(x) "numfunc_address_" + std::string(x)

#define BOOLEAN_FUNCTION(x) "boolfunc_exists_" + std::string(x)
#define BOOLEAN_FUNCTION_PARAMS(x) "boolfunc_params_" + std::string(x)
#define BOOLEAN_FUNCTION_ADDRESS(x) "boolfunc_address_" + std::string(x)

using namespace std;

/**The configuration manager is the module responsible for storing and
 * retrieving configuration keys and values.*/
class ConfigurationManager {

public:
  /**
   * Gets a boolean configuration value.
   * @param key is the key string for the configuration value.
   * @return A boolean value for the configuration key. Returns false
   * if the key is not set.
   */
  virtual bool GetBooleanValue(string key) = 0;

  /**
   * Sets a boolean configuration value;
   * @param key is the key string for the configuration value.
   * @param value is the boolean value to be set.
   */
  virtual void SetBooleanValue(string key, bool value) = 0;

  /**
  * Gets a string configuration value.
  * @param key is the key string for the configuration value.
  * @return A string value for the configuration key. Returns a empty string
  * if the key is not set.
  */
  virtual string GetStringValue(string key) = 0;

  /**
   * Sets a string configuration value;
   * @param key is the key string for the configuration value.
   * @param value is the string value to be set.
   */
  virtual void SetStringValue(string key, std::string value) = 0;

  /**
   * Gets a 32-bit integer configuration value.
   * @param key is the key string for the configuration value.
   * @return A 32-bit integer value for the configuration key. Returns 0
   * if the key is not set.
   */
  virtual int32_t GetIntValue(string key) = 0;

  /**
   * Sets a 32-bit integer configuration value;
   * @param key is the key string for the configuration value.
   * @param value is the 32-bit integer value to be set.
   */
  virtual void SetIntValue(string key, int32_t value) = 0;

  /**
   * Gets a 64-bit integer configuration value.
   * @param key is the key string for the configuration value.
   * @return A 64-bit integer value for the configuration key. Returns 0
   * if the key is not set.
   */
  virtual int64_t GetLongValue(string key) = 0;

  /**
   * Sets a 64-bit integer configuration value;
   * @param key is the key string for the configuration value.
   * @param value is the 64-bit integer value to be set.
   */
  virtual void SetLongValue(string key, int64_t) = 0;

  /**
   * Gets a 64-bit floating point configuration value.
   * @param key is the key string for the configuration value.
   * @return A 64-bit floating point integer value for the configuration key.
   * Returns 0 if the key is not set.
   */
  virtual double GetDoubleValue(string key) = 0;

  /**
   * Sets a 64-bit floating point configuration value;
   * @param key is the key string for the configuration value.
   * @param value is the 64-bit floating point value to be set.
   */
  virtual void SetDoubleValue(string key, double value) = 0;

  /**
   * Loads the configuration file;
   * @param file is the path for the configuration file.
   */
  virtual void LoadConfigFile(string file) = 0;
};

typedef std::shared_ptr<ConfigurationManager> ConfigurationManagerPtr;
#endif /* CONFIG_MANAGER_H */

