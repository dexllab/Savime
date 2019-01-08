#include <utility>

#ifndef KERNEL_H
#define KERNEL_H
/*! \file */
#include <string>
#include <memory>
#include "system_logger.h"
#include "config_manager.h"
    
using namespace std;

/**Enum with codes for results of executions of main modules functions. */
typedef enum {
  SAVIME_SUCCESS /*!<Indicates sucessful execution. */
  ,
  SAVIME_FAILURE /*!<Indicates failure during execution.*/
} SavimeResult;

/**
 * Base class for all modules in SAVIME. It contains a reference to
 * the standard SystemLogger and ConfigurationManager.
 */
class SavimeModule {

protected:
  string _moduleName; /*!<String containing the modules name for logging
                         purposes.*/
  ConfigurationManagerPtr
      _configurationManager; /*!<Instance of the standard ConfigurationManager*/
  SystemLoggerPtr _systemLogger; /*!<Instance of the standard SystemLogger*/

public:
  /**
  * Constructor.
  * @param moduleName is a string for the module's name.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  SavimeModule(string moduleName, ConfigurationManagerPtr configurationManager,
               SystemLoggerPtr systemLogger) {
    _moduleName = std::move(moduleName);
    _configurationManager = std::move(configurationManager);
    _systemLogger = std::move(systemLogger);
  }

  /**
  * Sets the module's name.
  * @param name is a string containing the module's name.
  */
  void SetName(string name) { this->_moduleName = name; }

  /**
  * Sets the module's standard ConfigurationManager.
  * @param configurationManager is a standard ConfigurationManager.
  */
  void SetConfigurationManager(ConfigurationManagerPtr configurationManager) {
    this->_configurationManager = configurationManager;
  }

  /**
  * Sets the module's standard SystemLogger.
  * @param systemLogger is a standard SystemLogger.
  */
  void SetSystemLogger(SystemLoggerPtr systemLogger) {
    this->_systemLogger = systemLogger;
  }

  /**
  * Sets the module's standard ConfigurationManager.
  * @param configurationManager is a standard ConfigurationManager.
  */
  ConfigurationManagerPtr GetConfigurationManager() {
    return this->_configurationManager;
  }

  /**
  * Sets the module's standard SystemLogger.
  * @param systemLogger is a standard SystemLogger.
  */
  SystemLoggerPtr GetSystemLogger() { return this->_systemLogger; }
};

#endif /* KERNEL_H */
