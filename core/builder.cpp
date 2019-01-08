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
#include <stdexcept>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sstream>
#include <string>
#include <config.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "include/builder.h"
#include "include/config_manager.h"
#include "include/system_logger.h"
#include "include/session.h"
#include "include/engine.h"
#include "include/parser.h"
#include "include/optimizer.h"
#include "include/metadata.h"

#include "../session/default_session_manager.h"
#include "../parser/default_parser.h"
#include "../optimizer/default_optimizer.h"
#include "../metada/default_metada_manager.h"
#include "../connection/default_connection_manager.h"
#include "../configuration/default_config_manager.h"
#include "../core/include/builder.h"
#include "../core/include/config_manager.h"
#include "../core/include/system_logger.h"
#include "../core/include/session.h"
#include "../core/include/engine.h"
#include "../core/include/parser.h"
#include "../core/include/optimizer.h"
#include "../core/include/metadata.h"
#include "../engine/include/default_engine.h"
#include "../query/default_query_data_manager.h"
#include "../storage/default_storage_manager.h"

#define STARTUP_MSG(DAEMON, THREADS, PORT, SHM, SEC, CAT, RDMA, TYPE, TBB)     \
  "\nDAEMON MODE:      " + DAEMON + "\n"                                       \
                                    "MAX CORES USAGE:  " +                     \
      std::to_string(THREADS) + "\n"                                           \
                                "PORT:             " +                         \
      std::to_string(PORT) + "\n"                                              \
                             "SHM DIR:          " +                            \
      SHM + "\n"                                                               \
            "STORAGE DIR:      " +                                             \
      SEC + "\n"                                                               \
          "CATALYST ENABLED: " +                                               \
      CAT + "\n"                                                               \
            "RDMA ENABLED:     " +                                             \
      RDMA + "\n"                                                              \
            "TYPE SUPPORT:     " +                                             \
      TYPE + "\n"                                                              \
            "TBB SUPPORT:      " +                                             \
      TBB + "\n"
      
      
ModulesBuilder::ModulesBuilder(int argc, char **args) {
#define BOOT_QUERY_FILE "boot_query_file"

  char c;
  int64_t threads, subtars;
  string bootFile;
  BuildConfigurationManager();

  struct option longopts[] = {{DAEMON_MODE, no_argument, nullptr, 'D'},
                              {MAX_THREADS, required_argument, nullptr, 'm'},
                              {MAX_PARA_SUBTARS, required_argument, nullptr, 'n'},
                              {SHM_STORAGE_DIR, required_argument, nullptr, 's'},
                              {SEC_STORAGE_DIR, required_argument, nullptr, 'd'},
                              {CONFIG_FILE, required_argument, nullptr, 'f'},
                              {BOOT_QUERY_FILE, required_argument, nullptr, 'b'},
                              {0, 0, 0, 0}};

  while ((c = (char)getopt_long(argc, args, "Dm:n:s:d:f:b:", longopts, nullptr)) != -1) {
    switch (c) {
    case 'D':_configurationManager->SetBooleanValue(DAEMON_MODE, true);
      break;
    case 'm':threads = strtol(optarg, nullptr, 10);
      _configurationManager->SetIntValue(MAX_THREADS, static_cast<int32_t>(threads));
      break;
    case 'n':subtars = strtol(optarg, nullptr, 10);
      _configurationManager->SetIntValue(MAX_PARA_SUBTARS, static_cast<int32_t>(subtars));
      break;
    case 's':
      _configurationManager->SetStringValue(SHM_STORAGE_DIR,
                                            std::string(optarg));
      break;
    case 'd':
      _configurationManager->SetStringValue(SEC_STORAGE_DIR,
                                            std::string(optarg));
      break;
    case 'f':_configurationManager->LoadConfigFile(std::string(optarg));
      break;
    case 'b':bootFile = std::string(optarg);
      break;
    }
  }

  BuildSystemLogger();

  bool daemonModeSet = _configurationManager->GetBooleanValue(DAEMON_MODE);
  int32_t numThreads = _configurationManager->GetIntValue(MAX_THREADS);
  int32_t numSubtars = _configurationManager->GetIntValue(MAX_PARA_SUBTARS);
  int32_t port = _configurationManager->GetIntValue(SERVER_PORT(0));
  std::string shmPath = _configurationManager->GetStringValue(SHM_STORAGE_DIR);
  std::string secPath = _configurationManager->GetStringValue(SEC_STORAGE_DIR);

  if (!EXIST_FILE(shmPath)) {
    if (mkdir(shmPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
      throw std::runtime_error("Could not create dir " + shmPath + ". " +
          strerror(errno));
  }

  if (!EXIST_FILE(secPath))
    if (mkdir(secPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
      throw std::runtime_error("Could not create dir " + secPath + ". " +
          strerror(errno));

  if (!bootFile.empty())
    RunBootQueryFile(bootFile);

  if (daemonModeSet)
    Daemonize();

  string daemon = (daemonModeSet) ? "yes" : "no";

#ifdef CATALYST
  string catalyst = "yes";
#else
  string catalyst = "no";
#endif

#ifdef STAGING
  string staging = "yes";
#else
  string staging = "no";
#endif

#ifdef FULL_TYPE_SUPPORT
  string type = "FULL";
#else
  string type = "BASIC";
#endif

#ifdef TBB_SUPPORT
  string tbb_support = "yes";
#else
  string tbb_support = "no";
#endif


  string SAVIME_ASCII_LOGO = string("\n   _____             _              \n"
                                    "  / ___/____ __   __(_)___ ___  ___ \n"
                                    "  \\__ \\/ __ `/ | / / / __ `__ \\/ _ \\\n"
                                    " ___/ / /_/ /| |/ / / / / / / /  __/\n"
                                    "/____/\\__,_/ |___/_/_/ /_/ /_/\\___/  v" +
                                    string(PACKAGE_VERSION) + "\n");
  
  /*Log startup message*/
  string startupMsg =
      SAVIME_ASCII_LOGO + STARTUP_MSG(daemon, numThreads * numSubtars, port,
          shmPath, secPath, catalyst, staging, type, tbb_support);
  _systemLogger->LogEvent("SAVIME", startupMsg);
}

void ModulesBuilder::Daemonize() {

  _systemLogger->LogEvent("SAVIME", "Starting SAVIME as a daemon.");

  pid_t pid, sid;
  string secDir = _configurationManager->GetStringValue(SEC_STORAGE_DIR);
  string logFile = _configurationManager->GetStringValue(LOG_FILE);

  pid = fork();
  if (pid < 0) {
    throw std::runtime_error(
        string("Error while entering in daemon mode. Unable to fork: ") +
            strerror(errno));
  }
  if (pid > 0) {
    exit(EXIT_SUCCESS);
  }

  umask(0);

  sid = setsid();
  if (sid < 0) {
    throw std::runtime_error(string("Error while entering in daemon mode. "
                                    "Unable to set new unix session: ") +
        strerror(errno));
  }

  if ((chdir(secDir.c_str())) < 0) {
    throw std::runtime_error(string("Error while entering in daemon mode. "
                                    "Unable to set new working dir: ") +
        strerror(errno));
  }

  close(STDOUT_FILENO);
  close(STDIN_FILENO);
  close(STDERR_FILENO);

  FILE *new_stdout = freopen(pathAppend(secDir, logFile).c_str(), "w", stdout);
  if (!new_stdout) {
    throw std::runtime_error(string("Error while entering in daemon mode. "
                                    "Unable to set new stdout file: ") +
        strerror(errno));
  }
  stdout = new_stdout;
}

ConfigurationManagerPtr ModulesBuilder::BuildConfigurationManager() {
  if (_configurationManager == nullptr) {
    _configurationManager =
        ConfigurationManagerPtr(new DefaultConfigurationManager());
  }

  return _configurationManager;
}

SystemLoggerPtr ModulesBuilder::BuildSystemLogger() {
  if (_systemLogger == nullptr) {
    _systemLogger =
        SystemLoggerPtr(new DefaultSystemLogger(BuildConfigurationManager()));
  }

  return _systemLogger;
}

EnginePtr ModulesBuilder::BuildEngine() {
  if (_engine == nullptr) {
    _engine = EnginePtr(
        new DefaultEngine(BuildConfigurationManager(), BuildSystemLogger(),
                          BuildMetadataManager(), BuildStorageManager()));

    ((DefaultEngine *) _engine.get())->SetThisPtr(_engine);
  }

  return _engine;
}

ParserPtr ModulesBuilder::BuildParser() {
  if (_parser == nullptr) {
    _parser = ParserPtr(
        new DefaultParser(BuildConfigurationManager(), BuildSystemLogger()));
    _parser->SetMetadataManager(BuildMetadataManager());
    _parser->SetStorageManager(BuildStorageManager());
  }

  return _parser;
}

OptimizerPtr ModulesBuilder::BuildOptimizer() {
  if (_optimizer == nullptr) {
    _optimizer = OptimizerPtr(
        new DefaultOptimizer(BuildConfigurationManager(), BuildSystemLogger(),
          BuildMetadataManager()));
    _optimizer->SetParser(BuildParser());
  }

  return _optimizer;
}

MetadataManagerPtr ModulesBuilder::BuildMetadataManager() {
  if (_metadataManager == nullptr) {
    _metadataManager = MetadataManagerPtr(new DefaultMetadataManager(
        BuildConfigurationManager(), BuildSystemLogger()));
  }

  return _metadataManager;
}

ConnectionManagerPtr ModulesBuilder::BuildConnectionManager() {
  if (_connectionManager == nullptr) {
    _connectionManager = ConnectionManagerPtr(new DefaultConnectionManager(
        BuildConfigurationManager(), BuildSystemLogger()));
  }

  return _connectionManager;
}

StorageManagerPtr ModulesBuilder::BuildStorageManager() {
  if (_storageManager == nullptr) {
    _storageManager = StorageManagerPtr(new DefaultStorageManager(
        BuildConfigurationManager(), BuildSystemLogger()));
    ((DefaultStorageManager *) (_storageManager.get()))
        ->SetThisPtr(
            std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
  }

  return _storageManager;
}

QueryDataManagerPtr ModulesBuilder::BuildQueryDataManager() {
  if (_queryDataManager == nullptr) {
    _queryDataManager = QueryDataManagerPtr(new DefaultQueryDataManager(
        BuildConfigurationManager(), BuildSystemLogger()));
  }

  return _queryDataManager;
}

SessionManagerPtr ModulesBuilder::BuildSessionManager() {
  if (_sessionManager == nullptr) {
    _sessionManager = SessionManagerPtr(new DefaultSessionManager(
        BuildConfigurationManager(), BuildSystemLogger(),
        BuildConnectionManager(), BuildEngine(), BuildParser(),
        BuildOptimizer(), BuildMetadataManager(), BuildQueryDataManager()));
  }

  return _sessionManager;
}

void ModulesBuilder::RunBootQueryFile(string queryFile) {
  std::string query;
  int queryId = 0;
  auto configurationManager = BuildConfigurationManager();
  auto systemLogger = BuildSystemLogger();
  auto parser = BuildParser();
  auto engine = BuildEngine();

  _systemLogger->LogEvent("SAVIME", "Processing startup file.");

  std::ifstream input(queryFile);
  while (std::getline(input, query)) {
    auto queryDataManager = QueryDataManagerPtr(
        new DefaultQueryDataManager(configurationManager, systemLogger));
    queryDataManager->SetQueryId(queryId++);
    queryDataManager->AddQueryTextPart(query);

    if (parser->Parse(queryDataManager) != SAVIME_SUCCESS) {
      continue;
    }

    if (engine->Run(queryDataManager, this) != SAVIME_SUCCESS) {
      systemLogger->LogEvent("Builder",
                             "Error during boot query execution: " + query +
                                 " " + queryDataManager->GetErrorResponse());
    }
  }
}