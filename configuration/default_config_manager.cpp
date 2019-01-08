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
#include "default_config_manager.h"
#include "../include/savime.h"
#include "../include/util.h"
#include <fstream>
#include <vector>

DefaultConfigurationManager::DefaultConfigurationManager() {
  SetIntValue(SERVER_ID, 0);
  SetIntValue(NUM_SERVERS, 1);
  SetStringValue(SERVER_ADDRESS(0), "127.0.0.1");
  SetIntValue(SERVER_PORT(0), 65000);
  SetStringValue(SERVER_UNIX_PATH(0), "/tmp/savime-socket");
  SetStringValue(RDMA_ADDRESS(0), "127.0.0.1");
  SetIntValue(RDMA_PORT(0), 65001);

  SetStringValue(SHM_STORAGE_DIR, "/dev/shm/savime");
  SetStringValue(SEC_STORAGE_DIR, "/dev/shm/savime");
  SetStringValue(LOG_FILE, "savime-log.txt");
  SetStringValue(LOG_TRACE_FILE, "savime-trace.txt");
  SetBooleanValue(DAEMON_MODE, false);
  SetIntValue(SUBDIRS_NUM, 1024);
  
  SetIntValue(MAX_CONNECTIONS, 30);
  SetLongValue(HUGE_TBL_THRESHOLD, 1073741824l / 2);
  SetLongValue(HUGE_TBL_SIZE, 2 * 1024l * 1024l);
  SetIntValue(MAX_CONNECTIONS, 30);
  SetLongValue(MAX_TFX_BUFFER_SIZE, 512l * 1024l * 1024l * 1024l);
  SetLongValue(MAX_STORAGE_SIZE, 600l * 1024l * 1024l * 1024l);

  SetIntValue(MAX_THREADS, 2);
  SetIntValue(MAX_PARA_SUBTARS, 2);
  SetIntValue(MAX_THREADS_ENGINE, 1);
  SetIntValue(WORK_PER_THREAD, 10);
  SetIntValue(DEFAULT_TARS, 1);
  SetBooleanValue(ITERATOR_MODE_ENABLED, true);
  SetBooleanValue(FREE_BUFFERED_SUBTARS, false);
  SetLongValue(MAX_SPLIT_LEN, 100000);
  SetLongValue(MAX_AGGREGATE_BUFFER, 10*1024l * 1024l * 1024l);
  SetStringValue(CATALYST_EXECUTABLE, "savime-catalyst");

  SetBooleanValue(OPERATOR("catalyze"), true);
  SetBooleanValue(OPERATOR("store"), true);

  SetBooleanValue(NUMERICAL_FUNCTION("cos"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("cos"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("sin"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("sin"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("tan"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("tan"), 1);

  SetBooleanValue(NUMERICAL_FUNCTION("acos"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("acos"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("asin"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("asin"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("atan"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("atan"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("atan2"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("atan2"), 2);

  SetBooleanValue(NUMERICAL_FUNCTION("cosh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("cosh"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("sinh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("sinh"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("tanh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("tanh"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("acosh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("acosh"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("asinh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("asinh"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("atanh"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("atanh"), 1);

  SetBooleanValue(NUMERICAL_FUNCTION("exp"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("exp"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("log"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("log"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("log10"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("log10"), 1);

  SetBooleanValue(NUMERICAL_FUNCTION("pow"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("pow"), 2);
  SetBooleanValue(NUMERICAL_FUNCTION("sqrt"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("sqrt"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("ceil"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("ceil"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("floor"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("floor"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("round"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("round"), 1);
  SetBooleanValue(NUMERICAL_FUNCTION("abs"), true);
  SetIntValue(NUMERICAL_FUNCTION_PARAMS("abs"), 1);

  SetBooleanValue(AGGREGATION_FUNCTION("avg"), true);
  SetBooleanValue(AGGREGATION_FUNCTION("max"), true);
  SetBooleanValue(AGGREGATION_FUNCTION("min"), true);
  SetBooleanValue(AGGREGATION_FUNCTION("sum"), true);
  SetBooleanValue(AGGREGATION_FUNCTION("count"), true);
}

bool DefaultConfigurationManager::GetBooleanValue(std::string key) {
  if (_boolKeys.find(key) != _boolKeys.end())
    return _boolKeys[key];
  else
    return false;
}

void DefaultConfigurationManager::SetBooleanValue(std::string key, bool value) {
  _boolKeys[key] = value;
}

std::string DefaultConfigurationManager::GetStringValue(std::string key) {
  if (_strKeys.find(key) != _strKeys.end())
    return _strKeys[key];
  else
    return "";
}

void DefaultConfigurationManager::SetStringValue(std::string key,
                                                 std::string value) {
  _strKeys[key] = value;
}

int32_t DefaultConfigurationManager::GetIntValue(std::string key) {
  if (_intKeys.find(key) != _intKeys.end())
    return _intKeys[key];
  else
    return 0;
}

void DefaultConfigurationManager::SetIntValue(std::string key, int32_t value) {
  _intKeys[key] = value;
}

int64_t DefaultConfigurationManager::GetLongValue(std::string key) {
  if (_longKeys.find(key) != _longKeys.end())
    return _longKeys[key];
  else
    return 0;
}

void DefaultConfigurationManager::SetLongValue(std::string key, int64_t value) {
  _longKeys[key] = value;
}

double DefaultConfigurationManager::GetDoubleValue(std::string key) {
  if (_doubleKeys.find(key) != _doubleKeys.end())
    return _doubleKeys[key];
  else
    return 0.0;
}

void DefaultConfigurationManager::SetDoubleValue(std::string key,
                                                 double value) {
  _doubleKeys[key] = value;
}

void DefaultConfigurationManager::LoadConfigFile(string file) {
  string line;
  ifstream infile(file);

  while (getline(infile, line)) {
    line = trim(line);
    if (line.at(0) == '#')
      continue;
    auto splitLine = split(line, ' ');
    if (splitLine.size() != 3)
      continue;

    string key, svalue, type;
    key = trim(splitLine[0]);
    key.erase(std::remove(key.begin(), key.end(), '"'), key.end());
    type = trim(splitLine[1]);
    svalue = trim(splitLine[2]);
    svalue.erase(std::remove(svalue.begin(), svalue.end(), '"'), svalue.end());

    if (type == "s" || type == "S") {
      SetStringValue(key, svalue);
    } else if (type == "i" || type == "I") {
      int32_t val = (int32_t) strtol(svalue.c_str(), nullptr, 10);
      SetIntValue(key, val);
    } else if (type == "l" || type == "L") {
      //int64_t val = atoll(svalue.c_str());
      int64_t val = strtoll(svalue.c_str(), nullptr, 10);
      SetLongValue(key, val);
    } else if (type == "b" || type == "B") {
      bool val = (bool)strtol(svalue.c_str(), nullptr, 10);
      SetBooleanValue(key, val);
    } else if (type == "d" || type == "D") {
      double val = strtod(svalue.c_str(), nullptr);
      SetDoubleValue(key, val);
    } else {
      continue;
    }
  }
}