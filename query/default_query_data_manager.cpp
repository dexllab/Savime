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
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include "../core/include/util.h"
#include "default_query_data_manager.h"


QueryDataManagerPtr DefaultQueryDataManager::GetInstance() {
  QueryDataManagerPtr newInstance = QueryDataManagerPtr(
      new DefaultQueryDataManager(_configurationManager, _systemLogger));
  return newInstance;
}

void DefaultQueryDataManager::SetQueryId(int32_t queryId) {
  _queryId = queryId;
}

int32_t DefaultQueryDataManager::GetQueryId() { return _queryId; }

SavimeResult DefaultQueryDataManager::AddQueryTextPart(std::string queryPart) {
  _query = _query + queryPart;
  return SAVIME_SUCCESS;
}

void DefaultQueryDataManager::SetErrorResponseText(std::string error) {
  _error = error;
}

std::string DefaultQueryDataManager::GetErrorResponse() { return _error; }

void DefaultQueryDataManager::SetQueryResponseText(std::string text) {
  _responseText = text;
}

std::string DefaultQueryDataManager::GetQueryResponseText() {
  return _responseText;
}

std::string DefaultQueryDataManager::GetQueryText() { return _query; }

std::list<std::string> DefaultQueryDataManager::GetParamsList() {
  std::list<std::string> params;

  for (auto &_blockFile : _blockFiles) {
    params.push_back(_blockFile.first);
  }

  return params;
}

SavimeResult DefaultQueryDataManager::RegisterTransferBuffer(int64_t size) {
  int64_t max = _configurationManager->GetLongValue(MAX_TFX_BUFFER_SIZE);
  if ((_usedTransferBuffer + size) <= max) {
    _usedTransferBuffer += size;
    return SAVIME_SUCCESS;
  } else {
    return SAVIME_FAILURE;
  }
}

int32_t DefaultQueryDataManager::GetParamFile(std::string paramName) {
  std::string dir = _configurationManager->GetStringValue(SEC_STORAGE_DIR);

  if (_blockFiles.find(paramName) != _blockFiles.end()) {
    return _blockFiles[paramName];
  } else {
    int32_t subdirsNum = _configurationManager->GetIntValue(SUBDIRS_NUM);
    std::string filePath = generateUniqueFileName(dir, subdirsNum);
    int file = open(filePath.c_str(), O_CREAT | O_WRONLY, 0666);
    _blockFiles[paramName] = file;
    _paths[paramName] = filePath;
    return file;
  }
}

std::string DefaultQueryDataManager::GetParamFilePath(std::string paramName) {
  if (_paths.find(paramName) != _paths.end())
    return _paths[paramName];
  else
    return "";
}

void DefaultQueryDataManager::RemoveParamFile(std::string paramName) {
  if (_paths.find(paramName) != _paths.end()) {
    close(_blockFiles[paramName]);
    _paths.erase(paramName);
    _blockFiles.erase(paramName);
  }
}

SavimeResult DefaultQueryDataManager::SetQueryPlan(QueryPlanPtr queryPlan) {
  _queryPlan = queryPlan;
  return SAVIME_SUCCESS;
}

QueryPlanPtr DefaultQueryDataManager::GetQueryPlan() { return _queryPlan; }

SavimeResult DefaultQueryDataManager::Release() {
  _blockFiles.clear();
  _paths.clear();
  _queryPlan = nullptr;
  _query = "";
  _error = "";
  _responseText = "";
  _usedTransferBuffer = 0;
  return SAVIME_SUCCESS;
}