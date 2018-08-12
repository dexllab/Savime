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
#ifndef DEFAULT_QUERY_DATA_MANAGER_H
#define DEFAULT_QUERY_DATA_MANAGER_H

#include <map>
#include "../core/include/query_data_manager.h"

class DefaultQueryDataManager : public QueryDataManager {
  int32_t _queryId;
  int32_t _file;
  int64_t _usedTransferBuffer;
  QueryPlanPtr _queryPlan;
  std::map<std::string, int> _blockFiles;
  std::map<std::string, std::string> _paths;
  std::string _query;
  std::string _error;
  std::string _responseText;

public:
  DefaultQueryDataManager(ConfigurationManagerPtr configurationManager,
                          SystemLoggerPtr systemLogger)
      : QueryDataManager(configurationManager, systemLogger) {
    _file = 0;
    _queryPlan = nullptr;
    _usedTransferBuffer = 0;
  }

  QueryDataManagerPtr GetInstance();
  void SetQueryId(int32_t queryId);
  int32_t GetQueryId();
  SavimeResult AddQueryTextPart(std::string queryPart);
  void SetErrorResponseText(std::string error);
  std::string GetErrorResponse();
  void SetQueryResponseText(std::string text);
  std::string GetQueryResponseText();
  std::string GetQueryText();
  std::list<std::string> GetParamsList();
  SavimeResult RegisterTransferBuffer(int64_t size);
  int32_t GetParamFile(std::string paramName);
  std::string GetParamFilePath(std::string paramName);
  void RemoveParamFile(std::string paramName);
  SavimeResult SetQueryPlan(QueryPlanPtr queryPlan);
  QueryPlanPtr GetQueryPlan();
  SavimeResult Release();
};

#endif /* DEFAULT_QUERY_DATA_MANAGER_H */

