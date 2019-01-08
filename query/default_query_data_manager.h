#include <utility>

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

#include <utility>
#include <map>
#include "../core/include/query_data_manager.h"

class DefaultQueryDataManager : public QueryDataManager {
  int32_t _queryId = UNSAVED_ID;
  int64_t _usedTransferBuffer = UNSAVED_ID;
  QueryPlanPtr _queryPlan;
  std::map<std::string, int> _blockFiles;
  std::map<std::string, std::string> _paths;
  std::string _query;
  std::string _error;
  std::string _responseText;

public:
  DefaultQueryDataManager(ConfigurationManagerPtr configurationManager,
                          SystemLoggerPtr systemLogger)
      : QueryDataManager(std::move(configurationManager), std::move(systemLogger)) {
    _queryPlan = nullptr;
    _usedTransferBuffer = 0;
  }

  QueryDataManagerPtr GetInstance() override;
  void SetQueryId(int32_t queryId) override;
  int32_t GetQueryId() override;
  SavimeResult AddQueryTextPart(std::string queryPart) override;
  void SetErrorResponseText(std::string error) override;
  std::string GetErrorResponse() override;
  void SetQueryResponseText(std::string text) override;
  std::string GetQueryResponseText() override;
  std::string GetQueryText() override;
  std::list<std::string> GetParamsList() override;
  SavimeResult RegisterTransferBuffer(int64_t size) override;
  int32_t GetParamFile(std::string paramName) override;
  std::string GetParamFilePath(std::string paramName) override;
  void RemoveParamFile(std::string paramName) override;
  SavimeResult SetQueryPlan(QueryPlanPtr queryPlan) override;
  QueryPlanPtr GetQueryPlan() override;
  SavimeResult Release() override;
};

#endif /* DEFAULT_QUERY_DATA_MANAGER_H */

