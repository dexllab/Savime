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
*    HERMANO L. S. LUSTOSA				JANUARY 2019
*/
#include "include/ddl_operators.h"

DropType::DropType(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                   QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                   StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult DropType::Run() {
  try {
    std::list<ParameterPtr> parameters = _operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string typeName = parameter->literal_str;
    typeName = trim_delimiters(typeName);
    parameters.pop_front();

    int32_t defaultTarsId = _configurationManager->GetIntValue(DEFAULT_TARS);
    TARSPtr defaultTars = _metadataManager->GetTARS(defaultTarsId);
    TypePtr type = _metadataManager->GetTypeByName(defaultTars, typeName);

    if (type == nullptr)
      throw std::runtime_error("There is no type named " + typeName + ".");

    if (_metadataManager->RemoveType(defaultTars, type) != SAVIME_SUCCESS)
      throw std::runtime_error(
        "Could not remove type. It was possibly given to at least one TAR.");

    _metadataManager->RegisterQuery(_queryDataManager->GetQueryText());
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}
