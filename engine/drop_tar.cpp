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

DropTAR::DropTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult DropTAR::Run() {
  try {
    std::list<ParameterPtr> parameters = _operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string tarName = parameter->literal_str;
    tarName = trim_delimiters(tarName);
    parameters.pop_front();

    int32_t defaultTarsId = _configurationManager->GetIntValue(DEFAULT_TARS);
    TARSPtr defaultTars = _metadataManager->GetTARS(defaultTarsId);
    TARPtr tar = _metadataManager->GetTARByName(defaultTars, tarName);

    if (tar == nullptr)
      throw std::runtime_error("There is no TAR named " + tarName + ".");

    if (_metadataManager->RemoveTar(defaultTars, tar) != SAVIME_SUCCESS)
      throw std::runtime_error("Could not remove " + tarName + ".");

    _metadataManager->RegisterQuery(_queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

