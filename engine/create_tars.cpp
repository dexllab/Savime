#include <memory>

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

CreateTARS::CreateTARS(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                     QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                     StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult CreateTARS::Run() {
  try {
    ParameterPtr parameter = _operation->GetParameters().front();
    std::string tarsName = parameter->literal_str;
    tarsName = trim_delimiters(tarsName);

    if (_metadataManager->ValidateIdentifier(tarsName, "tars")) {
      TARSPtr newTars = std::make_shared<TARS>();
      newTars->id = UNSAVED_ID;
      newTars->name = tarsName;

      if (_metadataManager->SaveTARS(newTars) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save new TARS: " + tarsName);
      }
    } else {
      throw std::runtime_error(
        "Invalid or already existing identifier for TARS: " + tarsName);
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
  return SAVIME_SUCCESS;
}
