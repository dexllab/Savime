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
#include "include/dml_operators.h"
#include "include/viz.h"
#include "engine/include/predict.h"


int store(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine) {
  const char *error_store =
    "Invalid parameters for store operation. Expected STORE(tar, tar_name),";
  try {
    if (operation->GetParameters().size() != 3)
      throw std::runtime_error(error_store);

    ParameterPtr inputTarParam = operation->GetParametersByName(OPERAND(0));
    ParameterPtr newNameParam = operation->GetParametersByName(OPERAND(1));
    TARSPtr defaultTARS = metadataManager->GetTARS(
      configurationManager->GetIntValue(DEFAULT_TARS));

    if (inputTarParam == nullptr || newNameParam == nullptr)
      throw std::runtime_error(error_store);

    if (inputTarParam->tar == nullptr)
      throw std::runtime_error(error_store);

    TARPtr inputTAR = inputTarParam->tar;
    TARPtr outputTAR = inputTAR->Clone(false, false, false);
    string newName = newNameParam->literal_str;
    newName.erase(std::remove(newName.begin(), newName.end(), '"'),
                  newName.end());

    if (!metadataManager->ValidateIdentifier(newName, "tar"))
      throw std::runtime_error("Invalid identifier for TAR: " + newName);

    if (metadataManager->GetTARByName(defaultTARS, newName) != nullptr)
      throw std::runtime_error("TAR " + newName + " already exists.");

    outputTAR->AlterTAR(UNSAVED_ID, newName);

    if (metadataManager->SaveTAR(defaultTARS, outputTAR) != SAVIME_SUCCESS)
      throw std::runtime_error("Could not save TAR: " + newName);

    // Obtaining subtar generator
    auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
      ->GetGenerators()[inputTAR->GetName()];
    SubtarPtr subtar;
    int32_t subtarCount = 0;

    while (true) {
      subtar = generator->GetSubtar(subtarCount);
      if (subtar == nullptr)
        break;

      if (metadataManager->SaveSubtar(outputTAR, subtar) != SAVIME_SUCCESS)
        throw std::runtime_error("Could not save subtar.");
      subtarCount++;
    }

    metadataManager->RegisterQuery(queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    throw e;
  }

  return SAVIME_SUCCESS;
}

UserDefined::UserDefined(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){
}

SavimeResult UserDefined::GenerateSubtar(SubTARIndex subtarIndex) {

#define _CATALYZE "catalyze"
#define _PREDICT "predict"
#define _STORE "store"

  if (_operation->GetParametersByName(OPERATOR_NAME)->literal_str == _CATALYZE) {
#ifdef CATALYST
    return (SavimeResult)catalyze(subtarIndex, _operation, _configurationManager,
                    _queryDataManager, _metadataManager, _storageManager, _engine);
#else
    return SAVIME_SUCCESS;
#endif
  } else if (_operation->GetParametersByName(OPERATOR_NAME)->literal_str ==
             _STORE) {
    return (SavimeResult) store(subtarIndex, _operation, _configurationManager, _queryDataManager,
                                _metadataManager, _storageManager, _engine);
  } else if (_operation->GetParametersByName(OPERATOR_NAME)->literal_str == _PREDICT) {
    return (SavimeResult) predict(subtarIndex, _operation, _configurationManager, _queryDataManager,
                                _metadataManager, _storageManager, _engine);
    ;
  }
}