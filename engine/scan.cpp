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

Scan::Scan(OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine) :
            EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_GENERATOR(_generator, _inputTAR->GetName());
}

SavimeResult Scan::GenerateSubtar(SubTARIndex subtarIndex) {

  while (true) {
    auto subtar = _generator->GetSubtar(subtarIndex);
    if (subtar == nullptr)
      break;
    _generator->TestAndDisposeSubtar(subtarIndex);
    subtarIndex++;
  }

  return SAVIME_SUCCESS;
}