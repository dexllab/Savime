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
#include "filter.h"

Filter::Filter(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  ParameterPtr filterParam = operation->GetParameters().back();
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_filterGenerator, filterParam->tar->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
}

SavimeResult Filter::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

  int64_t currentSubtar[_numSubtars];
  SubtarPtr subtar[_numSubtars], filterSubtar[_numSubtars];
  DatasetPtr filterDs[_numSubtars];

  for (int32_t i = 0; i < _numSubtars; i++) {
    currentSubtar[i] = subtarIndex + i;

    if (_outputGenerator->GetSubtarsIndexMap(currentSubtar[i] - 1) !=
        UNDEFINED) {
      currentSubtar[i] =
        _outputGenerator->GetSubtarsIndexMap(currentSubtar[i] - 1) + 1;
    }

    while (true) {
      filterSubtar[i] = _filterGenerator->GetSubtar(currentSubtar[i]);
      if (filterSubtar[i] == nullptr)
        break;

      filterDs[i] = filterSubtar[i]->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
      if (filterDs[i]->BitMask()->any_parallel(_numThreads, _workPerThread))
        break;

      _filterGenerator->TestAndDisposeSubtar(currentSubtar[i]);
      currentSubtar[i]++;
    }

    subtar[i] = _generator->GetSubtar(currentSubtar[i]);
    _outputGenerator->SetSubtarsIndexMap(subtarIndex + i, currentSubtar[i]);
  }

  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      SubtarPtr newSubtar = std::make_shared<Subtar>();

      if (subtar[i] == nullptr || filterDs[i] == nullptr)
        break;

      newSubtar->SetTAR(_outputTAR);

      if (!filterDs[i]->BitMask()->all_parallel(_numThreads, _workPerThread)) {

        applyFilter(subtar[i], newSubtar, filterDs[i], _storageManager);

      } else {
        for (auto entry : subtar[i]->GetDataSets()) {
          if (_outputTAR->GetDataElement(entry.first) != nullptr)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        for (auto entry : subtar[i]->GetDimSpecs()) {
          newSubtar->AddDimensionsSpecification(entry.second);
        }
      }

      _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      _generator->TestAndDisposeSubtar(currentSubtar[i]);
      _filterGenerator->TestAndDisposeSubtar(currentSubtar[i]);

    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}