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
#include "include/dml_operators.h"

Select::Select(OperationPtr operation, ConfigurationManagerPtr configurationManager, 
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, 
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
}

SavimeResult Select::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()
  
  SubtarPtr subtar[_numSubtars];
  SubTARIndex subtarIndexes[_numSubtars];
  int64_t offset, stride[_numSubtars];
  int64_t lowerBoundinI[_numSubtars];
  int64_t upperBoundinI[_numSubtars];

  for (int32_t i = 0; i < _numSubtars; i++) {
    subtar[i] = _generator->GetSubtar(subtarIndex + i);
    subtarIndexes[i] = subtarIndex + i;
  }

  for (int32_t i = 0; i < _numSubtars; i++) {

    if (subtar[i] == nullptr)
      break;

    if (subtarIndexes[i] > 0)
      offset = _outputGenerator->GetSubtarsIndexMap(subtarIndexes[i] - 1);
    else
      offset = 0;

    lowerBoundinI[i] = offset;
    upperBoundinI[i] = offset + subtar[i]->GetFilledLength() - 1;
    _outputGenerator->SetSubtarsIndexMap(subtarIndexes[i], upperBoundinI[i] + 1);
    stride[i] = upperBoundinI[i] - lowerBoundinI[i] + 1;
  }

  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      if (subtar[i] == nullptr)
        break;

      SubtarPtr newSubtar = std::make_shared<Subtar>();

      if (_outputTAR->GetDimensions().size() == 1 &&
          _outputTAR->GetDimensions().front()->GetName() == DEFAULT_SYNTHETIC_DIMENSION) {

        for (auto entry : subtar[i]->GetDataSets()) {
          if (_outputTAR->GetDataElement(entry.first) != nullptr
              && entry.first != DEFAULT_SYNTHETIC_DIMENSION)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        auto dimension = _outputTAR->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION)
          ->GetDimension();
        DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
          UNSAVED_ID, dimension, lowerBoundinI[i], upperBoundinI[i], stride[i],
          (savime_size_t)1);
        newSubtar->AddDimensionsSpecification(newDimSpecs);

        savime_size_t totalLength = subtar[i]->GetFilledLength();

        for (auto entry : subtar[i]->GetDimSpecs()) {
          if (_outputTAR->GetDataElement(entry.first) != nullptr
              && entry.first != DEFAULT_SYNTHETIC_DIMENSION) {
            auto dimSpec = entry.second;
            DatasetPtr dataset;
            if (_storageManager->MaterializeDim(dimSpec, totalLength, dataset) !=
                SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("MaterializeDim", "SELECT"));
            newSubtar->AddDataSet(dimSpec->GetDimension()->GetName(), dataset);
          }
        }
      } else {
        for (auto entry : subtar[i]->GetDataSets()) {
          if (_outputTAR->GetDataElement(entry.first) != nullptr)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        for (auto entry : subtar[i]->GetDimSpecs()) {
          newSubtar->AddDimensionsSpecification(entry.second);
        }
      }

      _outputGenerator->AddSubtar(subtarIndexes[i], newSubtar);
      _generator->TestAndDisposeSubtar(subtarIndexes[i]);
    CATCH()
  }

  RETHROW()

  return SAVIME_SUCCESS;
}