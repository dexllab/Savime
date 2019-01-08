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

CrossJoin::CrossJoin(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  _leftTAR = operation->GetParametersByName(OPERAND(0))->tar;
  _rightTAR = operation->GetParametersByName(OPERAND(1))->tar;
  assert(_leftTAR != nullptr && _rightTAR != nullptr);
  _outputTAR = operation->GetResultingTAR();
  _numSubtars = configurationManager->GetIntValue(MAX_PARA_SUBTARS);

  _freeBufferedSubtars = configurationManager->GetBooleanValue(FREE_BUFFERED_SUBTARS);

  SET_GENERATOR(_leftGenerator, _leftTAR->GetName());
  SET_GENERATOR(_rightGenerator, _rightTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
}

SavimeResult CrossJoin::GenerateSubtar(SubTARIndex subtarIndex) {

  int32_t leftSubtarIndexes[_numSubtars], rightSubtarIndexes[_numSubtars];
  SubtarPtr leftSubtars[_numSubtars], rightSubtars[_numSubtars];

  for (int32_t i = 0; i < _numSubtars; i++) {
    rightSubtarIndexes[i] =
      _outputGenerator->GetSubtarsIndexMap(1, subtarIndex + i - 1) + 1;
    rightSubtars[i] = _rightGenerator->GetSubtar(rightSubtarIndexes[i]);

    if (rightSubtars[i] == nullptr) {
      leftSubtarIndexes[i] =
        _outputGenerator->GetSubtarsIndexMap(0, subtarIndex + i - 1) + 1;
      _leftGenerator->TestAndDisposeSubtar(leftSubtarIndexes[i] - 1);
      rightSubtarIndexes[i] = 0;
      rightSubtars[i] = _rightGenerator->GetSubtar(rightSubtarIndexes[i]);
    } else if (subtarIndex + i > 0) {
      leftSubtarIndexes[i] =
        _outputGenerator->GetSubtarsIndexMap(0, subtarIndex + i - 1);
    } else {
      leftSubtarIndexes[i] = 0;
    }

    leftSubtars[i] = _leftGenerator->GetSubtar(leftSubtarIndexes[i]);
    _outputGenerator->SetSubtarsIndexMap(0, subtarIndex + i, leftSubtarIndexes[i]);
    _outputGenerator->SetSubtarsIndexMap(1, subtarIndex + i, rightSubtarIndexes[i]);
  }

  OMP_EXCEPTION_ENV()
  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
      int32_t leftSubtarIndex = leftSubtarIndexes[i];
      int32_t rightSubtarIndex = rightSubtarIndexes[i];
      SubtarPtr rightSubtar = rightSubtars[i], leftSubtar = leftSubtars[i];

      if (leftSubtar == nullptr || rightSubtar == nullptr) {
        int32_t lastSubtar =
          _outputGenerator->GetSubtarsIndexMap(1, subtarIndex + i - 2);
        if (!_freeBufferedSubtars) {
          for (int32_t i = 0; i <= lastSubtar; i++) {
            _rightGenerator->TestAndDisposeSubtar(i);
          }
        }
        break;
      }

      bool hasTotalDims = false;
      for (auto entry : leftSubtar->GetDimSpecs()) {
        if(entry.second->GetSpecsType() == TOTAL){
          hasTotalDims = true;
          break;
        }
      }

      if(!hasTotalDims)
        for (auto entry : rightSubtar->GetDimSpecs()) {
          if(entry.second->GetSpecsType() == TOTAL){
            hasTotalDims = true;
            break;
          }
        }

      int64_t leftSubtarLen = leftSubtar->GetFilledLength();
      int64_t rightSubtarLen = rightSubtar->GetFilledLength();

      for (auto entry : leftSubtar->GetDimSpecs()) {
        string dimName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec;

        if (dimspec->GetSpecsType() == ORDERED) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetLowerBound(), dimspec->GetUpperBound(),
            dimspec->GetStride() * rightSubtarLen,
            dimspec->GetAdjacency() * rightSubtarLen);

        } else if (dimspec->GetSpecsType() == PARTIAL) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetDataset(), dimspec->GetLowerBound(),
            dimspec->GetUpperBound(), dimspec->GetStride() * rightSubtarLen,
            dimspec->GetAdjacency() * rightSubtarLen);
        } else {
          DatasetPtr matDim;
          if (_storageManager->Stretch(dimspec->GetDataset(), leftSubtarLen,
                                      rightSubtarLen, 1,
                                      matDim) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            matDim, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }

        if(dimspec->GetSpecsType() != TOTAL && hasTotalDims){
          DatasetPtr materialized;
          _storageManager->MaterializeDim(newDimspec,
                                         leftSubtarLen*rightSubtarLen,
                                         materialized);

          if(dimspec->GetDimension()->GetDimensionType() == EXPLICIT){
            _storageManager->Logical2Real(newDimspec->GetDimension(),
                                         newDimspec,
                                         materialized,
                                         materialized);
          }

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            materialized, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }


        newSubtar->AddDimensionsSpecification(newDimspec);
      }

      for (auto entry : rightSubtar->GetDimSpecs()) {
        string dimName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec;

        if (dimspec->GetSpecsType() == ORDERED) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetLowerBound(), dimspec->GetUpperBound(),
            dimspec->GetStride(), dimspec->GetAdjacency());

        } else if (dimspec->GetSpecsType() == PARTIAL) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetDataset(), dimspec->GetLowerBound(),
            dimspec->GetUpperBound(), dimspec->GetStride(),
            dimspec->GetAdjacency());
        } else {
          DatasetPtr matDim;
          if (_storageManager->Stretch(dimspec->GetDataset(), rightSubtarLen, 1,
                                      leftSubtarLen, matDim) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            matDim, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }

        if(dimspec->GetSpecsType() != TOTAL && hasTotalDims){
          DatasetPtr materialized;
          _storageManager->MaterializeDim(newDimspec,
                                         leftSubtarLen*rightSubtarLen,
                                         materialized);

          if(dimspec->GetDimension()->GetDimensionType() == EXPLICIT){
            _storageManager->Logical2Real(newDimspec->GetDimension(),
                                         newDimspec,
                                         materialized,
                                         materialized);
          }

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, _outputTAR->GetDataElement(dimName)->GetDimension(),
            materialized, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }

        newSubtar->AddDimensionsSpecification(newDimspec);
      }

      for (auto entry : leftSubtar->GetDataSets()) {
        string attName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (_storageManager->Stretch(ds, leftSubtarLen, rightSubtarLen, 1,
                                    joinedDs) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

        newSubtar->AddDataSet(attName, joinedDs);
      }

      for (auto entry : rightSubtar->GetDataSets()) {
        string attName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (_storageManager->Stretch(ds, rightSubtarLen, 1, leftSubtarLen,
                                    joinedDs) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

        newSubtar->AddDataSet(attName, joinedDs);
      }

      if (_freeBufferedSubtars) {
        _rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
      }

      _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      //outputGenerator->SetSubtarsIndexMap(0, subtarIndex + i, leftSubtarIndex);
      //outputGenerator->SetSubtarsIndexMap(1, subtarIndex + i, rightSubtarIndex);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}