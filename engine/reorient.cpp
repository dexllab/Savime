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
#include <memory>
#include "include/dml_operators.h"


Reorient::Reorient(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  SET_LONG_CONFIG_VAL(_maxPartitionSize, REORIENT_PARTITION_SIZE);

  auto newMajorIdentifier = operation->GetParametersByName(PARAM(TAL_REORIENT, _OPERAND))->literal_str;
  _newMajorDimension = _inputTAR->GetDataElement(newMajorIdentifier)->GetDimension();
}

SavimeResult Reorient::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()


  _numSubtars = 1;

  SubtarPtr subtars[_numSubtars];


  for (int32_t i = 0; i < _numSubtars; i++) {
    subtars[i] = nullptr;
  }

  for (int32_t i = 0; i < _numSubtars; i++) {
    subtars[i] = _generator->GetSubtar(subtarIndex + i);
    if(subtars[i] == nullptr)
      break;
  }

  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()

      auto subtar = subtars[i];
      auto newSubtar = make_shared<Subtar>();

      if (subtar == nullptr)
        break;

      auto totalLength =  subtar->GetFilledLength();
      vector<DimSpecPtr> dimSpecs; int32_t newMajorIdx = 0, newMajorCount = 0;
      bool isAllOrdered = true;

      vector<DimSpecPtr> sortedSpecs;
      for(const auto& entry: subtar->GetDimSpecs()){
        sortedSpecs.push_back(entry.second->Clone());
      }
      std::sort(sortedSpecs.begin(), sortedSpecs.end(), compareAdj);


      for(const auto& specs: sortedSpecs){

        if(specs->GetSpecsType() != ORDERED){
          isAllOrdered = false;
          break;
        }

        if(specs->GetDimension()->GetName() == _newMajorDimension->GetName())
          newMajorIdx = newMajorCount;
        newMajorCount++;
      }

      if(isAllOrdered){

        auto majorSpecs = sortedSpecs[newMajorIdx];
        auto multiple = find_closest_multiple(majorSpecs->GetStride(), _maxPartitionSize);
        auto partitionSize = multiple*majorSpecs->GetStride();
        partitionSize = partitionSize > totalLength ? totalLength : partitionSize;

        for(const auto& entry :  subtar->GetDataSets()){
          DatasetPtr reorientedDataset = entry.second;
          DatasetPtr destinyDataset;

          if(newMajorIdx != 0){
            _storageManager->Reorient(reorientedDataset, sortedSpecs, totalLength,
                                      newMajorIdx, partitionSize, destinyDataset);
          } else {
            destinyDataset = reorientedDataset;
          }

          newSubtar->AddDataSet(entry.first, destinyDataset);
        }

        newMajorCount = 0;
        sortedSpecs.erase(sortedSpecs.begin()+newMajorIdx);
        sortedSpecs.insert(sortedSpecs.begin(), majorSpecs);
        ADJUST_SPECS(sortedSpecs)

        for(const auto& specs: sortedSpecs){
          newSubtar->AddDimensionsSpecification(specs);
        }

      } else {
        newSubtar = subtar;
      }

      _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      _generator->TestAndDisposeSubtar(subtarIndex + i);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}