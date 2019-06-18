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

Split::Split(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                     QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                     StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  //SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  _numSubtars = 1; //Set to 1 for split;
  _idealSize = operation->GetParametersByName(PARAM(TAL_SPLIT, _OPERAND))->literal_lng;
}

SavimeResult Split::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

  while (true) {
    TRY()

      SubtarPtr subtar;
      SubTARIndex lastSubtar = 0, currentSubtar = 0;

      currentSubtar = _outputGenerator->GetSubtarsIndexMap(subtarIndex - 1) + 1;

      subtar = _generator->GetSubtar(currentSubtar);

      if (subtar == nullptr)
        return SAVIME_SUCCESS;
      int64_t totalLength = subtar->GetFilledLength();

      if (subtar == nullptr)
        break;

      DimSpecPtr splitDimension = nullptr;
      int64_t greatestAdj = 0;
      bool hasTotalDimension = false;

      for (auto entry : subtar->GetDimSpecs()) {
        DimSpecPtr dimSpecs = entry.second;
        if (dimSpecs->GetFilledLength() > 1) {
          if (dimSpecs->GetAdjacency() > greatestAdj) {
            splitDimension = dimSpecs;
            greatestAdj = dimSpecs->GetAdjacency();
          }
        }
        if (dimSpecs->GetSpecsType() == TOTAL)
          hasTotalDimension = true;
      }

      if (splitDimension != nullptr) {

        int64_t maxSplitLength = _configurationManager->GetLongValue(MAX_SPLIT_LEN);

        if (splitDimension->GetFilledLength() > maxSplitLength)
          throw std::runtime_error(
            "Major dimension is too large for split operation. Consider"
            "loading smaller subtars.");

        int64_t subtarsNo = 1;
        int64_t paratitionLen = 1;
        int64_t splitDimLen = splitDimension->GetFilledLength();
        int64_t idealPartitionNumber = totalLength/_idealSize;


        if (!hasTotalDimension && idealPartitionNumber >= 2) {

          if(splitDimLen >= idealPartitionNumber) {
            int64_t bestDivisor = find_closest_divisor(splitDimLen, idealPartitionNumber);
            subtarsNo = bestDivisor;
            paratitionLen = splitDimLen/bestDivisor;

          } else {
            subtarsNo = splitDimLen;
            paratitionLen = 1;
          }

          vector<SubtarPtr> splitSubtars;
          map<string, vector<DatasetPtr>> datasets;

          for (auto entry : subtar->GetDataSets()) {
            vector<DatasetPtr> _datasets;
            if (_storageManager->Split(entry.second, totalLength, subtarsNo, _datasets) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("split", "SPLIT"));
            datasets[entry.first] = _datasets;
          }

          auto newStride = (splitDimension->GetStride()/splitDimension->GetAdjacency())*paratitionLen;
          auto dimension = _outputTAR->GetDataElement(splitDimension->GetDimension()->GetName())->GetDimension();

          if (splitDimension->GetSpecsType() == ORDERED) {

            for (int64_t index = splitDimension->GetLowerBound();
                 index <= splitDimension->GetUpperBound(); index+=paratitionLen) {

              DimSpecPtr dimSpecs;
              SubtarPtr newSubtar = make_shared<Subtar>();

              dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                             dimension,
                                                             index, index+paratitionLen-1,
                                                             newStride,
                                                             splitDimension->GetAdjacency());

              newSubtar->AddDimensionsSpecification(dimSpecs);

              for (auto entry : subtar->GetDimSpecs()) {

                if (splitDimension->GetDimension()->GetName() != entry.first) {

                  DimSpecPtr dimSpecs; int64_t adjacency;
                  auto dimension = _outputTAR->GetDataElement(entry.second->GetDimension()->GetName())->GetDimension();

                  if (entry.second->GetFilledLength() == 1 &&
                            entry.second->GetAdjacency() > splitDimension->GetAdjacency()) {
                    adjacency = (entry.second->GetAdjacency() / splitDimension->GetFilledLength())*paratitionLen;
                  } else {
                    adjacency = entry.second->GetAdjacency();
                  }

                  if(entry.second->GetSpecsType() == ORDERED){
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound(),
                                                                   entry.second->GetStride(),
                                                                   adjacency);

                  } else if (entry.second->GetSpecsType() == PARTIAL) {
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetDataset(),
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound(),
                                                                   entry.second->GetStride(),
                                                                   adjacency);
                  } else {
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetDataset(),
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound());
                  }

                  newSubtar->AddDimensionsSpecification(dimSpecs);

                }
              }

              for (auto entry : subtar->GetDataSets()) {
                DatasetPtr ds = datasets[entry.first][(index - splitDimension->GetLowerBound())/paratitionLen];
                newSubtar->AddDataSet(entry.first, ds);
              }

              splitSubtars.push_back(newSubtar);
            }

          } else {

            vector<DatasetPtr> _partialDimDatasets;
            if (_storageManager->Split(splitDimension->GetDataset(), totalLength, subtarsNo,
              _partialDimDatasets) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("split", "SPLIT"));

            for (int64_t index = splitDimension->GetLowerBound();
                            index <= splitDimension->GetUpperBound(); index+=paratitionLen) {

              DimSpecPtr dimSpecs;
              SubtarPtr newSubtar = make_shared<Subtar>();
              auto newStride = (splitDimension->GetStride()/splitDimension->GetAdjacency())*paratitionLen;

              dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                             dimension,
                                                             _partialDimDatasets[index],
                                                             index, index+paratitionLen-1,
                                                             newStride,
                                                             splitDimension->GetAdjacency());
              newSubtar->AddDimensionsSpecification(dimSpecs);


              for (auto entry : subtar->GetDimSpecs()) {

                if (splitDimension->GetDimension()->GetName() != entry.first) {

                  DimSpecPtr dimSpecs; int64_t adjacency;
                  auto dimension = _outputTAR->GetDataElement(entry.second->GetDimension()->GetName())->GetDimension();

                  if (entry.second->GetFilledLength() == 1 &&
                      entry.second->GetAdjacency() > splitDimension->GetAdjacency()) {
                    adjacency = entry.second->GetAdjacency() / splitDimension->GetFilledLength();
                  } else {
                    adjacency = entry.second->GetAdjacency();
                  }

                  if(entry.second->GetSpecsType() == ORDERED){
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound(),
                                                                   entry.second->GetStride(),
                                                                   adjacency);

                  } else if (entry.second->GetSpecsType() == PARTIAL) {
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetDataset(),
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound(),
                                                                   entry.second->GetStride(),
                                                                   adjacency);
                  } else {
                    dimSpecs = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                                   dimension,
                                                                   entry.second->GetDataset(),
                                                                   entry.second->GetLowerBound(),
                                                                   entry.second->GetUpperBound());
                  }

                  newSubtar->AddDimensionsSpecification(dimSpecs);

                }
              }

              for (auto entry : subtar->GetDataSets()) {
                DatasetPtr ds = datasets[entry.first][(index - splitDimension->GetLowerBound())/paratitionLen];
                newSubtar->AddDataSet(entry.first, ds);
              }
              splitSubtars.push_back(newSubtar);
            }
          }

          for (auto s : splitSubtars) {
            _outputGenerator->AddSubtar(subtarIndex++, s);
          }
          subtarIndex--;

        } else {
          _outputGenerator->AddSubtar(subtarIndex, subtar);
        }

      } else {
        _outputGenerator->AddSubtar(subtarIndex, subtar);
      }


      _generator->TestAndDisposeSubtar(subtarIndex);
      _outputGenerator->SetSubtarsIndexMap(subtarIndex, currentSubtar);
      subtarIndex++;

    CATCH()
  }
  RETHROW()


  return SAVIME_SUCCESS;
}
