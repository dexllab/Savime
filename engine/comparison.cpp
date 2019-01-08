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

Comparison::Comparison(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                 QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                 StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  _comparisonOperation = operation->GetParametersByName(OP);

  auto params = operation->GetParameters();
  _operand2 = params.back();
  params.pop_back();
  _operand1 = params.back();

  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());

}

SavimeResult Comparison::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

  //auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
  //    ->GetGenerators()[outputTAR->GetName()];

  SET_SUBTARS_THREADS(_numSubtars);

  SubtarPtr subtars[_numSubtars];
  for(int32_t i =0; i < _numSubtars; i++) {
    subtars[i] = _generator->GetSubtar(subtarIndex + i);
  }

#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()

      auto subtar = subtars[i];
      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = std::make_shared<Subtar>();
      DatasetPtr newDataset;

      newSubtar->SetTAR(_outputTAR);

      int64_t totalLength = subtar->GetFilledLength();

      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      // for (auto entry : subtar->GetDataSets()) {
      //  newSubtar->AddDataSet(entry.first, entry.second);
      //}

      DatasetPtr filterDataset;
      if (_operand1->type == LITERAL_DOUBLE_PARAM &&
          _operand2->type == LITERAL_DOUBLE_PARAM) {
        throw std::runtime_error("Constant comparators not supported.");
      } else if (_operand1->type == LITERAL_STRING_PARAM &&
                 _operand2->type == LITERAL_STRING_PARAM) {
        throw std::runtime_error("String constant comparators not supported.");

      } else if (_operand1->type == IDENTIFIER_PARAM &&
                 (_operand2->type == LITERAL_DOUBLE_PARAM || _operand2->type == LITERAL_STRING_PARAM)) {

        Literal literalOperand2;
        if(_operand2->type == LITERAL_DOUBLE_PARAM)
          literalOperand2 = _operand2->literal_dbl;
        else
          literalOperand2 = trim_delimiters(_operand2->literal_str);

        if (_inputTAR->GetDataElement(_operand1->literal_str)->GetType() ==
            DIMENSION_SCHEMA_ELEMENT) {

          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(_operand1->literal_str);
          if (_storageManager->ComparisonDim(
            _comparisonOperation->literal_str, dimSpecs, totalLength,
            literalOperand2, filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        } else {
          auto dataset = subtar->GetDataSetFor(_operand1->literal_str);
          if (_storageManager->Comparison(_comparisonOperation->literal_str,
                                         dataset, literalOperand2,
                                         filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        }

      } else if ((_operand1->type == LITERAL_DOUBLE_PARAM || _operand1->type == LITERAL_STRING_PARAM) &&
                 _operand2->type == IDENTIFIER_PARAM) {


        Literal literalOperand1;
        if(_operand1->type == LITERAL_DOUBLE_PARAM)
          literalOperand1 = _operand1->literal_dbl;
        else
          literalOperand1 = trim_delimiters(_operand1->literal_str);

        if (_inputTAR->GetDataElement(_operand2->literal_str)->GetType() ==
            DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(_operand2->literal_str);
          if (_storageManager->ComparisonDim(
            _comparisonOperation->literal_str, dimSpecs, totalLength,
            literalOperand1, filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        } else {
          auto dataset = subtar->GetDataSetFor(_operand2->literal_str);
          if (_storageManager->Comparison(_comparisonOperation->literal_str,
                                         dataset, literalOperand1,
                                         filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        }
      }  else if (_operand1->type == IDENTIFIER_PARAM &&
                  _operand2->type == IDENTIFIER_PARAM) {
        DatasetPtr dsOperand1, dsOperand2;

        if (_inputTAR->GetDataElement(_operand1->literal_str)->GetType() ==
            DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(_operand1->literal_str);
          if (_storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand1) !=
              SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
        } else {
          dsOperand1 = subtar->GetDataSetFor(_operand1->literal_str);
        }

        if (_inputTAR->GetDataElement(_operand2->literal_str)->GetType() ==
            DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(_operand2->literal_str);
          if (_storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand2) !=
              SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
        } else {
          dsOperand2 = subtar->GetDataSetFor(_operand2->literal_str);
        }

        if (_storageManager->Comparison(_comparisonOperation->literal_str,
                                       dsOperand1, dsOperand2,
                                       filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Comparison", "COMPARISON"));
      }

      filterDataset->BitMask()->resize(totalLength);
      newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);

      _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      _generator->TestAndDisposeSubtar(subtarIndex + i);

    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}