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

Arithmetic::Arithmetic(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
  _newMember = operation->GetParametersByName(NEW_MEMBER);
  _op = operation->GetParametersByName(OP);
}

SavimeResult Arithmetic::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

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

      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = std::make_shared<Subtar>();
      DatasetPtr newDataset;

      newSubtar->SetTAR(_outputTAR);

      int64_t totalLength = subtar->GetFilledLength();

      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      for (auto entry : subtar->GetDataSets()) {
        if (_outputTAR->HasDataElement(entry.first))
          newSubtar->AddDataSet(entry.first, entry.second);
      }

      ParameterPtr operand0 = _operation->GetParametersByName(OPERAND(0));
      ParameterPtr operand1 = _operation->GetParametersByName(OPERAND(1));

      if (operand0->type == LITERAL_DOUBLE_PARAM) {
        DatasetPtr stretched;
        Literal literal;
        literal.Simplify(operand0->literal_dbl);
        DatasetPtr constantDs = _storageManager->Create(
          literal.type, operand0->literal_dbl, 1, operand0->literal_dbl, 1);

        if (constantDs == nullptr)
          throw std::runtime_error("Could not create dataset.");

        _storageManager->Stretch(constantDs, 1, totalLength, 1, stretched);

        if (operand1 == nullptr) {
          if (_storageManager->Apply(_op->literal_str, stretched, Literal(0.0),
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == LITERAL_DOUBLE_PARAM) {
          Literal literalOp1;
          literalOp1.Simplify(operand1->literal_dbl);

          if (_storageManager->Apply(_op->literal_str, stretched, literalOp1,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == IDENTIFIER_PARAM) {
          auto dataset = subtar->GetDataSetFor(operand1->literal_str);

          if (!dataset) {
            auto dimSpecs =
              subtar->GetDimensionSpecificationFor(operand1->literal_str);

            if (_storageManager->MaterializeDim(dimSpecs, totalLength, dataset) !=
                SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
          }

          if (_storageManager->Apply(_op->literal_str, dataset, literal,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        }
      }

      if (operand0->type == IDENTIFIER_PARAM) {
        auto dataset = subtar->GetDataSetFor(operand0->literal_str);

        if (!dataset) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand0->literal_str);

          if (_storageManager->MaterializeDim(dimSpecs, totalLength, dataset) !=
              SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        }

        if (operand1 == nullptr) {
          double operand = 0;

          if (_storageManager->Apply(_op->literal_str, dataset, operand,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == LITERAL_DOUBLE_PARAM) {
          Literal literal;
          literal.Simplify(operand1->literal_dbl);

          if (_storageManager->Apply(_op->literal_str, dataset, literal,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));

        } else if (operand1->type == IDENTIFIER_PARAM) {
          if(_op->literal_str == _EQ){
            newDataset = subtar->GetDataSetFor(operand0->literal_str);
          } else {

            auto dataset1 = subtar->GetDataSetFor(operand1->literal_str);
            if (!dataset1) {
              auto dimSpecs =
                subtar->GetDimensionSpecificationFor(operand1->literal_str);

              if (_storageManager->MaterializeDim(dimSpecs, totalLength, dataset1) !=
                  SAVIME_SUCCESS)
                throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
            }

            if (_storageManager->Apply(_op->literal_str, dataset, dataset1,
                                       newDataset) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
          }
        }
      }

      newSubtar->AddDataSet(_newMember->literal_str, newDataset);
      _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      _generator->TestAndDisposeSubtar(subtarIndex + i);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}