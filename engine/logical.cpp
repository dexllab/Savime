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

Logical::Logical(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  SET_GENERATOR(_generator, _inputTAR->GetName());

  _logicalOperation = operation->GetParametersByName(OP);
  _operand1 = nullptr;
  _operand2 = nullptr;

  auto params = operation->GetParameters();
  if (params.size() == 4) {
    _operand2 = params.back();
    params.pop_back();
    _operand1 = params.back();
  } else {
    _operand1 = operation->GetParameters().back();
  }

  TARGeneratorPtr generator, generatorOp1 = nullptr, generatorOp2 = nullptr;
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_generatorOp1, _operand1->tar->GetName());
  if (_operand2 != nullptr)
    SET_GENERATOR(_generatorOp2, _operand2->tar->GetName());

}

SavimeResult Logical::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

  SubtarPtr subtars[_numSubtars];
  SubtarPtr subtarsOp1[_numSubtars];
  SubtarPtr subtarsOp2[_numSubtars];
  for(int32_t i =0; i < _numSubtars; i++) {

    subtars[i] = _generator->GetSubtar(subtarIndex + i);
    subtarsOp1[i] = _generatorOp1->GetSubtar(subtarIndex + i);

    if (_generatorOp2 != nullptr) {
      subtarsOp2[i] = _generatorOp2->GetSubtar(subtarIndex + i);
    }
  }

  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()
      SubtarPtr subtar, subtarOp1, subtarOp2;
      subtar = subtars[i];
      subtarOp1 = subtarsOp1[i];
      subtarOp2 = subtarsOp2[i];

      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = std::make_shared<Subtar>();
      newSubtar->SetTAR(_outputTAR);

      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      DatasetPtr filterDataset;
      if (_logicalOperation->literal_str == _AND) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
        auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (_storageManager->And(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("And", "LOGICAL"));
      } else if (_logicalOperation->literal_str == _OR) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
        auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (_storageManager->Or(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Or", "LOGICAL"));
      } else if (_logicalOperation->literal_str == _NOT) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (_storageManager->Not(dsOp1, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Not", "LOGICAL"));
      }

      newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);

      auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(_engine))
        ->GetGenerators()[_outputTAR->GetName()];
      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      _generator->TestAndDisposeSubtar(subtarIndex + i);
      _generatorOp1->TestAndDisposeSubtar(subtarIndex + i);

      if (_generatorOp2 != nullptr)
        _generatorOp2->TestAndDisposeSubtar(subtarIndex + i);

    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}