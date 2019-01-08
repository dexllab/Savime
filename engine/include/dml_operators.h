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
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef DML_OPERATORS_H
#define DML_OPERATORS_H

#include <unordered_map>
#include "../core/include/engine.h"
#include "../core/include/util.h"
#include "../core/include/query_data_manager.h"
#include "../core/../core/include/storage_manager.h"
#include "default_engine.h"
#include "subset.h"

#define ERROR_MSG(F, O)                                                        \
  "Error during " + std::string(F) + " execution in " + std::string(O) +       \
      " operator. Check the log file for more info."

#define FIRST_SUBTAR 0

#define SET_TARS(INPUT, OUTPUT)\
    ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);\
    INPUT = inputTarParam->tar;\
    assert(INPUT != nullptr);\
    OUTPUT = operation->GetResultingTAR();\
    assert(OUTPUT != nullptr);

#define SET_GENERATOR(GENERATOR, TAR_NAME)\
        GENERATOR = (std::dynamic_pointer_cast<DefaultEngine>(engine))\
                         ->GetGenerators()[TAR_NAME]

#define SET_INT_CONFIG_VAL(VAR, KEY)\
   VAR = configurationManager->GetIntValue(KEY)

#define SET_BOOL_CONFIG_VAL(VAR, KEY)\
   VAR = configurationManager->GetBooleanValue(KEY)

#define SET_SUBTARS_THREADS(NUM)\
    omp_set_num_threads(NUM);\
    omp_set_nested(true);

#define SUB_THREADS_FIRST()\
    omp_get_thread_num()

#define SUB_THREADS_LAST()\
    omp_get_thread_num()


typedef std::mutex DMLMutex;
typedef std::unordered_map<int32_t, SubtarPtr> SubtarMap;
typedef std::vector<int32_t> SubtarIndexList;


/*----------------------------OPERATORS PROTOTYPES----------------------------*/
class Scan : public EngineOperator {

  TARPtr _inputTAR;
  TARPtr _outputTAR;
  TARGeneratorPtr _generator;

public:
  Scan(OperationPtr operation, ConfigurationManagerPtr configurationManager,
       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
       EnginePtr engine);

  SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
  SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Select : public EngineOperator {

    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _outputGenerator;

public :
    Select(OperationPtr operation, ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
         EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override{ return SAVIME_FAILURE; }

};

class Filter : public EngineOperator {

    int32_t _numThreads;
    int32_t _workPerThread;
    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _filterGenerator;
    TARGeneratorPtr _outputGenerator;

public:
    Filter(OperationPtr operation, ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Subset : public EngineOperator {

    int32_t _numThreads;
    int32_t _workPerThread;
    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _outputGenerator;
    FilteredDimMap _filteredDim;
    BoundMap _lower_bounds, _upper_bounds;

public :
    Subset(OperationPtr operation, ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Logical : public EngineOperator {

    int32_t _numThreads;
    int32_t _workPerThread;
    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _generatorOp1;
    TARGeneratorPtr _generatorOp2;
    ParameterPtr _logicalOperation;
    ParameterPtr _operand1;
    ParameterPtr _operand2;

public :
    Logical(OperationPtr operation, ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Comparison : public EngineOperator {

    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _outputGenerator;
    ParameterPtr _operand1;
    ParameterPtr _operand2;
    ParameterPtr _comparisonOperation;

public :
    Comparison(OperationPtr operation, ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
            EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Arithmetic : public EngineOperator {

    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _outputGenerator;
    ParameterPtr _newMember;
    ParameterPtr _op;

public :
    Arithmetic(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class CrossJoin : public EngineOperator {

    int32_t _numSubtars;
    TARPtr _leftTAR;
    TARPtr _rightTAR;
    TARPtr _outputTAR;
    bool _freeBufferedSubtars;
    TARGeneratorPtr _leftGenerator;
    TARGeneratorPtr _rightGenerator;
    TARGeneratorPtr _outputGenerator;

public :
    CrossJoin(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class DimJoin : public EngineOperator {

    int32_t _numThreads;
    int32_t _workPerThread;
    int32_t _numSubtars;
    TARPtr _leftTAR;
    TARPtr _rightTAR;
    TARPtr _outputTAR;
    bool _freeBufferedSubtars;
    TARGeneratorPtr _leftGenerator;
    TARGeneratorPtr _rightGenerator;
    TARGeneratorPtr _outputGenerator;

public :
    DimJoin(OperationPtr operation, ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
              EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};

class Aggregate : public EngineOperator {

    int32_t _numThreads;
    int32_t _workPerThread;
    int32_t _numSubtars;
    TARPtr _inputTAR;
    TARPtr _outputTAR;
    TARGeneratorPtr _generator;
    TARGeneratorPtr _outputGenerator;

public :
    Aggregate(OperationPtr operation, ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
            EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};



class UserDefined : public EngineOperator {

public :
    UserDefined(OperationPtr operation, ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
              EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override;
    SavimeResult Run() override { return SAVIME_FAILURE; }

};


#endif /* DML_OPERATORS_H */

