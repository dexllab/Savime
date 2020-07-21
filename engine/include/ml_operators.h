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
*    ANDERSON C. SILVA				JUNE 2020
*/


#ifndef SAVIME_ML_OPERATORS_H
#define SAVIME_ML_OPERATORS_H

#include "../core/include/engine.h"

class AssignLearningTAR : public EngineOperator {
public:
    AssignLearningTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                      QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
                      EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
//private:
    //unordered_map<string, unordered_map<string, string>> ParseModelsFile(string filePath);
};


#endif //SAVIME_ML_OPERATORS_H
