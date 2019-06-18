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
#include <boost/regex.hpp>
#include "include/ddl_operators.h"

//using namespace boost;

CreateDataset::CreateDataset(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult CreateDataset::Run() {
  regex range("[0-9]+:[0-9]+:[0-9]+:[0-9]+");

  try {
    ParameterPtr parameter = _operation->GetParametersByName(COMMAND);
    ParameterPtr parameter2 = _operation->GetParametersByName(OPERAND(0));
    std::string commandString = parameter->literal_str;
    std::string inBetween = trim_delimiters(commandString);
    std::vector<std::string> arguments = split(inBetween, _COLON[0]);

    if (arguments.size() == 2 || arguments.size() == 3) {
      std::string dsName = trim(arguments[0]);
      std::string type = trim(arguments[1]);
      std::string ssize = "1";
      int64_t size = 1;

      if (arguments.size() == 3) {
        ssize = trim(arguments[2]);
        size = strtol(ssize.c_str(), nullptr, 10);
      }

      if (size == 0)
        throw std::runtime_error("Invalid vector type length: " + ssize + ".");

      std::string file, filler = parameter2->literal_str;
      bool fileFiller = false;
      filler = trim_delimiters(filler);
      file = filler;

      if (!_metadataManager->ValidateIdentifier(dsName, "dataset"))
        throw std::runtime_error("Invalid dataset name: " + dsName + ".");

      if (_metadataManager->GetDataSetByName(dsName) != nullptr)
        throw std::runtime_error("Dataset " + dsName + " already exists.");

      DataType dsType = DataType(STR2TYPE(type.c_str()), size);
      if (dsType == NO_TYPE)
        throw std::runtime_error("Invalid type: " + type + ".");

      int typeSize = TYPE_SIZE(dsType);
      DatasetPtr ds;

      if (!_queryDataManager->GetParamsList().empty()) {
        file = _queryDataManager->GetParamFilePath(file);
        fileFiller = true;
      } else if (EXIST_FILE(file)) {
        std::string dir = _configurationManager->GetStringValue(SEC_STORAGE_DIR);

        if (file.compare(0, dir.length(), dir) != 0) {
          // To-do move to storage dir in case it is not.
          throw std::runtime_error("File is not in SAVIME storage dir: " + dir +
                                   ".");
        }
        fileFiller = true;
      }

      if (!fileFiller) {
        filler = trim(filler);
        if (regex_match(filler, range)) {

          filler = trim_delimiters(filler);
          auto range = split(filler, _COLON[0]);
          double dRanges[4];

          if (range.size() != 4)
            throw std::runtime_error("Invalid range specification. It must be "
                                     "defined as initial:spacing:final:rep.");

          for (int i = 0; i < range.size(); i++) {
            try {
              dRanges[i] = stod(range[i]);
            } catch (std::invalid_argument &e) {
              throw std::runtime_error("Invalid range specification. Could not "
                                       "parse numerical values.");
            } catch (std::out_of_range &e) {
              throw std::runtime_error("Invalid range specification. Numerical "
                                       "value is out of range.");
            }
          }

          if (dRanges[0] > dRanges[2])
            throw std::runtime_error("Invalid range specification. Initial "
                                     "value must be lower than the final "
                                     "value.");

          if (dRanges[3] < 1)
            throw std::runtime_error("Number of repetitions must be greater or "
                                     "equal to 1.");


          SET_SINGLE_THREAD_MULTIPLE_SUBTARS(_configurationManager);
          _storageManager->SetUseSecStorage(true);
          ds = _storageManager->Create(dsType, dRanges[0], dRanges[1],
                                      dRanges[2], dRanges[3]);
          _storageManager->SetUseSecStorage(false);
          UNSET_SINGLE_THREAD_MULTIPLE_SUBTARS(_configurationManager);

          if (ds == nullptr)
            throw std::runtime_error("Invalid range specification");

          ds->GetId() = UNSAVED_ID;
          ds->GetName() = dsName;
          ds->Sorted() = true;

        } else if (filler.find(LITERAL_FILLER_MARK) != std::string::npos) {
          int32_t paramCounter = 1;
          vector<string> literals;

          while (true) {
            auto param =
              _operation->GetParametersByName(OPERAND(paramCounter++));
            if (param == nullptr)
              break;
            literals.push_back(param->literal_str);
          }

          if (dsType != CHAR && literals.size() % dsType.vectorLength() != 0)
            throw std::runtime_error("Number of elements in the literal"
                                     " dataset must"
                                     " be a multiple of the type vector"
                                     " length.");

          _storageManager->SetUseSecStorage(true);
          ds = _storageManager->Create(dsType, literals);
          _storageManager->SetUseSecStorage(false);

          if (ds == nullptr) {
            throw std::runtime_error(
              "Could not create dataset " + dsName + ".");
          }

          ds->GetId() = UNSAVED_ID;
          ds->GetName() = dsName;
        } else {
          throw std::runtime_error("File does not exist: " + file + ".");
        }
      } else {
        int64_t fileSize = FILE_SIZE(file);
        int64_t entryCount = fileSize / dsType.getElementSize();

        if (entryCount % dsType.vectorLength() != 0)
          throw std::runtime_error("Number of elements in dataset must"
                                   " be a multiple of the type vector "
                                   " length.");

        ds = make_shared<Dataset>(UNSAVED_ID, dsName, file, dsType);
        ds->Sorted() = false;

        if (_storageManager->Save(ds) == SAVIME_FAILURE) {
          throw std::runtime_error("Could not save dataset. "
                                   "Not enough space left. Consider "
                                   "increasing the max storage size.");
        }
      }

      TARSPtr defaultTARS = _metadataManager->GetTARS(
        _configurationManager->GetIntValue(DEFAULT_TARS));

      if (_metadataManager->SaveDataSet(defaultTARS, ds) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save dataset.");
      }
    } else {
      throw std::runtime_error("Invalid dataset definition.");
    }
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}