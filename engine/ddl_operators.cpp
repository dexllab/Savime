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
#include "../core/include/query_data_manager.h"
#include "../core/include/storage_manager.h"
#include "../core/include/symbols.h"
#include "include/ddl_operators.h"
#include "include/dml_operators.h"
#include "include/create_tar.h"
#include "include/load_subtar.h"
#include <algorithm>
#include <vector>
#include <regex>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/*
 * CREATE_TARS("[tar_name]");
 */
int create_tars(SubTARIndex subtarIndex, OperationPtr operation,
                ConfigurationManagerPtr configurationManager,
                QueryDataManagerPtr queryDataManager,
                MetadataManagerPtr metadataManager,
                StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    ParameterPtr parameter = operation->GetParameters().front();
    std::string tarsName = parameter->literal_str;
    tarsName = trim_delimiters(tarsName);

    if (metadataManager->ValidateIdentifier(tarsName, "tars")) {
      TARSPtr newTars = TARSPtr(new TARS());
      newTars->id = UNSAVED_ID;
      newTars->name = tarsName;

      if (metadataManager->SaveTARS(newTars) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save new TARS: " + tarsName);
      }
    } else {
      throw std::runtime_error(
          "Invalid or already existing identifier for TARS: " + tarsName);
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
  return SAVIME_SUCCESS;
}

/* CREATE_TAR("tar_name", "type_name", "(dimensions_block)",
 * "(attributes_block)", ["(roles_block)"]);
 * dimensions_block: dim_block_implicit = dimtype, name, type, logical_lower,
 * logical_upper, spacing |
 *                   dim_block_explicit = dimtype, name, dataset |//Type and
 * size inferred by dataset in this case;
 *                                                               //Logical_upper
 * can be an "*", which means it is an unbounded dimensions.
 * attributes_block = name, type |
 * roles_block = data_element_name, role1, data_element2, role2, .... | // This
 * block is optional.
 */
int create_tar(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    std::map<std::string, RolePtr> roles;
    std::list<AttributePtr> atts;
    std::list<DimensionPtr> dims;
    TypePtr type = nullptr;

    std::list<ParameterPtr> parameters = operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string tarName = parameter->literal_str;
    tarName = trim_delimiters(tarName);
    parameters.pop_front();

    parameter = parameters.front();
    std::string typeName = parameter->literal_str;
    typeName = trim_delimiters(typeName);
    parameters.pop_front();

    parameter = parameters.front();
    std::string dimBlock = parameter->literal_str;
    dimBlock = trim_delimiters(dimBlock);
    parameters.pop_front();

    parameter = parameters.front();
    std::string attBlock = parameter->literal_str;
    attBlock = trim_delimiters(attBlock);
    parameters.pop_front();

    std::string rolesBlock;
    if (!parameters.empty()) {
      parameter = parameters.front();
      rolesBlock = parameter->literal_str;
      rolesBlock = trim_delimiters(rolesBlock);
    }

    if (!metadataManager->ValidateIdentifier(tarName, "tar"))
      throw std::runtime_error("Invalid identifier for TAR: " + tarName);

    TARSPtr defaultTARS = metadataManager->GetTARS(
        configurationManager->GetIntValue(DEFAULT_TARS));

    if (metadataManager->GetTARs(defaultTARS).size() >= MAX_NUM_TARS_IN_TARS)
      throw std::runtime_error("Too many TARs already defined.");

    if (metadataManager->GetTARByName(defaultTARS, tarName) != nullptr)
      throw std::runtime_error(tarName + " TAR already exists.");

    if (typeName.compare(_ASTERISK) != 0) {
      type = metadataManager->GetTypeByName(defaultTARS, typeName);
      if (type == nullptr)
        throw std::runtime_error("Undefined type: " + typeName);
    }

    dims = create_dimensions(dimBlock, metadataManager, storageManager);
    atts = create_attributes(attBlock);

    TARPtr newTar = TARPtr(new TAR());
    newTar->AlterTAR(UNSAVED_ID, tarName);
    newTar->AlterType(type);

    for (auto d : dims) {
      if (!metadataManager->ValidateIdentifier(d->GetName(), "dimension"))
        throw std::runtime_error("Invalid dimension name: " + d->GetName());

      if (newTar->GetDataElement(d->GetName()) != nullptr)
        throw std::runtime_error(
            "Duplicated data element name: " + d->GetName());

      newTar->AddDimension(d);
    }

    for (auto a : atts) {
      if (!metadataManager->ValidateIdentifier(a->GetName(), "attribute"))
        throw std::runtime_error("Invalid attribute name: " + a->GetName());

      if (newTar->GetDataElement(a->GetName()) != nullptr)
        throw std::runtime_error(
            "Duplicated data element name: " + a->GetName());

      newTar->AddAttribute(a);
    }

    if (type != nullptr) {
      roles = create_roles(newTar, type, rolesBlock);
    }

    for (auto r : roles) {
      if (newTar->GetDataElement(r.first) == nullptr)
        throw std::runtime_error(
            "Invalid role specification, undefined data element: " + r.first);

      newTar->SetRole(r.first, r.second);
    }

    if (newTar->GetAttributes().size() > MAX_NUM_OF_ATT_IN_TAR) {
      throw std::runtime_error("Too many attributes in " + tarName + ".");
    }

    if (newTar->GetDimensions().size() > MAX_NUM_OF_DIMS_IN_TAR) {
      throw std::runtime_error("Too many dimensions in " + tarName + ".");
    }

    if (metadataManager->SaveTAR(defaultTARS, newTar) == SAVIME_FAILURE)
      throw std::runtime_error("Could not save new TAR: " + tarName + ".");

    metadataManager->RegisterQuery(queryDataManager->GetQueryText());
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
}

/*
 * CREATE_TYPE("[type_name](mandatory_role1, mandatory_role2,
 * *non_mandatory_role3, ..., roleN)");
 * Role definitions starting with * are non-mandatory.
 */
int create_type(SubTARIndex subtarIndex, OperationPtr operation,
                ConfigurationManagerPtr configurationManager,
                QueryDataManagerPtr queryDataManager,
                MetadataManagerPtr metadataManager,
                StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    ParameterPtr parameter = operation->GetParameters().front();
    std::string commandString = parameter->literal_str;
    commandString = trim_delimiters(commandString);
    std::string newTypeName;

    newTypeName = commandString.substr(0, commandString.find(_LEFT_PAREN[0]));
    std::string
        betweenParenthesis = between(commandString, _LEFT_PAREN, _RIGHT_PAREN);
    std::vector<std::string> roles = split(betweenParenthesis, _COMMA[0]);

    if (betweenParenthesis.empty() || roles.empty()) {
      throw std::runtime_error("Invalid type string definition.");
    }

    if (metadataManager->ValidateIdentifier(newTypeName, "type")) {
      TARSPtr defaultTARS = metadataManager->GetTARS(
          configurationManager->GetIntValue(DEFAULT_TARS));

      if (metadataManager->GetTypes(defaultTARS).size()
          >= MAX_NUM_OF_TYPES_IN_TARS)
        throw std::runtime_error("Too many types already defined.");

      if (metadataManager->GetTypeByName(defaultTARS, newTypeName) != nullptr)
        throw std::runtime_error("Type " + newTypeName + " already exists.");

      TypePtr newType = TypePtr(new Type());
      newType->id = UNSAVED_ID;
      newType->name = newTypeName;
      std::map<std::string, std::string> roleMaps;

      for (auto role : roles) {
        std::string trimmedRole = trim(role);
        std::string trimmedRoleWithoutStar = trimmedRole;
        trimmedRoleWithoutStar.erase(std::remove(trimmedRoleWithoutStar.begin(),
                                                 trimmedRoleWithoutStar.end(),
                                                 '*'),
                                     trimmedRoleWithoutStar.end());

        if (roleMaps.find(trimmedRole) == roleMaps.end() &&
            metadataManager->ValidateIdentifier(trimmedRoleWithoutStar,
                                                "role")) {
          roleMaps[trimmedRole] = trimmedRoleWithoutStar;
        } else {
          throw std::runtime_error("Invalid or duplicate definition of role: " +
              role);
        }
      }

      for (auto entry : roleMaps) {
        RolePtr role = RolePtr(new Role());
        role->id = UNSAVED_ID;
        role->name = entry.second;
        role->is_mandatory = entry.first[0] != _TYPELESS_TAR_MARK;
        newType->roles[role->name] = role;
      }

      if (newType->roles.size() > MAX_NUM_OF_ROLES_IN_TYPE) {
        throw std::runtime_error("Too many roles defined in the type.");
      }

      if (metadataManager->SaveType(defaultTARS, newType) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save new Type: " + newTypeName);
      }

      metadataManager->RegisterQuery(queryDataManager->GetQueryText());

    } else {
      throw std::runtime_error(
          "Invalid or already existing identifier for Type: " + newTypeName);
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
}

/*
 * CREATE_DATASET("name:type", "data_file_path");
 * //Local file paths must starts with @ and remote server files don't.
 * CREATE_DATASET("name:type", "literal dataset");
 * //Literal dataset is a series of comma separated values between squared
 * brackets.
 * CREATE_DATASET("name:type", "initial:spacing:final");
 * //Datasets can be filled with a sequential interval of numerical values
 * specified by an initial and a final values along with a spacing.
 */
//
int create_dataset(SubTARIndex subtarIndex, OperationPtr operation,
                   ConfigurationManagerPtr configurationManager,
                   QueryDataManagerPtr queryDataManager,
                   MetadataManagerPtr metadataManager,
                   StorageManagerPtr storageManager, EnginePtr engine) {

  regex range("[0-9]+:[0-9]+:[0-9]+:[0-9]+");

  try {
    ParameterPtr parameter = operation->GetParametersByName(COMMAND);
    ParameterPtr parameter2 = operation->GetParametersByName(OPERAND(0));
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

      if (!metadataManager->ValidateIdentifier(dsName, "dataset"))
        throw std::runtime_error("Invalid dataset name: " + dsName + ".");

      if (metadataManager->GetDataSetByName(dsName) != nullptr)
        throw std::runtime_error("Dataset " + dsName + " already exists.");

      DataType dsType = DataType(STR2TYPE(type.c_str()), size);
      if (dsType == NO_TYPE)
        throw std::runtime_error("Invalid type: " + type + ".");

      int typeSize = TYPE_SIZE(dsType);
      DatasetPtr ds;

      if (!queryDataManager->GetParamsList().empty()) {
        file = queryDataManager->GetParamFilePath(file);
        fileFiller = true;
      } else if (EXIST_FILE(file)) {
        std::string dir = configurationManager->GetStringValue(SEC_STORAGE_DIR);

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

          storageManager->SetUseSecStorage(true);
          ds = storageManager->Create(dsType, dRanges[0], dRanges[1],
                                      dRanges[2], dRanges[3]);
          storageManager->SetUseSecStorage(false);

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
                operation->GetParametersByName(OPERAND(paramCounter++));
            if (param == nullptr)
              break;
            literals.push_back(param->literal_str);
          }

          if (dsType != CHAR && literals.size() % dsType.vectorLength() != 0)
            throw std::runtime_error("Number of elements in the literal"
                                     " dataset must"
                                     " be a multiple of the type vector"
                                     " length.");

          storageManager->SetUseSecStorage(true);
          ds = storageManager->Create(dsType, literals);
          storageManager->SetUseSecStorage(false);

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
        int64_t fileSize = FILE_SIZE(file.c_str());
        int64_t entryCount = fileSize / dsType.getElementSize();

        if (entryCount % dsType.vectorLength() != 0)
          throw std::runtime_error("Number of elements in dataset must"
                                   " be a multiple of the type vector "
                                   " length.");

        ds = make_shared<Dataset>(UNSAVED_ID, dsName, file, dsType);
        ds->Sorted() = false;

        if (storageManager->Save(ds) == SAVIME_FAILURE) {
          throw std::runtime_error("Could not save dataset. "
                                   "Not enough space left. Consider "
                                   "increasing the max storage size.");
        }
      }

      TARSPtr defaultTARS = metadataManager->GetTARS(
          configurationManager->GetIntValue(DEFAULT_TARS));

      if (metadataManager->SaveDataSet(defaultTARS, ds) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save dataset.");
      }
    } else {
      throw std::runtime_error("Invalid dataset definition.");
    }
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

/*
 * LOAD_SUBTAR("tar_name", "dimension_specs", "dataset_specs");
 * dimension_specs
 *      ordered: type, dimensionName, lower_bound, upper_bound, order |
 *      partial: type, dimensionName, lower_bound, upper_bound, dataset |
 *      total:   type, dimensionName, lower_bound, upper_bound, dataset |
 * dataset_specs: attribute_name, dataset_name | ...
 */
int load_subtar(SubTARIndex subtarIndex, OperationPtr operation,
                ConfigurationManagerPtr configurationManager,
                QueryDataManagerPtr queryDataManager,
                MetadataManagerPtr metadataManager,
                StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    std::list<ParameterPtr> parameters = operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string tarName = parameter->literal_str;
    tarName = trim_delimiters(tarName);
    parameters.pop_front();

    parameter = parameters.front();
    std::string dimSpecsBlock = parameter->literal_str;
    dimSpecsBlock = trim_delimiters(dimSpecsBlock);
    parameters.pop_front();

    parameter = parameters.front();
    std::string dsSpecsBlock = parameter->literal_str;
    dsSpecsBlock = trim_delimiters(dsSpecsBlock);
    parameters.pop_front();

    TARSPtr defaultTARS = metadataManager->GetTARS(
        configurationManager->GetIntValue(DEFAULT_TARS));
    TARPtr tar = metadataManager->GetTARByName(defaultTARS, tarName);

    if (tar == nullptr)
      throw std::runtime_error(tarName + " does not exist.");

    if (tar->GetSubtars().size() >= MAX_NUM_SUBTARS_IN_TAR)
      throw std::runtime_error("Max number of subtars for " + tar->GetName() +
          " reached.");

    std::list<DimSpecPtr> dimSpecs = create_dimensionsSpecs(
        dimSpecsBlock, tar, metadataManager, storageManager);

    SubtarPtr subtar = SubtarPtr(new Subtar());
    subtar->SetTAR(tar);
    subtar->SetId(UNSAVED_ID);

    for (auto &dimSpec : dimSpecs) {
      subtar->AddDimensionsSpecification(dimSpec);
    }

    validate_subtar_size(subtar);

    for (auto &dimSpec : dimSpecs) {
      validate_dimensionSpecs(dimSpec, storageManager);
    }

    int64_t subtarTotalLength = subtar->GetFilledLength();
    std::vector<std::string> dataSetSpecs = split(dsSpecsBlock, '|');
    for (auto &dsSpec : dataSetSpecs) {
      std::vector<std::string> dsSpecSplit = split(dsSpec, ',');

      if (dsSpecSplit.size() == 2) {
        std::string attName = trim(dsSpecSplit.front());
        std::string dsName = trim(dsSpecSplit.back());
        DataElementPtr dataElement = tar->GetDataElement(attName);

        if (dataElement != nullptr &&
            dataElement->GetType() == ATTRIBUTE_SCHEMA_ELEMENT) {
          AttributePtr att = dataElement->GetAttribute();
          DatasetPtr ds = metadataManager->GetDataSetByName(dsName);
          if (ds == nullptr)
            throw std::runtime_error("Dataset " + dsName + " not found.");

          if (att->GetType() != ds->GetType()
              || ds->GetEntryCount() < subtarTotalLength)
            throw std::runtime_error(
                "Dataset " + dsName +
                    " do not conform with attribute or subtar specification. "
                    "Dataset entry count: " + to_string(ds->GetEntryCount()) +
                    ". Expected: " + to_string(subtarTotalLength) + ".");

          subtar->AddDataSet(att->GetName(), ds);
        } else {
          throw std::runtime_error("Not a valid attribute name: " + attName +
              ".");
        }
      } else {
        throw std::runtime_error("Invalid datasets definition.");
      }
    }

    auto intersectionSubtars = tar->GetIntersectingSubtars(subtar);
    if (!intersectionSubtars.empty())
      throw std::runtime_error("This new subtar definition intersects with "
                               "already existing subtar!");

    if (metadataManager->SaveSubtar(tar, subtar) == SAVIME_FAILURE) {
      throw std::runtime_error("Could not insert subtar.");
    }

    updateCurrentUpperBounds(tar, subtar);
    metadataManager->RegisterQuery(queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

int drop_tars(SubTARIndex subtarIndex, OperationPtr operation,
              ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager,
              MetadataManagerPtr metadataManager,
              StorageManagerPtr storageManager, EnginePtr engine) {
  return SAVIME_SUCCESS;
}

int drop_tar(SubTARIndex subtarIndex, OperationPtr operation,
             ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager,
             MetadataManagerPtr metadataManager,
             StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    std::list<ParameterPtr> parameters = operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string tarName = parameter->literal_str;
    tarName = trim_delimiters(tarName);
    parameters.pop_front();

    int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
    TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
    TARPtr tar = metadataManager->GetTARByName(defaultTars, tarName);

    if (tar == nullptr)
      throw std::runtime_error("There is no TAR named " + tarName + ".");

    if (metadataManager->RemoveTar(defaultTars, tar) != SAVIME_SUCCESS)
      throw std::runtime_error("Could not remove " + tarName + ".");

    metadataManager->RegisterQuery(queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

int drop_type(SubTARIndex subtarIndex, OperationPtr operation,
              ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager,
              MetadataManagerPtr metadataManager,
              StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    std::list<ParameterPtr> parameters = operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string typeName = parameter->literal_str;
    typeName = trim_delimiters(typeName);
    parameters.pop_front();

    int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
    TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
    TypePtr type = metadataManager->GetTypeByName(defaultTars, typeName);

    if (type == nullptr)
      throw std::runtime_error("There is no type named " + typeName + ".");

    if (metadataManager->RemoveType(defaultTars, type) != SAVIME_SUCCESS)
      throw std::runtime_error(
          "Could not remove type. It was possibly given to at least one TAR.");

    metadataManager->RegisterQuery(queryDataManager->GetQueryText());
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

int drop_dataset(SubTARIndex subtarIndex, OperationPtr operation,
                 ConfigurationManagerPtr configurationManager,
                 QueryDataManagerPtr queryDataManager,
                 MetadataManagerPtr metadataManager,
                 StorageManagerPtr storageManager, EnginePtr engine) {
  try {
    std::list<ParameterPtr> parameters = operation->GetParameters();
    ParameterPtr parameter = parameters.front();
    std::string dsName = parameter->literal_str;
    dsName = trim_delimiters(dsName);
    parameters.pop_front();

    int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
    TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
    DatasetPtr ds = metadataManager->GetDataSetByName(dsName);

    if (ds == nullptr)
      throw std::runtime_error("There is no dataset named " + dsName + ".");

    if (metadataManager->RemoveDataSet(defaultTars, ds) != SAVIME_SUCCESS)
      throw std::runtime_error("Could not remove dataset. It is possibly being "
                               "used by at least one TAR.");
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}

int save(SubTARIndex subtarIndex, OperationPtr operation,
         ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager,
         MetadataManagerPtr metadataManager,
         StorageManagerPtr storageManager, EnginePtr engine) {

  string file = operation->GetParametersByName(COMMAND)->literal_str;
  file = trim_delimiters(file);
  int32_t fd = open(file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd == -1) {
    throw std::runtime_error("Could not open file: " + file +
        " Error: " + std::string(strerror(errno)));
  }

  for (auto query : metadataManager->GetQueries()) {
    auto query_nl = query + _NEWLINE;
    write(fd, query_nl.c_str(), query_nl.size());
  }
  close(fd);

  return SAVIME_SUCCESS;
}

int show(SubTARIndex subtarIndex, OperationPtr operation,
         ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager,
         MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
         EnginePtr engine) {

  TARSPtr defaultTARS =
      metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));

  try {
    if (defaultTARS == nullptr)
      throw std::runtime_error("Invalid default TARS configuration!");

    std::string dumpText = defaultTARS->name + " TARS:" + _NEWLINE + _NEWLINE;

    for (auto &tar : metadataManager->GetTARs(defaultTARS)) {
      int64_t subtarCount = 0;
      dumpText += tar->toString() + _NEWLINE + "  Subtars:" + _NEWLINE;

      for (auto &subtar : metadataManager->GetSubtars(tar)) {
        dumpText += "    subtar #" + to_string(subtarCount++) + _SPACE +
            subtar->toString() + _NEWLINE;
      }

      dumpText += _NEWLINE;
    }
    queryDataManager->SetQueryResponseText(dumpText);
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}