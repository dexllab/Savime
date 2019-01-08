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
#include "include/ddl_operators.h"

CreateTAR::CreateTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                             QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                             StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult CreateTAR::Run() {
  try {
    std::map<std::string, RolePtr> roles;
    std::list<AttributePtr> atts;
    std::list<DimensionPtr> dims;
    TypePtr type = nullptr;

    std::list<ParameterPtr> parameters = _operation->GetParameters();
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

    if (!_metadataManager->ValidateIdentifier(tarName, "tar"))
      throw std::runtime_error("Invalid identifier for TAR: " + tarName);

    TARSPtr defaultTARS = _metadataManager->GetTARS(
      _configurationManager->GetIntValue(DEFAULT_TARS));

    if (_metadataManager->GetTARs(defaultTARS).size() >= MAX_NUM_TARS_IN_TARS)
      throw std::runtime_error("Too many TARs already defined.");

    if (_metadataManager->GetTARByName(defaultTARS, tarName) != nullptr)
      throw std::runtime_error(tarName + " TAR already exists.");

    if (typeName != _ASTERISK) {
      type = _metadataManager->GetTypeByName(defaultTARS, typeName);
      if (type == nullptr)
        throw std::runtime_error("Undefined type: " + typeName);
    }

    dims = create_dimensions(dimBlock, _metadataManager, _storageManager);
    atts = create_attributes(attBlock);

    TARPtr newTar = std::make_shared<TAR>();
    newTar->AlterTAR(UNSAVED_ID, tarName);
    newTar->AlterType(type);

    for (const auto &d : dims) {
      if (!_metadataManager->ValidateIdentifier(d->GetName(), "dimension"))
        throw std::runtime_error("Invalid dimension name: " + d->GetName());

      if (d->GetName() == DEFAULT_SYNTHETIC_DIMENSION)
        throw std::runtime_error("A dimension can not be named " +
                                 d->GetName() + ".");

      if (newTar->GetDataElement(d->GetName()) != nullptr)
        throw std::runtime_error(
          "Duplicated data element name: " + d->GetName());

      newTar->AddDimension(d);
    }

    for (const auto &a : atts) {
      if (!_metadataManager->ValidateIdentifier(a->GetName(), "attribute"))
        throw std::runtime_error("Invalid attribute name: " + a->GetName());

      if (a->GetName() == DEFAULT_SYNTHETIC_DIMENSION)
        throw std::runtime_error("A dimension can not be named " +
                                 a->GetName() + ".");

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

    if (_metadataManager->SaveTAR(defaultTARS, newTar) == SAVIME_FAILURE)
      throw std::runtime_error("Could not save new TAR: " + tarName + ".");

    _metadataManager->RegisterQuery(_queryDataManager->GetQueryText());
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
}
