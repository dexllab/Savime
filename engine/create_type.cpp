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

CreateType::CreateType(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult CreateType::Run() {
  try {
    ParameterPtr parameter = _operation->GetParameters().front();
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

    if (_metadataManager->ValidateIdentifier(newTypeName, "type")) {
      TARSPtr defaultTARS = _metadataManager->GetTARS(
        _configurationManager->GetIntValue(DEFAULT_TARS));

      if (_metadataManager->GetTypes(defaultTARS).size()
          >= MAX_NUM_OF_TYPES_IN_TARS)
        throw std::runtime_error("Too many types already defined.");

      if (_metadataManager->GetTypeByName(defaultTARS, newTypeName) != nullptr)
        throw std::runtime_error("Type " + newTypeName + " already exists.");

      TypePtr newType = std::make_shared<Type>();
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
            _metadataManager->ValidateIdentifier(trimmedRoleWithoutStar,
                                                "role")) {
          roleMaps[trimmedRole] = trimmedRoleWithoutStar;
        } else {
          throw std::runtime_error("Invalid or duplicate definition of role: " +
                                   role);
        }
      }

      for (auto entry : roleMaps) {
        RolePtr role = std::make_shared<Role>();
        role->id = UNSAVED_ID;
        role->name = entry.second;
        role->is_mandatory = entry.first[0] != _TYPELESS_TAR_MARK;
        newType->roles[role->name] = role;
      }

      if (newType->roles.size() > MAX_NUM_OF_ROLES_IN_TYPE) {
        throw std::runtime_error("Too many roles defined in the type.");
      }

      if (_metadataManager->SaveType(defaultTARS, newType) == SAVIME_FAILURE) {
        throw std::runtime_error("Could not save new Type: " + newTypeName);
      }

      _metadataManager->RegisterQuery(_queryDataManager->GetQueryText());

    } else {
      throw std::runtime_error(
        "Invalid or already existing identifier for Type: " + newTypeName);
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }
}
