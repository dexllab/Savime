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
#ifndef CREATE_TAR_H
#define CREATE_TAR_H

#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"

/*Auxiliary functions for create TAR:
 * Dimensions can be either natural integer or long typed with and integer
 * spacing equals to 1 (simplest case) or greater than
 * They can also be float or double typed, and also have a float/double spacing
 * value
 * In addition, dimensions can be explicity, meaning the user must specify a
 * dataset with values to be explicitly mapped from and integer
 * index into a value in the mapping.
 * Upper and lower bound have different meanings. If it is a spaced or
 * implicitly dimensions, it must hold the actual values.
 * If it is an explicity, then the upper and lower bounds must be integer/long
 * indexes that will be mapped to values in the dataset.
 */
inline std::list<DimensionPtr> create_dimensions(std::string dimBlock,
                                          MetadataManagerPtr metadataManager,
                                          StorageManagerPtr storageManager) {
  std::list<DimensionPtr> dimensions;
  std::vector<std::string> dimensionsSpecification = split(dimBlock, '|');

  for (std::string dimSpecs : dimensionsSpecification) {

    std::vector<std::string> params = split(dimSpecs, ',');
    //DimensionPtr dimension = DimensionPtr(new Dimension);

    auto dimension_type = STR2DIMTYPE(trim(params[0]).c_str());
    if (dimension_type == NO_DIM_TYPE)
      throw std::runtime_error("Invalid type " + trim(params[0]) +
                               " in dimension specification.");

    if (dimension_type == IMPLICIT) {
      if (params.size() != 6)
        throw std::runtime_error(
            "Invalid dimension definition. Wrong number of parameters.");

      // name validation done outside this function
      double lower_bound, upper_bound, spacing;
      auto dimensionName = trim(params[1]);

      DataType type = STR2TYPE(trim(params[2]).c_str());
      if (type == NO_TYPE)
        throw std::runtime_error("Invalid type in dimension specification: " +
                                 params[2]);

      lower_bound = strtod(trim(params[3]).c_str(), NULL);

      if (trim(params[4])[0] == _UNBOUNDED_DIMENSION_MARK) {
        if (!type.isIntegerType()) {
          throw std::runtime_error("Non integer dimension " + dimensionName +
                                   " cannot be unbounded.");
        }

        switch (type.type()) {
        case CHAR:
          upper_bound = std::numeric_limits<int8_t>::max() - 1;
          break;
#ifdef FULL_TYPE_SUPPORT
        case UCHAR:
          upper_bound = std::numeric_limits<int16_t>::max() - 1;
          break;
        case INT8:
          upper_bound = std::numeric_limits<int8_t>::max() - 1;
          break;
        case INT16:
          upper_bound = std::numeric_limits<int16_t>::max() - 1;
          break;
#endif
        case INT32:
          upper_bound = std::numeric_limits<int32_t>::max() - 1;
          break;
        case INT64:
          upper_bound = MAX_DIM_LEN-2;
          break;
#ifdef FULL_TYPE_SUPPORT
        case UINT8:
          upper_bound = std::numeric_limits<uint8_t>::max() - 1;
          break;
        case UINT16:
          upper_bound = std::numeric_limits<uint16_t>::max() - 1;
          break;
        case UINT32:
          upper_bound = std::numeric_limits<uint32_t>::max() - 1;
          break;
        case UINT64:
         upper_bound = MAX_DIM_LEN-2;
          break;
#endif
         default:
          throw std::runtime_error("Invalid dimension type.");
        }

      } else {
        upper_bound = strtod(trim(params[4]).c_str(), NULL);
      }

      if (upper_bound <= lower_bound)
        throw std::runtime_error(
            "Dimension " + dimensionName +
            " upper bound must be greater than the lower bound.");

      spacing = strtod((trim(params[5]).c_str()), NULL);

      if (spacing <= 0)
        throw std::runtime_error("Spacing for dimension " + dimensionName +
            " must be greater than 0.");

      if (spacing < 1 && type.isIntegerType())
        throw std::runtime_error("Spacing for dimension " + dimensionName +
            " must be equal or greater than 1.");

#ifdef FULL_TYPE_SUPPORT
      SET_SPACING(int8_t, INT8);
      SET_SPACING(int16_t, INT16);
      SET_SPACING(uint8_t, UINT8);
      SET_SPACING(uint16_t, UINT16);
      SET_SPACING(uint32_t, UINT32);
      SET_SPACING(uint64_t, UINT64);
#endif

      DimensionPtr dimension = make_shared<Dimension>(UNSAVED_ID,
                                                      dimensionName,
                                                      type,
                                                      lower_bound,
                                                      upper_bound,
                                                      spacing);
      dimensions.push_back(dimension);

    } else if (dimension_type == EXPLICIT) {

      auto dimensionName = trim(params[1]);

      DatasetPtr ds = metadataManager->GetDataSetByName(trim(params[2]));
      if (ds == nullptr)
        throw std::runtime_error("Invalid dataset name: " + trim(params[2]));

      DimensionPtr
          dimension = make_shared<Dimension>(UNSAVED_ID, dimensionName, ds);

      if (!dimension->GetDataset()->Sorted())
        storageManager->CheckSorted(dimension->GetDataset());

      dimensions.push_back(dimension);
    }
  }

  return dimensions;
}

inline std::list<AttributePtr> create_attributes(std::string attBlock) {
  std::list<AttributePtr> attributes;
  int32_t vectorLen = 0;
  std::vector<std::string> attributeSpecification = split(attBlock, _PIPE[0]);

  for (std::string attSpecs : attributeSpecification) {
    std::vector<std::string> params = split(attSpecs, _COMMA[0]);

    if (params.size() != 2)
      throw std::runtime_error("Invalid attribute definition.");


    auto attName = trim(params[0]);
    EnumDataType type = STR2TYPEV(trim(params[1]).c_str(), vectorLen);

    if(vectorLen > MAX_VECTOR_ATT_LEN)
      throw std::runtime_error("Vector attribute "+attName+" is too large.");

    auto attType = DataType(type, vectorLen);

    if (attType == NO_TYPE || attType.vectorLength() == 0)
      throw std::runtime_error("Invalid type in attribute specification: " +
                               params[1]);

    AttributePtr att = make_shared<Attribute>(UNSAVED_ID, attName, attType);
    attributes.push_back(att);
  }

  return attributes;
}

inline std::map<std::string, RolePtr> create_roles(TARPtr tar, TypePtr type,
                                            std::string rolesBlock) {
  if (rolesBlock.empty())
    throw std::runtime_error("Invalid roles definition.");

  std::vector<std::string> params = split(rolesBlock, _COMMA[0]);
  std::map<std::string, std::string> roles2Element;
  std::map<std::string, std::string> element2Role;
  std::map<std::string, RolePtr> roleSpecification;

  if (params.size() % 2 != 0)
    throw std::runtime_error("Invalid roles block definition.");

  // Check for undefined or duplicated types or roles definitions
  for (int i = 0; i < params.size(); i += 2) {
    params[i] = trim(params[i]);
    params[i + 1] = trim(params[i + 1]);

    if (type->roles.find(params[i + 1]) == type->roles.end())
      throw std::runtime_error("The role " + params[i + 1] +
                               " is not defined in the type: " + type->name +
                               ".");

    if (tar->GetDataElement(params[i]) == nullptr)
      throw std::runtime_error("The data element " + params[i] +
                               " has not been defined.");

    if (element2Role.find(params[i]) == element2Role.end())
      element2Role[params[i]] = params[i + 1];
    else
      throw std::runtime_error("Duplicated definition for data element " +
                               params[i] + " in roles definition.");

    element2Role[params[i]] = params[i + 1];

    if (roles2Element.find(params[i + 1]) == roles2Element.end())
      roles2Element[params[i + 1]] = params[i];
    else
      throw std::runtime_error("Duplicated definition for role " +
                               params[i + 1] + " in roles definition.");
  }

  // test if all mandatory roles are defined
  for (auto entry : type->roles) {
    if (entry.second->is_mandatory) {
      if (roles2Element.find(entry.first) == roles2Element.end())
        throw std::runtime_error("Mandatory role " + entry.second->name +
                                 " is not assigned a data element.");
    }
  }

  // Creating roles specification
  for (auto entry : element2Role) {
    roleSpecification[entry.first] = type->roles[entry.second];
  }

  return roleSpecification;
}
#endif /* CREATE_TAR_H */

