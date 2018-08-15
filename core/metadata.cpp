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
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "include/metadata.h"
#include "include/types.h"
#include "include/parser.h"
#include "include/util.h"


using namespace std;

//------------------------------------------------------------------------------
// Declarations

const char *dimTypeNames[] = {"explicit", "implicit", "spaced"};
const char *specsTypeNames[] = {"ordered", "partial", "total", "morton"};

std::mutex TAR::_mutex;
std::vector<int64_t> TAR::_intersectingSubtarsIndexes;

//------------------------------------------------------------------------------
DimensionType STR2DIMTYPE(const char *type) {
  switch (type[0]) {
  case 'e':
    return EXPLICIT;
  case 'i':
    return IMPLICIT;
  default:
    return NO_DIM_TYPE;
  }
}

SpecsType STR2SPECTYPE(const char *type) {
  switch (type[0]) {
  case 'o':
    return ORDERED;
  case 'p':
    return PARTIAL;
  case 't':
    return TOTAL;
  default:
    NO_SPEC_TYPE;
  }
}

bool compareAdj(DimSpecPtr a, DimSpecPtr b) {
  return (a->GetAdjacency() > b->GetAdjacency());
}

//------------------------------------------------------------------------------
// Dataset member functions
void Dataset::Define(int64_t id, string name, string file, DataType type) {
  this->_dsId = id;
  if (name.empty())
    this->_name = file;
  else
    this->_name = name;

  if (!EXIST_FILE(file))
    throw runtime_error("Attempt to create a dataset with non-existing file.");

  auto fileSize = FILE_SIZE(file.c_str());
  if (fileSize <= 0)
    throw runtime_error("Attempt to create a dataset with empty file.");

  if (type.type() == NO_TYPE)
    throw runtime_error("Attempt to create a dataset with invalid type.");

  savime_size_t remainder = fileSize % type.getSize();
  if (remainder != 0)
    throw runtime_error("Attempt to create a dataset with invalid file size.");

  this->_location = file;
  this->_type = type;
  this->_length = (savime_size_t)fileSize;
  this->_entry_count = (savime_size_t)(fileSize / type.getSize());

  if (this->_entry_count > MAX_ENTRIES_IN_DATASET)
    throw std::runtime_error("Attempt to create a dataset with invalid size.");
}

Dataset::Dataset(int64_t id, string name, string file, DataType type) {
  Define(id, name, file, type);
}

Dataset::Dataset(savime_size_t bitmaskSize) {
  this->_dsId = UNSAVED_ID;

  if (bitmaskSize > MAX_ENTRIES_IN_DATASET)
    throw std::runtime_error("Attempt to create a bitmask dataset "
                             "with invalid size.");

  this->_bitMask = make_shared<boost::dynamic_bitset<>>(bitmaskSize);
  if (_bitMask == nullptr)
    throw runtime_error("Could not allocate memory for dataset bitmask.");

  this->_length = bitmaskSize;
  this->_entry_count = bitmaskSize;
  this->_type = NO_TYPE;
}

Dataset::Dataset(BitsetPtr bitMask) {
  this->_dsId = UNSAVED_ID;
  if (bitMask == nullptr)
    throw runtime_error("Attempt to create bitmask dataset from null bitmask.");

  if (bitMask->size() > MAX_ENTRIES_IN_DATASET)
    throw std::runtime_error("Attempt to create a bitmaks dataset "
                             "from a bitmask that is too large.");

  this->_bitMask = bitMask;
  this->_length = bitMask->size();
  this->_entry_count = bitMask->size();
  this->_type = NO_TYPE;
}

void Dataset::InflateBitMask() {
  _bitMask = make_shared<boost::dynamic_bitset<>>(_entry_count);
  if (_bitMask == nullptr)
    throw runtime_error("Could not allocate memory for dataset bitmask.");
}

savime_size_t Dataset::GetBitsPerBlock() {
  return boost::dynamic_bitset<>::bits_per_block;
}

void Dataset::Resize(savime_size_t size) {

  if (size == 0)
    throw std::runtime_error("Attempt to set dataset size to zero.");

  if (size > MAX_ENTRIES_IN_DATASET)
    throw std::runtime_error("Attempt to set dataset size over the limit.");

  this->_length = (savime_size_t)size;
  this->_entry_count = (savime_size_t)(size / _type.getSize());
}

void Dataset::Redefine(int64_t id, string name, string file, DataType type) {
  Define(id, name, file, type);
}

Dataset::~Dataset() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObject *)this);
}

//------------------------------------------------------------------------------
// Dimension member functions

Dimension::Dimension(int32_t id, string name, DataType type, double lower_bound,
                     double upper_bound, double spacing) {

  this->_dimId = id;
  if (!validadeIdentifier(name)) {
    runtime_error("Attempt to create a dimension with invalid name.");
  }
  this->_name = name;

  if (type == NO_TYPE || type.isVector()) {
    throw runtime_error("Attempt to create a dimension with invalid type.");
  }
  this->_type = type;

  if (upper_bound < lower_bound)
    throw runtime_error("Attempt to create a dimension with invalid bounds.");

  if (std::isinf(lower_bound) || std::isnan(lower_bound))
    throw runtime_error(
      "Attempt to create a dimension with invalid lower bound.");

  if (std::isinf(upper_bound) || std::isnan(upper_bound))
    throw runtime_error(
      "Attempt to create a dimension with invalid upper bound.");

  if (std::abs(lower_bound) > MAX_DIM_BOUND ||
    std::abs(upper_bound) > MAX_DIM_BOUND)
    throw runtime_error("Magnitude of bound values is too large.");

  if (check_precision_loss(lower_bound, type) ||
    check_precision_loss(upper_bound, type))
    throw runtime_error("Bounds are not within valid range for defined type.");

  this->_lower_bound = lower_bound;
  this->_upper_bound = upper_bound;

  if (std::isinf(spacing) || std::isnan(spacing))
    throw runtime_error("Attempt to create a dimension with invalid spacing.");

  if (spacing < MIN_SPACING)
    throw std::runtime_error("Attempt to create dimension with "
                             " spacing too small.");

  if (type.isIntegerType()) {
    double integerPart;

    std::modf(lower_bound, &integerPart);
    if (lower_bound != integerPart)
      throw std::runtime_error("Attempt to create dimension with "
                               "invalid lower bound.");

    std::modf(upper_bound, &integerPart);
    if (upper_bound != integerPart)
      throw std::runtime_error("Attempt to create dimension with "
                               "invalid upper bound.");

    std::modf(spacing, &integerPart);
    if (spacing != integerPart)
      throw std::runtime_error("Attempt to create dimension with "
                               "invalid spacing.");
  }
  _spacing = spacing;
  savime_size_t dimensionLength = GetLength();

  if (dimensionLength > MAX_DIM_LEN)
    throw std::runtime_error("Attempt to create too long dimension.");

  _real_upper_bound = (RealIndex)(dimensionLength - 1);
  _current_upper_bound = 0;
  _dimension_type = IMPLICIT;
  _dataset = nullptr;
}

Dimension::Dimension(int32_t id, string name, DatasetPtr dataset) {
  this->_dimId = id;
  if (!validadeIdentifier(name)) {
    throw runtime_error("Attempt to create a dimension with invalid name.");
  }
  this->_name = name;

  if (dataset == nullptr) {
    throw runtime_error("Attempt to create a dimension with invalid dataset.");
  }

  if (dataset->GetType() == NO_TYPE) {
    throw runtime_error(
      "Attempt to create a dimension with a no type dataset.");
  }
  this->_type = dataset->GetType();

  if (dataset->GetType().isVector()) {
    throw runtime_error(
      "Attempt to create a dimension with a vector typed dataset.");
  }

  if (dataset->GetEntryCount() == 0) {
    throw runtime_error(
      "Attempt to create an explicit dimension with empty dataset.");
  }

  if (dataset->GetEntryCount() > MAX_DIM_LEN) {
    throw runtime_error(
      "Attempt to create an explicit dimension with a dataset that "
      "is too large.");
  }

  _lower_bound = 0.0;
  _upper_bound = (RealIndex)(dataset->GetEntryCount() - 1);
  _spacing = 1.0;
  _real_upper_bound = (RealIndex)(_upper_bound);
  _current_upper_bound = 0;
  _dimension_type = EXPLICIT;
  _dataset = dataset;
}

void Dimension::AlterName(string name) {
  if (!validadeIdentifier(name)) {
    throw runtime_error(
      "Attempt to set a dimension name to an invalid identifier.");
  }
  _name = name;
}

Dimension::~Dimension() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObject *)this);
}

//------------------------------------------------------------------------------
// Attribute member functions
Attribute::Attribute(int32_t id, string name, DataType type) {
  _id = id;
  if (!validadeIdentifier(name))
    throw runtime_error(
      "Attempt to set an attribute name to an invalid identifier.");
  _name = name;

  if (type == NO_TYPE)
    throw runtime_error("Attempt to create an attribute with an invalid type.");

  if (type.vectorLength() == 0 || type.vectorLength() > MAX_VECTOR_ATT_LEN)
    throw runtime_error(
      "Attempt to create a vector typed attribute with an invalid length.");

  _type = type;
}

Attribute::~Attribute() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObject *)this);
}

//------------------------------------------------------------------------------
// TARS member functions
TARS::~TARS() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObject *)this);
}

//------------------------------------------------------------------------------
// DATAELEMENT member Functions
DataElement::DataElement(DimensionPtr dimension) {

  if (dimension == nullptr)
    throw runtime_error(
      "Attempt to create data element for dimension with an invalid reference.");
  _dimension = dimension;
  _attribute = nullptr;
  _type = DIMENSION_SCHEMA_ELEMENT;
}

DataElement::DataElement(AttributePtr attribute) {
  if (attribute == nullptr)
    throw runtime_error(
      "Attempt to create data element for attribute with an invalid reference.");
  _attribute = attribute;
  _dimension = nullptr;
  _type = ATTRIBUTE_SCHEMA_ELEMENT;
}

DimensionPtr DataElement::GetDimension() { return _dimension; }

AttributePtr DataElement::GetAttribute() { return _attribute; }

DataElementType DataElement::GetType() { return _type; }

std::string DataElement::GetName() {
  switch (_type) {
  case DIMENSION_SCHEMA_ELEMENT:
    return _dimension->GetName();
    break;
  case ATTRIBUTE_SCHEMA_ELEMENT:
    return _attribute->GetName();
    break;
  default:
    return "";
  }
}

DataType DataElement::GetDataType() {
  switch (_type) {
  case DIMENSION_SCHEMA_ELEMENT:
    return _dimension->GetType();
    break;
  case ATTRIBUTE_SCHEMA_ELEMENT:
    return _attribute->GetType();
    break;
  }
}

bool DataElement::IsNumeric() {
  DataType dataType;

  switch (_type) {
  case DIMENSION_SCHEMA_ELEMENT:
    dataType = _dimension->GetType();
    break;
  case ATTRIBUTE_SCHEMA_ELEMENT:
    dataType = _attribute->GetType();
    break;
  default:
    return false;
  }

  return dataType.isNumeric();
}

DataElement::~DataElement() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObjectPtr)this);
}
//------------------------------------------------------------------------------
// DimensionSpecification members implementations
DimensionSpecification::DimensionSpecification(
  int32_t id, DimensionPtr dimension, RealIndex lowerBound,
  RealIndex upperBound, savime_size_t skew, savime_size_t adjacency) {

  _id = id;
  if (dimension == nullptr)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid dimension.");
  _dimension = dimension;

  if (lowerBound < 0 || lowerBound > upperBound)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");

  if (lowerBound > _dimension->GetRealUpperBound() ||
    upperBound > _dimension->GetRealUpperBound())
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");
  _lower_bound = lowerBound;
  _upper_bound = upperBound;

  if (skew < 1)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid skew value.");
  _skew = skew;

  if (adjacency < 1)
    throw runtime_error("Attempt to create a dimension specification with "
                        "invalid adjacency value.");
  _adjacency = adjacency;
  _dataset = nullptr;
  _materialized = nullptr;
  _type = ORDERED;
}

DimensionSpecification::DimensionSpecification(
  int32_t id, DimensionPtr dimension, DatasetPtr dataset,
  RealIndex lowerBound, RealIndex upperBound, savime_size_t skew,
  savime_size_t adjacency) {
  _id = id;
  if (dimension == nullptr)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid dimension.");
  _dimension = dimension;

  if (lowerBound < 0 || lowerBound > upperBound)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");

  if (lowerBound > dimension->GetRealUpperBound() ||
    upperBound > dimension->GetRealUpperBound())
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");
  _lower_bound = lowerBound;
  _upper_bound = upperBound;

  auto minLength = _upper_bound - _lower_bound + 1;
  if (minLength < dataset->GetEntryCount())
    throw runtime_error("Attempt to create a dimension specification with a "
                        "dataset too large for the specified boundaries.");

  if (skew < 1)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid skew value.");
  _skew = skew;

  if (adjacency < 1)
    throw runtime_error("Attempt to create a dimension specification with "
                        "invalid adjacency value.");
  _adjacency = adjacency;

  if (dataset == nullptr)
    throw runtime_error("Attempt to create a partial dimension specification "
                        "with invalid dataset.");

  _dataset = dataset;
  _materialized = nullptr;
  _type = PARTIAL;
}

DimensionSpecification::DimensionSpecification(int32_t id,
                                               DimensionPtr dimension,
                                               DatasetPtr dataset,
                                               RealIndex lowerBound,
                                               RealIndex upperBound) {
  _id = id;
  if (dimension == nullptr)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid dimension.");
  _dimension = dimension;

  if (lowerBound < 0 || lowerBound > upperBound)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");

  if (lowerBound > dimension->GetRealUpperBound() ||
    upperBound > dimension->GetRealUpperBound())
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");
  _lower_bound = lowerBound;
  _upper_bound = upperBound;

  if (dataset == nullptr)
    throw runtime_error("Attempt to create a total dimension specification "
                        "with invalid dataset.");

  _dataset = dataset;
  _materialized = nullptr;
  _type = TOTAL;
}

void DimensionSpecification::AlterDimension(DimensionPtr dimension) {
  if (dimension == nullptr)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid dimension.");
  _dimension = dimension;

  if (_lower_bound > dimension->GetRealUpperBound() ||
    _upper_bound > dimension->GetRealUpperBound())
    throw runtime_error(
      "Attempt to create a dimension specification with invalid bounds.");
}

void DimensionSpecification::AlterSkew(savime_size_t skew) {
  if (skew < 1)
    throw runtime_error(
      "Attempt to create a dimension specification with invalid skew value.");
  _skew = skew;
}

void DimensionSpecification::AlterAdjacency(savime_size_t adj) {
  if (adj < 1)
    throw runtime_error("Attempt to create a dimension specification with "
                        "invalid adjacency value.");
  _adjacency = adj;
}

void DimensionSpecification::AlterBoundaries(RealIndex lowerBound,
                                             RealIndex upperBound) {
  if (lowerBound < 0 || lowerBound > upperBound)
    throw runtime_error(
      "Attempt to set a dimension specification with invalid bounds.");

  if (lowerBound > _dimension->GetRealUpperBound() ||
    upperBound > _dimension->GetRealUpperBound())
    throw runtime_error(
      "Attempt to set a dimension specification with invalid bounds.");

  if (_type == PARTIAL) {
    auto minLength = _upper_bound - _lower_bound + 1;
    if (minLength < _dataset->GetEntryCount())
      throw runtime_error(
        "Attempt to set a dimension specification with invalid boundaries.");
  }

  _lower_bound = lowerBound;
  _upper_bound = upperBound;
}

void DimensionSpecification::AlterDataset(DatasetPtr dataset) {
  if (_type != ORDERED && dataset == NULL) {
    throw runtime_error("Attempt to set invalida dataset to non ordered "
                        "dimension specification.");
  }
  _dataset = dataset;
}

std::shared_ptr<DimensionSpecification> DimensionSpecification::Clone() {
  if (_type == ORDERED)
    return make_shared<DimensionSpecification>(
      UNSAVED_ID, _dimension, _lower_bound, _upper_bound, _skew, _adjacency);

  if (_type == PARTIAL)
    return make_shared<DimensionSpecification>(UNSAVED_ID, _dimension, _dataset,
                                               _lower_bound, _upper_bound,
                                               _skew, _adjacency);

  if (_type == TOTAL)
    return make_shared<DimensionSpecification>(UNSAVED_ID, _dimension, _dataset,
                                               _lower_bound, _upper_bound);
}

//------------------------------------------------------------------------------
// Subtar members implementations
int32_t Subtar::GetId() { return _id; }

void Subtar::SetId(int32_t id) { _id = id; }

void Subtar::SetTAR(TARPtr tar) { _tar = tar; }

TARPtr Subtar::GetTAR() { return _tar; }

std::map<std::string, DimSpecPtr> &Subtar::GetDimSpecs() { return _dimSpecs; }

std::map<std::string, DatasetPtr> &Subtar::GetDataSets() { return _dataSets; }

savime_size_t Subtar::GetFilledLength() {
  savime_size_t filledLength = 1;
  for (auto entry : _dimSpecs) {
    if (entry.second->GetSpecsType() == ORDERED ||
      entry.second->GetSpecsType() == PARTIAL) {
      filledLength *= entry.second->GetFilledLength();
    } else if (entry.second->GetSpecsType() == TOTAL) {
      return entry.second->GetFilledLength();
    }
  }
  return filledLength;
}

savime_size_t Subtar::GetSpannedLength() {
  savime_size_t filledLength = 1;
  for (auto entry : _dimSpecs) {
    if (entry.second->GetSpecsType() == ORDERED ||
      entry.second->GetSpecsType() == PARTIAL) {
      filledLength *= entry.second->GetFilledLength();
    } else if (entry.second->GetSpecsType() == TOTAL) {
      return entry.second->GetFilledLength();
    }
  }
  return filledLength;
}

void Subtar::AddDimensionsSpecification(DimSpecPtr dimSpecs) {

  if (dimSpecs == nullptr)
    throw runtime_error(
      "Attempt to add an invalid dimension specification in a subTAR.");

  if (_tar != nullptr) {
    auto dataElement =
      _tar->GetDataElement(dimSpecs->GetDimension()->GetName());
    if (dataElement == nullptr || dataElement->GetDimension() == nullptr)
      throw runtime_error("Attempt to add a dimension specification in a "
                          "subTAR with no matching dimension in TAR.");
  }

  savime_size_t currentLen = GetFilledLength();
  savime_size_t dimSpecsLen = dimSpecs->GetFilledLength();
  savime_size_t len = currentLen * dimSpecsLen;

  if (len > MAX_SUBTAR_LEN || len < currentLen || len < dimSpecsLen) {
    throw runtime_error(
      "Attempt to add a too large dimension specification to subTAR.");
  }

  _dimSpecs[dimSpecs->GetDimension()->GetName()] = dimSpecs;
}

void Subtar::AddDataSet(string dataElementName, DatasetPtr dataset) {
  if (dataset == nullptr)
    throw runtime_error("Attempt to add invalid dataset in subTAR.");

  if (_tar != nullptr) {
    auto dataElement = _tar->GetDataElement(dataElementName);
    if (dataElement == nullptr || dataElement->GetAttribute() == nullptr)
      throw runtime_error("Attempt to add a dataset in a subTAR with no "
                          "matching attribute in TAR.");
  }

  _dataSets[dataElementName] = dataset;
}

void Subtar::CreateBoundingBox(int64_t min[], int64_t max[], int32_t n) {
  int32_t i = 0;
  for (auto dimension : _tar->GetDimensions()) {
    auto dimSpec = _dimSpecs[dimension->GetName()];
    min[i] = dimSpec->GetLowerBound();
    max[i] = dimSpec->GetUpperBound();
    if (++i >= n)
      break;
  }
}

DimSpecPtr Subtar::GetDimensionSpecificationFor(string name) {
  if (_dimSpecs.find(name) == _dimSpecs.end())
    return nullptr;

  return _dimSpecs[name];
}

DatasetPtr Subtar::GetDataSetFor(string name) {
  auto splittedName = split(name, INDEX_SEPARATOR);

  if (_dataSets.find(splittedName[0]) == _dataSets.end())
    return nullptr;

  DatasetPtr dsOriginal = _dataSets[splittedName[0]];

  if (dsOriginal->GetType().isVector()) {

    /*Should not add listener,
     * because is a copied instance that
     * serves only for index selecting.*/

    auto newType = dsOriginal->GetType();
    if (splittedName.size() > 1) {
      int32_t selected = strtol(splittedName[1].c_str(), NULL, 10);
      if (selected < newType.vectorLength()) {
        newType = DataType(newType.type(), newType.vectorLength(), selected);
      }
    }

    DatasetPtr dsCopy =
      make_shared<Dataset>(dsOriginal->GetId(), dsOriginal->GetName(),
                           dsOriginal->GetLocation(), newType);
    dsCopy->HasIndexes() = dsOriginal->HasIndexes();
    dsCopy->Sorted() = dsOriginal->Sorted();
    return dsCopy;
  } else {
    return dsOriginal;
  }
}

void Subtar::RemoveTempDataElements() {
  list<string> toRemove;

  for (auto entry : _dataSets) {
    if (entry.first.compare(0, 4, DEFAULT_TEMP_MEMBER) == 0) {
      toRemove.push_back(entry.first);
    }
  }

  for (auto dataElement : toRemove) {
    _dataSets.erase(dataElement);
  }
  toRemove.clear();

  for (auto entry : _dimSpecs) {
    if (entry.first.compare(0, 4, DEFAULT_TEMP_MEMBER) == 0) {
      toRemove.push_back(entry.first);
    }
  }

  for (auto dataElement : toRemove) {
    _dimSpecs.erase(dataElement);
  }
}

bool Subtar::IntersectsWith(SubtarPtr subtar) {
  if (_tar == nullptr)
    return false;

  for (auto dimension : _tar->GetDimensions()) {
    auto dimSpec1 = _dimSpecs[dimension->GetName()];
    auto dimSpec2 = subtar->GetDimensionSpecificationFor(dimension->GetName());

    if (dimSpec1 == nullptr || dimSpec2 == nullptr)
      return false;

    if (dimSpec1->GetLowerBound() <= dimSpec2->GetLowerBound() &&
      dimSpec2->GetLowerBound() <= dimSpec1->GetUpperBound())
      continue;

    if (dimSpec1->GetLowerBound() <= dimSpec2->GetUpperBound() &&
      dimSpec2->GetUpperBound() <= dimSpec1->GetUpperBound())
      continue;

    return false;
  }

  return true;
}

string Subtar::toString() {
  string text = "<";
  for (auto entry : this->_dimSpecs) {
    std::stringstream lower, upper;
    lower << std::setprecision(6) << entry.second->GetLowerBound();
    upper << std::setprecision(6) << entry.second->GetUpperBound();

    text = text + "[" + entry.first + "]" + lower.str() + _COLON + upper.str() +
      " ";
  }

  text = text.substr(0, text.size() - 1) + ">";
  return text;
}

Subtar::~Subtar() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObjectPtr)this);
}

//------------------------------------------------------------------------------
// TAR member functions
bool TAR::validateTARName(string name) { return validadeIdentifier(name); }

bool TAR::validateSchemaElementName(string name) {
  if (!validadeIdentifier(name))
    return false;

  if (HasDataElement(name))
    return false;

  return true;
}

void TAR::validadeSubtar(SubtarPtr subtar) {

  if (_subtars.size() >= MAX_NUM_SUBTARS_IN_TAR) {
    throw runtime_error(
      "Attempt to add subTAR to TAR with already too many subTARs.");
  }

  if (GetDimensions().size() != subtar->GetDimSpecs().size())
    throw runtime_error(
      "Attempt to add subTAR without enough dimension specifications.");

  for (auto entry : subtar->GetDimSpecs()) {
    auto dimSpecs = entry.second;
    string dimName = dimSpecs->GetDimension()->GetName();
    auto dataElement = GetDataElement(dimName);

    if (dataElement == nullptr)
      throw runtime_error(
        "Attempt to add subTAR with invalid dimension specifications.");

    if (dataElement->GetType() == ATTRIBUTE_SCHEMA_ELEMENT)
      throw runtime_error(
        "Attempt to add subTAR with invalid dimension specifications.");
  }
}

TAR::TAR(int32_t id, string name, TypePtr type) {
  if (!validateTARName(name))
    throw runtime_error("Attempt to create TAR with invalid name.");

  _id = id;
  _name = name;
  _subtarsIndex = nullptr;
  _type = type;
}

void TAR::SetRole(std::string dataElementName, RolePtr role) {
  if (_type == nullptr)
    throw runtime_error("Attempt to set role in a typeless TAR.");

  if (_type->roles.find(role->name) == _type->roles.end())
    throw runtime_error(
      "Attempt to set a role that is not part of the TAR's type.");

  if (!HasDataElement(dataElementName))
    throw runtime_error(
      "Attempt to set a role for a non existing data element in TAR.");

  _roles[dataElementName] = role;
}

map<std::string, RolePtr> &TAR::GetRoles() { return _roles; }

list<DimensionPtr> TAR::GetDimensions() {
  list<DimensionPtr> dims;

  for (auto elem : _elements) {
    if (elem->GetType() == DIMENSION_SCHEMA_ELEMENT) {
      dims.push_back(elem->GetDimension());
    }
  }

  return dims;
}

std::list<AttributePtr> TAR::GetAttributes() {
  std::list<AttributePtr> attribs;

  for (auto elem : _elements) {
    if (elem->GetType() == ATTRIBUTE_SCHEMA_ELEMENT) {
      attribs.push_back(elem->GetAttribute());
    }
  }

  return attribs;
}

void TAR::AddDimension(std::string name, DataType type, double lowerBound,
                       double upperBound, int64_t current_upper_bound) {
  if (!validateSchemaElementName(name))
    throw runtime_error("Attempt to add dimension with invalid name in TAR.");

  if (!_subtars.empty())
    throw runtime_error("Attempt to add dimension in filled TAR.");

  if (_elements.size() >= MAX_NUM_OF_DIMS_IN_TAR)
    throw runtime_error("Attempt to extra dimension in a complete TAR.");

  DimensionPtr dimension =
    make_shared<Dimension>(UNSAVED_ID, name, type, lowerBound, upperBound, 1);

  dimension->CurrentUpperBound() = current_upper_bound;
  DataElementPtr dataElement = make_shared<DataElement>(dimension);
  _elements.push_back(dataElement);
}

void TAR::AddDimension(string name, DataType type, double lowerBound,
                       double upperBound, RealIndex realLower,
                       RealIndex realUpper, double spacing,
                       int64_t current_upper_bound) {

  if (!validateSchemaElementName(name))
    throw runtime_error("Attempt to add dimension with invalid name in TAR.");

  if (!_subtars.empty())
    throw runtime_error("Attempt to add dimension in filled TAR.");

  if (_elements.size() >= MAX_NUM_OF_DIMS_IN_TAR)
    throw runtime_error("Attempt to add extra dimension in a complete TAR.");

  DimensionPtr dimension = make_shared<Dimension>(
    UNSAVED_ID, name, type, lowerBound, upperBound, spacing);

  dimension->CurrentUpperBound() = current_upper_bound;

  DataElementPtr dataElement = make_shared<DataElement>(dimension);
  _elements.push_back(dataElement);
}

void TAR::AddDimension(string name, DataType type, double lowerBound,
                       double upperBound, RealIndex realLower,
                       RealIndex realUpper, int64_t current_upper_bound,
                       DatasetPtr dataset) {

  if (!validateSchemaElementName(name))
    throw runtime_error("Attempt to add dimension with invalid name in TAR.");

  if (!_subtars.empty())
    throw runtime_error("Attempt to add dimension in filled TAR.");

  if (_elements.size() >= MAX_NUM_OF_DATA_ELEMENTS)
    throw runtime_error("Attempt to extra dimension in a complete TAR.");

  DimensionPtr dimension = make_shared<Dimension>(UNSAVED_ID, name, dataset);
  dimension->CurrentUpperBound() = current_upper_bound;
  DataElementPtr dataElement = DataElementPtr(new DataElement(dimension));
  _elements.push_back(dataElement);
}

void TAR::AddDimension(DimensionPtr dimension) {
  if (dimension->GetDimensionType() == IMPLICIT) {
    return AddDimension(
      dimension->GetName(), dimension->GetType(), dimension->GetLowerBound(),
      dimension->GetUpperBound(), 0, dimension->GetRealUpperBound(),
      dimension->GetSpacing(), dimension->CurrentUpperBound());
  } else if (dimension->GetDimensionType() == EXPLICIT) {
    return AddDimension(
      dimension->GetName(), dimension->GetType(), dimension->GetLowerBound(),
      dimension->GetUpperBound(), 0, dimension->GetRealUpperBound(),
      dimension->CurrentUpperBound(), dimension->GetDataset());
  }
}

void TAR::AddAttribute(string name, DataType type) {

  if (!validateSchemaElementName(name))
    throw runtime_error("Attempt to add attribute with invalid name in TAR.");

  if (!_subtars.empty())
    throw runtime_error("Attempt to add attribute in filled TAR.");

  if (_elements.size() >= MAX_NUM_OF_DATA_ELEMENTS)
    throw runtime_error("Attempt to add extra attribute in a complete TAR.");

  AttributePtr attribute = make_shared<Attribute>(UNSAVED_ID, name, type);
  DataElementPtr dataElement = DataElementPtr(new DataElement(attribute));
  _elements.push_back(dataElement);
}

void TAR::AddAttribute(AttributePtr attribute) {
  AddAttribute(attribute->GetName(), attribute->GetType());
}

void TAR::AddSubtar(SubtarPtr subtar) {
  validadeSubtar(subtar);

  int32_t dimensionsNo = GetDimensions().size();
  int64_t min[dimensionsNo], max[dimensionsNo];

  if (_subtarsIndex == nullptr) {
    _subtarsIndex = SubtarsIndex(new RTree<int64_t, int64_t>(dimensionsNo));
  }

  auto intersectionSubtars = GetIntersectingSubtars(subtar);
  if (!intersectionSubtars.empty())
    throw std::runtime_error(
      "Attempt to add subTAR that intersects with other subTARs.");

  auto lenBeforeAddingSubtar = GetSpannedTARLen();

  // Updating current upper bounds
  for (auto entry : subtar->GetDimSpecs()) {
    DimensionPtr dim = GetDataElement(entry.first)->GetDimension();
    DimSpecPtr dimSpecs = entry.second;
    if (dim->CurrentUpperBound() < dimSpecs->GetUpperBound()) {
      dim->CurrentUpperBound() = dimSpecs->GetLowerBound();
    }
  }

  // If the span is less than it was before, an overflow happened.
  if (GetSpannedTARLen() < lenBeforeAddingSubtar) {
    throw std::runtime_error("Attempt to add subTAR that makes the spanned "
                             "length of the TAR too big.");
  }

  // Registering subtar
  subtar->CreateBoundingBox(min, max, dimensionsNo);
  _subtarsIndex->Insert(min, max, _subtars.size());
  _subtars.push_back(subtar);
}

const int32_t TAR::GetId() { return _id; }

const std::string TAR::GetName() { return _name; }

TypePtr TAR::GetType() { return _type; }

void TAR::AlterTAR(int32_t newId, string newName) {
  if (validateTARName(newName)) {
    _id = newId;
    _name = newName;
  } else {
    throw std::runtime_error("Attempt to alter TAR with invalid name.");
  }
}

void TAR::AlterType(TypePtr type) {
  _type = type;
  _roles.clear();
}

int32_t TAR::GetTypeId() { return _idType; }

void TAR::SetTypeId(int32_t idType) { _idType = idType; }

vector<SubtarPtr> &TAR::GetSubtars() { return _subtars; }

vector<SubtarPtr> TAR::GetIntersectingSubtars(SubtarPtr subtar) {
  int32_t dimensionsNo = GetDimensions().size();
  int64_t min[dimensionsNo], max[dimensionsNo];
  vector<SubtarPtr> intersectingSubtars;

  TAR::_mutex.lock();
  if (_subtarsIndex != nullptr) {
    subtar->CreateBoundingBox(min, max, dimensionsNo);
    _subtarsIndex->Search(min, max, TAR::_callback, nullptr);
  }

  for (int64_t index : TAR::_intersectingSubtarsIndexes) {
    intersectingSubtars.push_back(_subtars[index]);
  }

  TAR::_intersectingSubtarsIndexes.clear();
  _mutex.unlock();

  return intersectingSubtars;
}

vector<int32_t> &TAR::GetIdSubtars() { return _idSubtars; }

bool TAR::HasDataElement(std::string name) {
  int32_t selected = 0;
  auto splittedName = split(name, INDEX_SEPARATOR);
  name = splittedName[0];

  if (splittedName.size() > 1) {
    selected = strtol(splittedName[1].c_str(), nullptr, 10);
  } else if (splittedName.size() > 2) {
    return false;
  }

  for (auto &dataElement : _elements) {
    if (!dataElement->GetName().compare(name)) {

      if (selected != 0 &&
        (selected + 1) > dataElement->GetDataType().vectorLength()) {
        return false;
      }

      return true;
    }
  }

  return false;
}

DataElementPtr TAR::GetDataElement(string name) {
  auto splittedName = split(name, INDEX_SEPARATOR);
  name = splittedName[0];

  for (auto &dataElement : _elements) {
    if (!dataElement->GetName().compare(splittedName[0].c_str())) {
      if (splittedName.size() > 1) {
        int32_t selected = strtol(splittedName[1].c_str(), nullptr, 10);

        if (dataElement->GetDataType().isVector())
          if (selected < dataElement->GetDataType().vectorLength()) {
            AttributePtr originalAttrib = dataElement->GetAttribute();

            auto attName = originalAttrib->GetName();
            auto attId = originalAttrib->GetId();
            auto type =
              DataType(originalAttrib->GetType().type(),
                       originalAttrib->GetType().vectorLength(), selected);

            AttributePtr att = make_shared<Attribute>(attId, attName, type);

            DataElementPtr cloneDE = DataElementPtr(new DataElement(att));
            return cloneDE;
          } else {
            return nullptr;
          }
      }
      return dataElement;
    }
  }
  return nullptr;
}

std::list<DataElementPtr> &TAR::GetDataElements() { return _elements; }

void TAR::RemoveDataElement(std::string name) {
  for (auto dataElement : _elements) {
    if (!dataElement->GetName().compare(name)) {
      _elements.remove(dataElement);
      return;
    }
  }
}

std::string TAR::toSmallString() {
  std::string smallString;
  for (auto &dataElement : _elements) {
    switch (dataElement->GetType()) {
    case DIMENSION_SCHEMA_ELEMENT:
      smallString += "|" + dataElement->GetDimension()->GetName() + "," + "d," +
        dataElement->GetDimension()->GetType().toString();
      break;

    case ATTRIBUTE_SCHEMA_ELEMENT:
      smallString +=
        "|" + dataElement->GetAttribute()->GetName() + "," + "a," +
          dataElement->GetAttribute()->GetType().toString() + _COLON +
          to_string(dataElement->GetAttribute()->GetType().vectorLength());
      break;
    }
  }

  std::string smallStringAux = smallString.replace(0, 1, "#");
  return smallString;
}

std::string TAR::toString() {

  std::string dimensions;
  std::string attributes;
  std::string typeName(_ASTERISK);

  for (auto &dataElement : _elements) {

    switch (dataElement->GetType()) {
    case DIMENSION_SCHEMA_ELEMENT: {
      std::stringstream lower, upper, spacing, rlower, rupper, current;
      lower << std::setprecision(6)
            << dataElement->GetDimension()->GetLowerBound();
      upper << std::setprecision(6)
            << dataElement->GetDimension()->GetUpperBound();
      spacing << std::setprecision(6)
              << dataElement->GetDimension()->GetSpacing();
      rlower << std::setprecision(6) << 0;
      rupper << std::setprecision(6)
             << dataElement->GetDimension()->GetRealUpperBound();
      current << std::setprecision(6)
              << dataElement->GetDimension()->CurrentUpperBound();

      dimensions += "    " + dataElement->GetDimension()->GetName() + "[" +
        dataElement->GetDimension()->GetType().toString() +
        "]   from " + rlower.str() + "(" + lower.str() + ") to " +
        current.str() + " of " + rupper.str() + "(" + upper.str() +
        ") with spacing " + spacing.str() + "\n";
      break;
    }
    case ATTRIBUTE_SCHEMA_ELEMENT: {
      attributes = "    " + dataElement->GetAttribute()->GetName() + "[" +
        dataElement->GetAttribute()->GetType().toString() + "]\n";
      break;
    }
    }
  }

  if (_type != nullptr)
    typeName = _type->name;

  return _name + "[" + typeName + "]" + "\n  Dimensions:\n" + dimensions +
    "\n  Attributes:\n" + attributes;
}

void TAR::RemoveTempDataElements() {
  std::list<DataElementPtr> toRemove;

  for (auto &dataElement : _elements) {
    if (dataElement->GetName().compare(0, 4, DEFAULT_TEMP_MEMBER) == 0)
      toRemove.push_back(dataElement);
  }

  for (auto dataElement : toRemove) {
    _elements.remove(dataElement);
  }
}

void TAR::RecalculatesRealBoundaries() {
  throw runtime_error("Unsupported RecalculatesRealBoundaries operation.");
}

bool TAR::CheckMaxSpan() {
  auto dimensions = GetDimensions();
  savime_size_t spannedLen = dimensions.empty() ? 0 : 1;
  for (auto dim : dimensions) {
    auto beforeMultiplying = spannedLen;
    spannedLen *= dim->GetCurrentLength();

    /*if value overflows*/
    if (spannedLen < beforeMultiplying)
      return true;
  }
  return false;
}

savime_size_t TAR::GetSpannedTARLen() {
  auto dimensions = GetDimensions();
  savime_size_t spannedLen = dimensions.empty() ? 0 : 1;
  for (auto dim : dimensions) {
    spannedLen *= dim->GetCurrentLength();
  }
  return spannedLen;
}

savime_size_t TAR::GetFilledTARLen() {
  auto subtars = GetSubtars();
  savime_size_t filledLen = 0;
  for (auto subtar : subtars) {
    filledLen += subtar->GetFilledLength();
  }
  return filledLen;
}

savime_size_t TAR::GetTotalTARLen() {
  auto dimensions = GetDimensions();
  savime_size_t totalLen = dimensions.empty() ? 0 : 1;
  for (auto dim : dimensions) {
    totalLen *= dim->GetLength();
  }
  return totalLen;
}

TARPtr TAR::Clone(bool copyId, bool copySubtars, bool dimensionsOnly) {
  TARPtr clonedTAR = TARPtr(new TAR(0, "", nullptr));

  if (copyId) {
    clonedTAR->AlterTAR(_id, _name);
  }

  for (auto &dataElement : _elements) {
    if (dataElement->GetType() == DIMENSION_SCHEMA_ELEMENT) {
      clonedTAR->AddDimension(dataElement->GetDimension());
    } else if (dataElement->GetType() == ATTRIBUTE_SCHEMA_ELEMENT &&
      !dimensionsOnly) {
      clonedTAR->AddAttribute(dataElement->GetAttribute());
    }
  }

  if (copySubtars) {
    for (auto subtar : _subtars) {
      clonedTAR->AddSubtar(subtar);
    }
  }

  return clonedTAR;
}

TAR::~TAR() {
  for (auto listener : _listeners)
    listener->DisposeObject((MetadataObject *)this);
}