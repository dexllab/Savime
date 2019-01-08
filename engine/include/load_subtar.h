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
#ifndef LOAD_SUBTAR_H
#define LOAD_SUBTAR_H
#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"

inline void create_bounds(RealIndex &lowerBound, RealIndex &upperBound,
                          std::string sLowerBound, std::string sUpperBound,
                          const DimensionPtr &dimension,
                          const StorageManagerPtr &storageManager) {

  Literal index;
  index.type = DOUBLE;

  if (sLowerBound.c_str()[0] == _LOGICAL_INDEX_SPECIFIER_MARK) {
    sLowerBound = sLowerBound.replace(0, 1, "");

    double val = strtod(sLowerBound.c_str(), nullptr);
    index.dbl = val;
    lowerBound = storageManager->Logical2Real(dimension, index);

    if (lowerBound == -1)
      throw std::runtime_error("Invalid logical index: " + sLowerBound);

  } else {
    //lowerBound = atoll(sLowerBound.c_str());
    lowerBound = strtoll(sLowerBound.c_str(), nullptr, 10);
  }

  if (sUpperBound.c_str()[0] == _LOGICAL_INDEX_SPECIFIER_MARK) {
    sUpperBound = sUpperBound.replace(0, 1, "");
    double val = strtod(sUpperBound.c_str(), nullptr);
    index.dbl = val;
    upperBound = storageManager->Logical2Real(dimension, index);

    if (upperBound == -1)
      throw std::runtime_error("Invalid logical index: " + sUpperBound);

  } else {
    //upperBound = atoll(sUpperBound.c_str());
    upperBound = strtoll(sUpperBound.c_str(), nullptr, 10);
  }
}

/* Auxiliary functions for load_subtar operator:
 *
 * DimensionSpecs can be either ORDERED, PARTIAL or TOTAL:
 *
 * ORDERED for and implicit dimension is the simplest possible case. Indexes
 * values can
 * be generated automatically, lower and upper bounds are actual index values.
 * ORDERED for explicit dimensions means that dimensions values require only a
 * look up in the dimension dataset,
 * but that they follow a well-behaved pattern. Lower and upper bounds can be
 * actual
 * index values (real indexes) or lookup values (logical indexes) using the
 * syntax: #value.
 *
 * PARTIAL means that the dimension is not conforming with original dimension
 * definition, having a particular mapping
 * of values that repeat themselves index into a value in the mapping. There is
 * a dataset that specifies positions.
 * For implicit and spaced dimensions, the partial mappings give the direct
 * dimension values (real indexes).
 * For explicit dimensions, the partial mapping gives lookup values (logical
 * indexes) into the dimensions dataset.
 *
 * TOTAL means that the dimension is not conforming with original dimension
 * definition, having a particular mapping of
 * values for the entire subtar. It means that values for dimensions can not be
 * derived, because they are not well-behaved,
 * instead they are explicitly stored.
 * For implicit dimensions, the full mappings gives the direct dimension values
 * (real indexes);
 * For explicit dimensions, the partial mapping gives lookup values (logical
 * indexes) into the dataset dimensions specification dataset.
 *
 * If one of the dimension specifications in a subtar is TOTAL, then all others
 * must also be TOTAL.
 * Users can use # before a logical index to convert it to a real one in subtar
 * loading command call.
 */
inline std::list<DimSpecPtr>
create_dimensionsSpecs(const std::string &dimSpecsStringBlock, const TARPtr &tar,
                       const MetadataManagerPtr& metadataManager,
                       const StorageManagerPtr& storageManager) {
  bool hasTotalDim = false;
  bool hasNonTotalDim = false;
  std::string dimensionName;
  std::list<DimSpecPtr> dimensionsSpecs;
  int64_t totalLen = 1;
  std::vector<std::string> dimSpecsBlocks =
    split(dimSpecsStringBlock, _PIPE[0]);

  for (const std::string &dimSpecsString : dimSpecsBlocks) {
    std::vector<std::string> params = split(dimSpecsString, ',');
    DimSpecPtr dimensionSpecification = nullptr;
    DatasetPtr dataset;
    RealIndex lowerBound, upperBound;
    auto id = UNSAVED_ID;
    auto type = STR2SPECTYPE(trim(params[0]).c_str());

    dimensionName = trim(params[1]);
    DataElementPtr dataElement = tar->GetDataElement(dimensionName);

    if (dataElement == nullptr)
      throw std::runtime_error("Invalid dimension name: " + dimensionName +
        ".");

    if (dataElement->GetType() != DIMENSION_SCHEMA_ELEMENT)
      throw std::runtime_error(dimensionName + " is not a dimension.");

    auto dimension = dataElement->GetDimension();
    std::string slowerBound = trim(params[2]);
    std::string supperBound = trim(params[3]);

    create_bounds(lowerBound, upperBound, slowerBound, supperBound, dimension,
                  storageManager);

    if (type == ORDERED) {
      hasNonTotalDim = true;
      if (params.size() < 4)
        throw std::runtime_error("Invalid dimension specification definition. "
                                 "Wrong number of parameters.");
    } else if (type == PARTIAL || type == TOTAL) {

      if (type == TOTAL)
        hasTotalDim = true;
      else
        hasNonTotalDim = true;

      if (params.size() < 5)
        throw std::runtime_error("Invalid dimension specification definition. "
                                 "Wrong number of parameters.");

      DatasetPtr ds = metadataManager->GetDataSetByName(trim(params[4]));
      if (ds == nullptr)
        throw std::runtime_error("Invalid dataset name: " + trim(params[4]));

      if (!ds->Sorted())
        storageManager->CheckSorted(ds);

      dataset = ds;

    } else {
      throw std::runtime_error("Invalid dimension specification type: " +
        params[0] + ".");
    }

    if (hasTotalDim && hasNonTotalDim)
      throw std::runtime_error("Invalid dimension specification for subtar: "
                               "Combination of total and non-total "
                               "specifications.");

    if (type == ORDERED) {
      dimensionSpecification = make_shared<DimensionSpecification>(
        id, dimension, lowerBound, upperBound, 1, 1);
    } else if (type == PARTIAL) {
      dimensionSpecification = make_shared<DimensionSpecification>(
        id, dimension, dataset, lowerBound, upperBound, 1, 1);
    } else if (type == TOTAL) {
      dimensionSpecification = make_shared<DimensionSpecification>(
        id, dimension, dataset, lowerBound, upperBound);
    }

    dimensionsSpecs.push_back(dimensionSpecification);
  }

  std::map<std::string, std::string> dimensionsInTar;
  for (const auto &d : tar->GetDimensions()) {
    dimensionsInTar[d->GetName()] = d->GetName();
  }

  for (const auto &spec : dimensionsSpecs) {
    if (dimensionsInTar.find(spec->GetDimension()->GetName()) !=
      dimensionsInTar.end()) {
      dimensionsInTar.erase(spec->GetDimension()->GetName());
    } else {
      throw std::runtime_error(
        "Duplicated specification definition for dimension " +
          spec->GetDimension()->GetName() + ".");
    }
  }

  if (!dimensionsInTar.empty()) {
    throw std::runtime_error(
      "Insufficient specification definition, there are dimensions in " +
        tar->GetName() + " without definition.");
  }

  for (const auto &spec : dimensionsSpecs) {
    totalLen *= spec->GetFilledLength();
  }

  ADJUST_SPECS(dimensionsSpecs);
  return dimensionsSpecs;
}

inline int validate_subtar_size(const SubtarPtr &subtar) {
  int64_t totalLength = subtar->GetFilledLength();

  for (auto entry : subtar->GetDimSpecs()) {
    if (entry.second->GetSpecsType() == TOTAL) {
      if (totalLength != entry.second->GetDataset()->GetEntryCount())
        throw std::runtime_error("There are dimension specifications of total "
                                 "type with invalid sizes: " +
          entry.second->GetDimension()->GetName() + ".");
    }
  }
}

inline int validate_dimensionSpecs(const DimSpecPtr &dimSpec,
                                   const StorageManagerPtr &storageManager) {
  auto ds = dimSpec->GetDataset();
  auto dimension = dimSpec->GetDimension();

  if (dimension->GetDimensionType() == IMPLICIT) {
    if (dimSpec->GetSpecsType() == ORDERED) {
      return SAVIME_SUCCESS;
    } else if (dimSpec->GetSpecsType() == PARTIAL ||
      dimSpec->GetSpecsType() == TOTAL) {
      DatasetPtr realIndexesDs;

      // Check if the dataset type is the same as dimensions type
      if (ds->GetType() != dimension->GetType())
        throw std::runtime_error(
          "Incompatible types for dimension specification and dimension " +
            dimension->GetName() + ".");

      // Check if a dataset entry_count is smaller than the dimspec length
      int64_t length = dimSpec->GetUpperBound() - dimSpec->GetLowerBound() + 1;
      if (dimSpec->GetSpecsType() == PARTIAL && length < ds->GetEntryCount())
        throw std::runtime_error(
          "Data set " + ds->GetName() +
            " is too large for dimension specification for dimension " +
            dimension->GetName() + ".");

      // Check if every value in the dataset is valid. Values must be logic
      // indexes that are mapped
      // to real indexes between dimspecs->lower_bound and dimspecs->upper_bound
      // and aligned.
      if (storageManager->Logical2Real(dimSpec->GetDimension(), dimSpec,
                                       dimSpec->GetDataset(),
                                       realIndexesDs) != SAVIME_SUCCESS)
        throw std::runtime_error(
          "Data set " + ds->GetName() +
            " contains invalid values for dimension specification.");
    }
  } else if (dimension->GetDimensionType() == EXPLICIT) {
    if (dimSpec->GetSpecsType() == ORDERED) {
      return SAVIME_SUCCESS;
    } else if (dimSpec->GetSpecsType() == PARTIAL ||
      dimSpec->GetSpecsType() == TOTAL) {
      DatasetPtr realIndexesDs;

      // Check if the dataset type is LONG typed
      if (ds->GetType() != INT64)
        throw std::runtime_error(
          "Incompatible types for dataset " + ds->GetName() +
            " dimension specification of explicit dimension " +
            dimension->GetName() + ". It must be long typed.");

      // Check if ds entry count is smaller than the dimspec length
      int64_t length = dimSpec->GetUpperBound() - dimSpec->GetLowerBound() + 1;
      if (dimSpec->GetSpecsType() == PARTIAL && length < ds->GetEntryCount())
        throw std::runtime_error(
          "Data set " + ds->GetName() +
            " is too large for dimension specification for dimension " +
            dimension->GetName() + ".");

      // Check if every value in the dataset is valid. Values must be real
      // indexes between Dimspecs->lower and Dimspecs->upper and aligned
      if (storageManager->Real2Logical(dimSpec->GetDimension(), dimSpec,
                                       dimSpec->GetDataset(),
                                       realIndexesDs) != SAVIME_SUCCESS)
        throw std::runtime_error(
          "Data set " + ds->GetName() +
            " contains invalid values for dimension specification.");
    }
  }
}

inline void updateCurrentUpperBounds(const TARPtr &tar, const SubtarPtr &subtar) {
  for (auto entry : subtar->GetDimSpecs()) {
    DimensionPtr dim = tar->GetDataElement(entry.first)->GetDimension();
    DimSpecPtr dimSpecs = entry.second;
    if (dim->CurrentUpperBound() < dimSpecs->GetUpperBound()) {
      dim->CurrentUpperBound() = dimSpecs->GetUpperBound();
    }
  }
}

#endif /* LOAD_SUBTAR_H */

