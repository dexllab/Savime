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
#ifndef SUBSET_H
#define SUBSET_H

#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"
#include "../../core/include/query_data_manager.h"
#include "../../core/include/parser.h"
#include <unordered_map>

typedef enum {
  SUBSET_NONE,
  SUBSET_PARTIAL,
  SUBSET_TOTAL
} SubsetIntersectionType;
typedef std::unordered_map<string, string> FilteredDimMap;
typedef std::unordered_map<string, double> BoundMap;

#define SUBSET_GET_NEXT_UNTESTED(CURRENT)                                      \
  mutex.lock();                                                                \
  CURRENT = nextUntested;                                                      \
  nextUntested++;                                                              \
  mutex.unlock();

inline void subset_get_boundaries(OperationPtr operation,
                                  FilteredDimMap &filteredDim,
                                  BoundMap &lower_bounds,
                                  BoundMap &upper_bounds) {

  int32_t paramCount = 0;
  while (true) {
    auto param = operation->GetParametersByName(DIM(paramCount));
    auto lb = operation->GetParametersByName(LB(paramCount));
    auto ub = operation->GetParametersByName(UP(paramCount++));

    if (!param)
      break;

    filteredDim[param->literal_str] = param->literal_str;
    lower_bounds[param->literal_str] = lb->literal_dbl;
    upper_bounds[param->literal_str] = ub->literal_dbl;
  }
}

inline void check_intersection(
    StorageManagerPtr storageManager, SubtarPtr subtar, TARPtr outputTAR,
    TARPtr inputTAR, FilteredDimMap filteredDim, BoundMap lower_bounds,
    BoundMap upper_bounds, SubsetIntersectionType &currentSubtarIntersection,
    bool &isAllOrdered) {

  for (auto entry : subtar->GetDimSpecs()) {

    double specsLowerBound, specsUpperBound;
    auto dimName = entry.first;
    auto specs = entry.second;
    auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
    auto originalDim = inputTAR->GetDataElement(dimName)->GetDimension();

    if (entry.second->GetSpecsType() != ORDERED)
      isAllOrdered = false;
    if (filteredDim.find(dimName) == filteredDim.end())
      continue;
    double lb = lower_bounds[dimName];
    double ub = upper_bounds[dimName];

    auto lowerLogicalIndex =
        storageManager->Real2Logical(originalDim, specs->GetLowerBound());
    auto upperLogicalIndex =
        storageManager->Real2Logical(originalDim, specs->GetUpperBound());
    GET_LITERAL(specsLowerBound, lowerLogicalIndex, double);
    GET_LITERAL(specsUpperBound, upperLogicalIndex, double);

    bool lbInRange =
        IN_RANGE(specsLowerBound, dim->GetLowerBound(), dim->GetUpperBound());
    bool upInRange =
        IN_RANGE(specsUpperBound, dim->GetLowerBound(), dim->GetUpperBound());
    bool lowerThanRange = dim->GetUpperBound() < specsLowerBound;
    bool greaterThanRange = dim->GetLowerBound() > specsUpperBound;
    bool inBetween = dim->GetLowerBound() > dim->GetUpperBound();
    
    if (lbInRange && upInRange) {
      continue;
    } else if (lbInRange || upInRange ||
               (!lowerThanRange && !greaterThanRange && !inBetween)) {
      currentSubtarIntersection = SUBSET_PARTIAL;
    } else {
      currentSubtarIntersection = SUBSET_NONE;
      break;
    }
  }
}

#endif /* SUBSET_H */
