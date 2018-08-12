#ifndef FILTER_H
#define FILTER_H

#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"
#include "dml_operators.h"

inline void applyFilter(const SubtarPtr& subtar,
                        SubtarPtr &newSubtar,
                        const DatasetPtr& filterDs,
                        const StorageManagerPtr& storageManager) {

  savime_size_t totalLength = subtar->GetFilledLength();

  for (auto entry : subtar->GetDimSpecs()) {

    DimSpecPtr newDimSpec;
    DatasetPtr matDim, realDim;
    DimensionPtr dimension = entry.second->GetDimension();
    RealIndex lowerBound = entry.second->GetLowerBound();
    RealIndex upperBound = entry.second->GetUpperBound();

    if (storageManager->PartiatMaterializeDim(filterDs, entry.second,
                                              totalLength, matDim,
                                              realDim) != SAVIME_SUCCESS)
      throw std::runtime_error(
          ERROR_MSG("PartialMaterializeDim", "FILTER"));

    if (dimension->GetDimensionType() == EXPLICIT) {
      newDimSpec = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                       dimension,
                                                       realDim,
                                                       lowerBound,
                                                       upperBound);
      newDimSpec->GetMaterialized() = matDim;
    } else {
      newDimSpec = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                       dimension,
                                                       matDim,
                                                       lowerBound,
                                                       upperBound);
    }

    newSubtar->AddDimensionsSpecification(newDimSpec);
  }

  for (auto entry : subtar->GetDataSets()) {
    DatasetPtr dataset;
    if (storageManager->Filter(entry.second, filterDs, dataset) !=
        SAVIME_SUCCESS)
      throw std::runtime_error(ERROR_MSG("Filter", "FILTER"));

    newSubtar->AddDataSet(entry.first, dataset);
  }
}

#endif /* FILTER_H */

