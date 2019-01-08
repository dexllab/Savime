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
#include "test_utils.h"

void InitBitmask(DatasetPtr dataset, vector<bool> bits) {
  for (int64_t i = 0; i < bits.size(); i++) {
    (*dataset->BitMask())[i] = bits[i];
  }
}

bool DatasetComparator::Compare(DatasetPtr ds1, DatasetPtr ds2) {
  if (ds1->GetType() == INT32 && ds2->GetType() == INT32) {
    TemplateDatasetComparator<int32_t, int32_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT32 && ds2->GetType() == INT64) {
    TemplateDatasetComparator<int32_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT32 && ds2->GetType() == FLOAT) {
    TemplateDatasetComparator<int32_t, float> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT32 && ds2->GetType() == DOUBLE) {
    TemplateDatasetComparator<int32_t, double> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT64 && ds2->GetType() == INT32) {
    TemplateDatasetComparator<int64_t, int32_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT64 && ds2->GetType() == INT64) {
    TemplateDatasetComparator<int64_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT64 && ds2->GetType() == FLOAT) {
    TemplateDatasetComparator<int64_t, float> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT64 && ds2->GetType() == DOUBLE) {
    TemplateDatasetComparator<int64_t, double> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == FLOAT && ds2->GetType() == INT32) {
    TemplateDatasetComparator<float, int32_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == FLOAT && ds2->GetType() == INT64) {
    TemplateDatasetComparator<float, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == FLOAT && ds2->GetType() == FLOAT) {
    TemplateDatasetComparator<float, float> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == FLOAT && ds2->GetType() == DOUBLE) {
    TemplateDatasetComparator<float, double> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == DOUBLE && ds2->GetType() == INT32) {
    TemplateDatasetComparator<double, int32_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == DOUBLE && ds2->GetType() == INT64) {
    TemplateDatasetComparator<double, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == DOUBLE && ds2->GetType() == FLOAT) {
    TemplateDatasetComparator<double, float> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == DOUBLE && ds2->GetType() == DOUBLE) {
    TemplateDatasetComparator<double, double> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == REAL_INDEX && ds2->GetType() == INT32) {
    TemplateDatasetComparator<int64_t, int32_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == REAL_INDEX && ds2->GetType() == INT64) {
    TemplateDatasetComparator<int64_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == REAL_INDEX && ds2->GetType() == FLOAT) {
    TemplateDatasetComparator<int64_t, float> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == REAL_INDEX && ds2->GetType() == DOUBLE) {
    TemplateDatasetComparator<int64_t, double> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT32 && ds2->GetType() == REAL_INDEX) {
    TemplateDatasetComparator<int32_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == INT64 && ds2->GetType() == REAL_INDEX) {
    TemplateDatasetComparator<int64_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == FLOAT && ds2->GetType() == REAL_INDEX) {
    TemplateDatasetComparator<float, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if (ds1->GetType() == DOUBLE && ds2->GetType() == REAL_INDEX) {
    TemplateDatasetComparator<double, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else if ((ds1->GetType() == SUBTAR_POSITION || ds1->GetType() == INT64) &&
      (ds2->GetType() == SUBTAR_POSITION || ds2->GetType() == INT64)) {
    TemplateDatasetComparator<int64_t, int64_t> comp(_storageManager);
    return comp.Compare(ds1, ds2);
  } else {
    throw runtime_error("Attempt to compare datasets with unsupported types.");
  }
}

void DatasetComparator::LogBitmask(DatasetPtr ds1, DatasetPtr ds2) {
  std::stringstream ss;
  ss << string("Different dataset bitmasks:\n");
  for (int64_t i = 0; i < ds1->GetEntryCount(); i++) {
    ss << i << "|\t" << (*ds1->BitMask())[i] << "|\t" << (*ds2->BitMask())[i]
       << "\n";
  }
  WARN("" << ss.str());
}

bool DatasetComparator::CompareBitmask(DatasetPtr ds1, DatasetPtr ds2) {

  if (ds1->BitMask()->size() != ds2->BitMask()->size())
    return false;

  for (int64_t i = 0; i < ds1->BitMask()->size(); i++) {
    if ((*ds1->BitMask())[i] != (*ds2->BitMask())[i]) {
      LogBitmask(ds1, ds2);
      return false;
    }
  }

  return true;
}

bool MetadataComparator::Compare(DimensionPtr dim1, DimensionPtr dim2,
                                 int64_t mask) {

  if (dim1->GetName() != dim2->GetName() && !CHECK_BIT(mask, 0)) {
    WARN("Dimensions are different (name):" << dim1->GetName() << " "
                                            << dim2->GetName());
    return false;
  }

  if (dim1->GetType() != dim2->GetType() && !CHECK_BIT(mask, 1)) {
    WARN("Dimensions are different (type):" << dim1->GetType().toString() << " "
                                            << dim2->GetType().toString());
    return false;
  }

  if (dim1->GetLowerBound() != dim2->GetLowerBound() && !CHECK_BIT(mask, 2)) {
    WARN("Dimensions are different (lower bound):"
             << dim1->GetLowerBound() << " " << dim2->GetLowerBound());
    return false;
  }

  if (dim1->GetUpperBound() != dim2->GetUpperBound() && !CHECK_BIT(mask, 3)) {
    WARN("Dimensions are different (upper bound):"
             << dim1->GetUpperBound() << " " << dim2->GetUpperBound());
    return false;
  }

  if (dim1->GetRealUpperBound() != dim2->GetRealUpperBound() &&
      !CHECK_BIT(mask, 4)) {
    WARN("Dimensions are different (real upper bound):"
             << dim1->GetRealUpperBound() << " " << dim2->GetRealUpperBound());
    return false;
  }

  if (dim1->CurrentUpperBound() != dim2->CurrentUpperBound() &&
      !CHECK_BIT(mask, 5)) {
    WARN("Dimensions are different (current upper bound):"
             << dim1->CurrentUpperBound() << " " << dim2->CurrentUpperBound());
    return false;
  }

  if (dim1->GetSpacing() != dim2->GetSpacing() && !CHECK_BIT(mask, 6)) {
    WARN("Dimensions are different (spacing):" << dim1->GetSpacing() << " "
                                               << dim2->GetSpacing());
    return false;
  }

  if (dim1->GetDimensionType() != dim2->GetDimensionType() &&
      !CHECK_BIT(mask, 7)) {
    WARN("Dimensions are different (dimension type):"
             << dim1->GetDimensionType() << " " << dim2->GetDimensionType());
    return false;
  }

  if (!CHECK_BIT(mask, 8) && dim1->GetDimensionType() == EXPLICIT) {
    bool equalDs =
        _datasetComparator.Compare(dim1->GetDataset(), dim2->GetDataset());
    if (!equalDs) {
      WARN("Dimensions are different (non-matching datasets):");
      return false;
    }
  }

  return true;
}

bool MetadataComparator::Compare(AttributePtr att1, AttributePtr att2,
                                 int64_t mask) {

  if (att1->GetName() != att2->GetName() && !CHECK_BIT(mask, 0)) {
    WARN("Attributes are different (name):" << att1->GetName() << " "
                                            << att1->GetName());
    return false;
  }

  if (att1->GetType() != att1->GetType() && !CHECK_BIT(mask, 1)) {
    WARN("Attributes are different (type):" << att1->GetType().toString() << " "
                                            << att1->GetType().toString());
    return false;
  }

  return true;
}

bool MetadataComparator::Compare(DimSpecPtr dimSpec1, DimSpecPtr dimSpec2,
                                 int64_t mask) {

  if (dimSpec1->GetUpperBound() != dimSpec2->GetUpperBound() &&
      !CHECK_BIT(mask, 0)) {
    WARN("DimSpecs are different (upperbound):"
             << dimSpec1->GetUpperBound() << " " << dimSpec2->GetUpperBound());
    return false;
  }

  if (dimSpec1->GetLowerBound() != dimSpec2->GetLowerBound() &&
      !CHECK_BIT(mask, 1)) {
    WARN("DimSpecs are different (lowerbound):"
             << dimSpec1->GetLowerBound() << " " << dimSpec2->GetLowerBound());
    return false;
  }

  if (!Compare(dimSpec1->GetDimension(), dimSpec2->GetDimension(), 0) &&
      !CHECK_BIT(mask, 2)) {
    return false;
  }

  if (dimSpec1->GetSpecsType() != dimSpec2->GetSpecsType() &&
      !CHECK_BIT(mask, 3)) {
    WARN("DimSpecs are different (specstype):");
    return false;
  }

  if (dimSpec1->GetSpecsType() == ORDERED ||
      dimSpec1->GetSpecsType() == PARTIAL) {

    if (dimSpec1->GetAdjacency() != dimSpec2->GetAdjacency() &&
        !CHECK_BIT(mask, 4)) {
      WARN("DimSpecs are different (adj):" << dimSpec1->GetAdjacency() << " "
                                           << dimSpec2->GetAdjacency());
      return false;
    }

    if (dimSpec1->GetStride() != dimSpec2->GetStride() && !CHECK_BIT(mask, 5)) {
      WARN("DimSpecs are different (stride):" << dimSpec1->GetStride() << " "
                                              << dimSpec2->GetStride());
      return false;
    }
  }

  if (dimSpec1->GetSpecsType() == PARTIAL &&
      dimSpec1->GetSpecsType() == TOTAL) {
    DatasetComparator comparator(_storageManager);
    if (!CHECK_BIT(mask, 6) &&
        comparator.Compare(dimSpec1->GetDataset(), dimSpec2->GetDataset())) {
      WARN("DimSpecs are different (spcestype):");
      return false;
    }
  }

  return true;
}

bool MetadataComparator::Compare(SubtarPtr subtar1, SubtarPtr subtar2,
                                 int64_t mask) {

  if (!CHECK_BIT(mask, 0)) {
    for (const auto &entry : subtar1->GetDimSpecs()) {
      auto dimSpec1 = entry.second;
      auto dimSpec2 = subtar2->GetDimensionSpecificationFor(entry.first);

      if (dimSpec2 == nullptr) {
        WARN("Subtars are different (non matching dimspecs).");
        return false;
      }

      if (!Compare(dimSpec1, dimSpec2, 0)) {
        WARN("Subtars are different (non matching dimspecs).");
        return false;
      }
    }
  }

  if (!CHECK_BIT(mask, 1)) {
    for (const auto &entry : subtar1->GetDataSets()) {
      auto dataset1 = entry.second;
      auto dataset2 = subtar2->GetDataSetFor(entry.first);

      if (dataset2 == nullptr)
        return false;

      DatasetComparator comparator(_storageManager);
      if (!comparator.Compare(dataset1, dataset2)) {
        return false;
      }
    }
  }

  return true;
}

bool MetadataComparator::Compare(TypePtr type1, TypePtr type2, int64_t mask) {

  if((type1 == nullptr && type2 != nullptr) &&
      (type1 != nullptr && type2 == nullptr)) {
    WARN("Types are different (null vs not null type).");
    return false;
  }

  if(type1 == nullptr && type2 == nullptr)
    return true;

  if (type1->name != type2->name && !CHECK_BIT(mask, 0)) {
    WARN("Types are different (name):" << type1->name << " " << type2->name);
    return false;
  }

  if(!CHECK_BIT(mask, 1)) {
    for (const auto &role : type1->roles) {

      auto role1 = role.second;
      if (type2->roles.find(role.first) == type2->roles.end()) {
        WARN("Types are different (non matching roles).");
        return false;
      }

      auto role2 = type2->roles[role.first];

      if(role1->is_mandatory != role2->is_mandatory) {
        WARN("Types are different (non matching roles).");
        return false;
      }
    }
  }

  return true;
}

bool MetadataComparator::Compare(TARPtr tar1, TARPtr tar2, int64_t mask) {

  if(tar1 == nullptr && tar2 == nullptr) {
    return true;
  }

  if(tar1 == nullptr || tar2 == nullptr) {
    WARN("TARs are different (one of them is an invalid reference).");
    return false;
  }

  if (tar1->GetName() != tar2->GetName() && !CHECK_BIT(mask, 0)) {
    WARN("TARs are different (name):" << tar1->GetName() << " "
                                      << tar2->GetName());
    return false;
  }

  if (!Compare(tar1->GetType(), tar2->GetType(), 0) && !CHECK_BIT(mask, 1)) {
    WARN("TARs are different (types).");
    return false;
  }

  if (!CHECK_BIT(mask, 2)) {
    for (const auto &dim1 : tar1->GetDimensions()) {

      auto dim2 = tar2->GetDataElement(dim1->GetName());
      if (dim2 == nullptr || dim2->GetDimension() == nullptr) {
        WARN("TARs are different (non matching dimensions).");
        return false;
      }

      if (!Compare(dim1, dim2->GetDimension(), 0)) {
        WARN("TARs are different (non matching dimensions).");
        return false;
      }
    }
  }

  if (!CHECK_BIT(mask, 3)) {
    for (const auto &att1 : tar1->GetAttributes()) {

      auto att2 = tar2->GetDataElement(att1->GetName());
      if (att2 == nullptr || att2->GetAttribute() == nullptr) {
        WARN("TARs are different (non matching attributes).");
        return false;
      }

      if (!Compare(att1, att2->GetAttribute(), 0)) {
        WARN("TARs are different (non matching attributes).");
        return false;
      }
    }
  }

  return true;
}

bool MetadataComparator::Compare(TARSPtr tars1, TARSPtr tars2, int64_t mask) {
  if (!CHECK_BIT(mask, 0) && tars1->name != tars2->name) {
    WARN("TARSs are different (name):" << tars1->name << " "
                                       << tars1->name);
    return false;
  }
  return true;
}


bool QueryStrucuturesComparator::Compare(ParameterPtr p1, ParameterPtr p2) {

  if (p1->type != p2->type) {
    WARN("Parameters types are different.");
    return false;
  }

  if (p1->name != p2->name) {
    WARN("Parameters names are different.");
    return false;
  }

  switch(p1->type) {
    case TAR_PARAM :
      if(!_metadataComparator.Compare(p1->tar, p2->tar, 0)) {
        WARN("Parameters TARs are different." << p1->tar->toSmallString() << ' '
                                             << p2->tar->toSmallString()
                                            );
        return false;
      }
      break;
    case IDENTIFIER_PARAM :
      if(p1->literal_str != p2->literal_str) {
        WARN("Parameters TARs are different." << p1->literal_str << ' '
                                             << p2->literal_str
        );
        return false;
      }
      break;
    case LITERAL_FLOAT_PARAM :
      if(p1->literal_flt != p2->literal_flt) {
        WARN("Parameters float are different." << p1->literal_flt << ' '
                                             << p2->literal_flt
        );
        return false;
      }
      break;
    case LITERAL_DOUBLE_PARAM :
      if(p1->literal_dbl != p2->literal_dbl) {
        WARN("Parameters double are different." << p1->literal_dbl << ' '
                                             << p2->literal_dbl
        );
        return false;
      }
      break;
    case LITERAL_INT_PARAM :
      if(p1->literal_int != p2->literal_int) {
        WARN("Parameters int are different." << p1->literal_int << ' '
                                             << p2->literal_int
        );
        return false;
      }
      break;
    case LITERAL_LONG_PARAM :
      if(p1->literal_lng != p2->literal_lng) {
        WARN("Parameters long are different." << p1->literal_lng << ' '
                                             << p2->literal_lng
        );
        return false;
      }
      break;
    case LITERAL_STRING_PARAM :
      if(p1->literal_str != p2->literal_str) {
        WARN("Parameters string are different." << p1->literal_lng << ' '
                                             << p2->literal_lng
        );
        return false;
      }
      break;
    case LITERAL_BOOLEAN_PARAM :
      if(p1->literal_bool != p2->literal_bool) {
        WARN("Parameters boolean are different." << p1->literal_bool << ' '
                                                 << p2->literal_bool
        );
        return false;
      }
    break;
  }

  return true;
}

bool QueryStrucuturesComparator::Compare(OperationPtr op1, OperationPtr op2) {

  if (op1->GetName() != op2->GetName()) {
    WARN("Operations are different (name):" << op1->GetName() << " "
                                            << op2->GetName());
    return false;
  }

  if (op1->GetOperation() != op2->GetOperation()) {
    WARN("Operations are different (opcode):" << op1->GetOperation() << " "
                                              << op2->GetOperation());
    return false;
  }

  auto tar1 = op1->GetResultingTAR();
  auto tar2 = op2->GetResultingTAR();

  if (!_metadataComparator.Compare(tar1, tar2, 0)) {
    WARN("Operations are different (resulting tars):" << tar1->toSmallString()
                                                      << tar2->toSmallString());
    return false;
  }

  auto params1 = op1->GetParameters();
  auto params2 = op2->GetParameters();

  for (auto i1 = params1.begin(), i2 = params2.begin();
       (i1 != params1.end()) && (i2 != params2.end());
       ++i1, ++i2) {
    if(!Compare(*i1, *i2)) {
      WARN("Query plans are different (different operations).");
      return false;
    }
  }


  return true;
}

bool QueryStrucuturesComparator::Compare(QueryPlanPtr plan1,
                                         QueryPlanPtr plan2,
                                         int64_t mask) {
  if (!CHECK_BIT(mask, 0)) {
    if (plan1->GetType() != plan2->GetType()) {
      WARN("Query plans are different (non matching types).");
      return false;
    }
  }

  if (!CHECK_BIT(mask, 1)) {
    auto operations1 = plan1->GetOperations();
    auto operations2 = plan2->GetOperations();

    if(operations1.size() != operations2.size()) {
      WARN("Query plans are different (different number of operations).");
      return false;
    }

    for (auto i1 = operations1.begin(), i2 = operations2.begin();
          (i1 != operations1.end()) && (i2 != operations2.end());
          ++i1, ++i2) {
      if(!Compare(*i1, *i2)) {
        WARN("Query plans are different (different operations).");
        return false;
      }
    }
  }

  return true;
}