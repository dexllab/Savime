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

LoadSubtar::LoadSubtar(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                   QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                   StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult LoadSubtar::Run() {
  try {
    std::list<ParameterPtr> parameters = _operation->GetParameters();
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

    TARSPtr defaultTARS = _metadataManager->GetTARS(
      _configurationManager->GetIntValue(DEFAULT_TARS));
    TARPtr tar = _metadataManager->GetTARByName(defaultTARS, tarName);

    if (tar == nullptr)
      throw std::runtime_error(tarName + " does not exist.");

    if (tar->GetSubtars().size() >= MAX_NUM_SUBTARS_IN_TAR)
      throw std::runtime_error("Max number of subtars for " + tar->GetName() +
                               " reached.");

    std::list<DimSpecPtr> dimSpecs = create_dimensionsSpecs(
      dimSpecsBlock, tar, _metadataManager, _storageManager);

    SubtarPtr subtar = std::make_shared<Subtar>();
    subtar->SetTAR(tar);
    subtar->SetId(UNSAVED_ID);

    for (auto &dimSpec : dimSpecs) {
      subtar->AddDimensionsSpecification(dimSpec);
    }

    validate_subtar_size(subtar);

    for (auto &dimSpec : dimSpecs) {
      validate_dimensionSpecs(dimSpec, _storageManager);
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
          DatasetPtr ds = _metadataManager->GetDataSetByName(dsName);
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

    if (_metadataManager->SaveSubtar(tar, subtar) == SAVIME_FAILURE) {
      throw std::runtime_error("Could not insert subtar.");
    }

    updateCurrentUpperBounds(tar, subtar);
    _metadataManager->RegisterQuery(_queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}