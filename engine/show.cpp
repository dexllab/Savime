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

Show::Show(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                   QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                   StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult Show::Run() {
  TARSPtr defaultTARS =
    _metadataManager->GetTARS(_configurationManager->GetIntValue(DEFAULT_TARS));

  try {
    if (defaultTARS == nullptr)
      throw std::runtime_error("Invalid default TARS configuration!");

    std::string dumpText = defaultTARS->name + " TARS:" + _NEWLINE + _NEWLINE;

    for (auto &tar : _metadataManager->GetTARs(defaultTARS)) {
      int64_t subtarCount = 0;
      dumpText += tar->toString() + _NEWLINE + "  Subtars:" + _NEWLINE;

      for (auto &subtar : _metadataManager->GetSubtars(tar)) {
        dumpText += "    subtar #" + to_string(subtarCount++) + _SPACE +
                    subtar->toString() + _NEWLINE;
      }

      dumpText += _NEWLINE;
    }
    _queryDataManager->SetQueryResponseText(dumpText);
  } catch (std::exception &e) {
    _queryDataManager->SetErrorResponseText(e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}
