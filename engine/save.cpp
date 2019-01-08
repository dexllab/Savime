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

Save::Save(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                   QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                   StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult Save::Run() {
  string file = _operation->GetParametersByName(COMMAND)->literal_str;
  file = trim_delimiters(file);
  int32_t fd = open(file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd == -1) {
    throw std::runtime_error("Could not open file: " + file +
                             " Error: " + std::string(strerror(errno)));
  }

  for (const auto &query : _metadataManager->GetQueries()) {
    auto query_nl = query + _NEWLINE;
    write(fd, query_nl.c_str(), query_nl.size());
  }
  close(fd);

  return SAVIME_SUCCESS;
}
