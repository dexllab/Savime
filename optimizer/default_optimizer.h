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
#ifndef DEFAULT_OPTIMIZER_H
#define DEFAULT_OPTIMIZER_H

#include "../core/include/optimizer.h"


class DefaultOptimizer : public Optimizer {
public:
  DefaultOptimizer(ConfigurationManagerPtr configurationManager,
                   SystemLoggerPtr systemLogger)
      : Optimizer(configurationManager, systemLogger) {}

  void SetMetadaManager(MetadataManagerPtr metadaManager);
  SavimeResult Optimize(QueryDataManagerPtr queryDataManager);
};


#endif /* DEFAULT_OPTIMIZER_H */
