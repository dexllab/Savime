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
#ifndef DEFAULT_CONFIG_MANAGER_H
#define DEFAULT_CONFIG_MANAGER_H

#include <unordered_map>
#include "../core/include/config_manager.h"

using namespace std;

class DefaultConfigurationManager : public ConfigurationManager
{  
  unordered_map<std::string, string> _strKeys;
  unordered_map<std::string, int32_t> _intKeys;
  unordered_map<std::string, int64_t> _longKeys;
  unordered_map<std::string, double> _doubleKeys;
  unordered_map<std::string, bool> _boolKeys;

public:
    
  DefaultConfigurationManager();
  bool GetBooleanValue(string key) override;
  void  SetBooleanValue(string key, bool value) override;
  string GetStringValue(string key) override;
  void  SetStringValue(string key, string value) override;
  int32_t  GetIntValue(string key) override;
  void  SetIntValue(string key, int32_t value) override;
  int64_t GetLongValue(string key) override;
  void SetLongValue(string key, int64_t) override;
  double GetDoubleValue(string key) override;
  void  SetDoubleValue(string key, double value) override;
  void LoadConfigFile(string file) override;
};

#endif /* DEFAULT_CONFIG_MANAGER_H */

