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
*    ANDERSON C. SILVA				JANUARY 2020
*/

#include "model_configuration_manager.h"
#include <fstream>
#include <core/include/util.h>

ModelConfigurationManager::ModelConfigurationManager(){

}

bool ModelConfigurationManager::GetBooleanValue(std::string key) {
    if (_boolKeys.find(key) != _boolKeys.end())
        return _boolKeys[key];
    else
        return false;
}

void ModelConfigurationManager::SetBooleanValue(std::string key, bool value) {
    _boolKeys[key] = value;
}

std::string ModelConfigurationManager::GetStringValue(std::string key) {
    if (_strKeys.find(key) != _strKeys.end())
        return _strKeys[key];
    else
        return "";
}

void ModelConfigurationManager::SetStringValue(std::string key,
                                                 std::string value) {
    _strKeys[key] = value;
}

int32_t ModelConfigurationManager::GetIntValue(std::string key) {
    if (_intKeys.find(key) != _intKeys.end())
        return _intKeys[key];
    else
        return 0;
}

void ModelConfigurationManager::SetIntValue(std::string key, int32_t value) {
    _intKeys[key] = value;
}

int64_t ModelConfigurationManager::GetLongValue(std::string key) {
    if (_longKeys.find(key) != _longKeys.end())
        return _longKeys[key];
    else
        return 0;
}

void ModelConfigurationManager::SetLongValue(std::string key, int64_t value) {
    _longKeys[key] = value;
}

double ModelConfigurationManager::GetDoubleValue(std::string key) {
    if (_doubleKeys.find(key) != _doubleKeys.end())
        return _doubleKeys[key];
    else
        return 0.0;
}

void ModelConfigurationManager::SetDoubleValue(std::string key,
                                                 double value) {
    _doubleKeys[key] = value;
}

void ModelConfigurationManager::LoadConfigFile(string file) {
    string line;
    ifstream infile(file);

    while (getline(infile, line)) {
        line = trim(line);
        if (line.at(0) == '#')
            continue;
        auto splitLine = split(line, ' ');
        if (splitLine.size() != 3)
            continue;

        string key, svalue, type;
        key = trim(splitLine[0]);
        key.erase(std::remove(key.begin(), key.end(), '"'), key.end());
        type = trim(splitLine[1]);
        svalue = trim(splitLine[2]);
        svalue.erase(std::remove(svalue.begin(), svalue.end(), '"'), svalue.end());

        if (type == "s" || type == "S") {
            SetStringValue(key, svalue);
        } else if (type == "i" || type == "I") {
            int32_t val = (int32_t) strtol(svalue.c_str(), nullptr, 10);
            SetIntValue(key, val);
        } else if (type == "l" || type == "L") {
            //int64_t val = atoll(svalue.c_str());
            int64_t val = strtoll(svalue.c_str(), nullptr, 10);
            SetLongValue(key, val);
        } else if (type == "b" || type == "B") {
            bool val = (bool)strtol(svalue.c_str(), nullptr, 10);
            SetBooleanValue(key, val);
        } else if (type == "d" || type == "D") {
            double val = strtod(svalue.c_str(), nullptr);
            SetDoubleValue(key, val);
        } else {
            continue;
        }
    }
}

void ModelConfigurationManager::SaveConfigFile(string filePath) {
    ofstream outputFile(filePath);

    for (auto entry : _strKeys){
        outputFile << entry.first + " s " + entry.second + "\n";
    }

    for (auto entry : _intKeys){
        outputFile << entry.first + " i " + std::to_string(entry.second) + "\n";
    }

    for (auto entry : _doubleKeys){
        outputFile << entry.first + " d " + std::to_string(entry.second) + "\n";
    }

    for (auto entry : _longKeys){
        outputFile << entry.first + " l " + std::to_string(entry.second) + "\n";
    }

    for (auto entry : _boolKeys){
        outputFile << entry.first + " b " + std::to_string(entry.second) + "\n";
    }

    outputFile.close();
}