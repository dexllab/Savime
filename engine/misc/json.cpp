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
*    ANDERSON C. SILVA				OCTOBER 2019
*/

#include "engine/misc/include/json.h"
#include <string>
#include <vector>
#include <jsoncpp/json/value.h>
#include <jsoncpp/json/json.h>
#include <cstring>
#include "../core/include/util.h"

vector<string> jsonArrayToVector(const Json::Value JSonArray){
    double val;
    vector<string> literals;

    for (int i = 0; i < JSonArray.size(); ++i) {
        for (int j = 0; j < JSonArray[i].size(); ++j) {
            for (int k = 0; k < JSonArray[i][j].size(); ++k) {
                for (int l = 0; l < JSonArray[i][j][k].size(); ++l) {
                    val = JSonArray[i][j][k][l][0].asDouble();
                    literals.push_back(to_string(val));
                }
            }
        }
    }
    return literals;
}

char* jsonToChar(Json::Value JSonQuery){
    Json::StyledWriter styledWriter;
    std::string jsonData =  styledWriter.write(JSonQuery);
    char* charJSonData = new char[jsonData.length() + 1];
    strcpy(charJSonData, jsonData.c_str());
    return charJSonData;
}

void writeJsonFile(string filePath, Json::Value JSonQuery){
    std::ofstream fileId;
    fileId.open(filePath);
    Json::StyledWriter styledWriter;
    fileId << styledWriter.write(JSonQuery);
    fileId.close();
}