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

void push_back_values(Json::Value jsonValue,  vector<string> *literals)
{
    int i = 0;
    if( jsonValue.isArray() ){
        for (i = 0; i < jsonValue.size(); ++i) {
            push_back_values(jsonValue[i], literals);
        }
    }
    else{
        if (jsonValue.isDouble()) {
            double val = jsonValue.asDouble();
            literals->push_back(to_string(val));
        }
        else if (jsonValue.isInt()) {
            int val = jsonValue.asInt();
            literals->push_back(to_string(val));
        }
        else if (jsonValue.isInt64()) {
            long int val = jsonValue.asInt64();
            literals->push_back(to_string(val));
        }
        else if (jsonValue.isString())
        {
            string val = jsonValue.asString();
            literals->push_back(val);
        }
    }
}


vector<string> jsonArrayToVector(const Json::Value JSonArray){
    vector<string> literals;
    push_back_values(JSonArray, &literals);
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

std::vector<std::string> splitString(const std::string& s, const char& c)
{
    std::string buff{""};
    std::vector<std::string> v;

    for(auto n:s)
    {
        if(n != c) buff+=n; else
        if(n == c && !buff.empty()) { v.push_back(buff); buff = ""; }
    }
    if(buff.empty()) v.push_back(buff);

    return v;
}