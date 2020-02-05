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

#include <string>
#include <iostream>
#include <jsoncpp/json/value.h>
#include <vector>

using namespace std;

//
// Created by anderson on 27/09/19.
//

#ifndef SAVIME_FILEREADER_H
#define SAVIME_FILEREADER_H

vector<string> jsonArrayToVector(const Json::Value JSonArray);
char* jsonToChar(Json::Value JSonQuery);
void writeJsonFile(string filePath, Json::Value JSonQuery);
std::vector<std::string> splitString(const std::string& s, const char& c);

#endif //SAVIME_FILEREADER_H
