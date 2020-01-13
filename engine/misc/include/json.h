#include <string>
#include <iostream>
#include <jsoncpp/json/value.h>


using namespace std;

//
// Created by anderson on 27/09/19.
//

#ifndef SAVIME_FILEREADER_H
#define SAVIME_FILEREADER_H

vector<string> jsonArrayToVector(const Json::Value JSonArray);
char* jsonToChar(Json::Value JSonQuery);
void writeJsonFile(string filePath, Json::Value JSonQuery);

#endif //SAVIME_FILEREADER_H
