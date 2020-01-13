//
// Created by anderson on 09/10/19.
//

#ifndef SAVIME_CURL_H
#define SAVIME_CURL_H
#include <jsoncpp/json/value.h>

std::size_t write_callback(char *in, size_t size, size_t nmemb, std::string *out);
Json::Value sendJsonToUrl(Json::Value JSonQuery, string url);

#endif //SAVIME_CURL_H
