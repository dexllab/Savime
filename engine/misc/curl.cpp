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

#include <curl/curl.h>
#include <engine/misc/include/json.h>
#include <jsoncpp/json/json.h>
#include <cstring>
#include <fstream>
#include "engine/misc/include/curl.h"

#define ERROR_MSG(F, O)                                                        \
  "Error during " + std::string(F) + " execution in " + std::string(O) +       \
      " operator. Check the log file for more info."

// This struct will store the server results
struct MemoryStruct {
  char *memory;
  size_t size;
};

static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userdata) {
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *)userdata;
    char *ptr = (char *)realloc(mem->memory, mem->size + realsize + 1);
    if (ptr == NULL) {
        /* out of memory */
        std::cerr << "Not enough memory (realloc returned NULL)\n";
        return 0;
    }
    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;
    return realsize;
}


Json::Value sendJsonToUrl(Json::Value JSonQuery, string url) {
    CURL *curl;
    CURLcode res;

    char *json_data = jsonToChar(JSonQuery);

    struct MemoryStruct chunk;
    chunk.memory = (char *)malloc(1);
    chunk.size = 0;
    Json::Value results;
    Json::Value jsonPrediction;

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk); // this is how chunk gets passed to the callback

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
        }
        else {
            std::cout << (unsigned long)chunk.size << " bytes retrieved\n";
            results = (Json::Value) chunk.memory;
        }
        curl_easy_cleanup(curl);

        Json::Reader reader;
        Json::Value root;
        if(!reader.parse(chunk.memory, root)){
            std::cout << "Could not parse JSon data" << std::endl;
            throw std::runtime_error("Could not parse JSon data");
        }

        return root;
    } else {
        std::cout << "Could not initialize curl." << std::endl;
        throw std::runtime_error("Could not initialize curl.");
    }
}