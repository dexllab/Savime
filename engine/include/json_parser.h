//
// Created by anderson on 02/10/19.
//

#ifndef SAVIME_JSONPARSER_H
#define SAVIME_JSONPARSER_H
#include <iostream>
#include <string>

#include <curl/curl.h>
#include <jsoncpp/json/json.h>
#include <jsoncpp/json/reader.h>
#include <jsoncpp/json/writer.h>
#include <jsoncpp/json/value.h>

class JSonParser{
 public:
  JSonParser(const std::string &s): url(s){}
  JSonParser(){}
  void request();
  std::string get_json_string();
  Json::Value get_json(){ return root; }
 private:
  std::string url;
  Json::Value root;

  static std::size_t write_callback(char* in, std::size_t size, std::size_t nmemb, std::string* out);
};

std::string JSonParser::get_json_string(){
    Json::FastWriter fastWriter;
    return fastWriter.write(root);
}

std::size_t JSonParser::write_callback(char *in, size_t size, size_t nmemb, std::string *out){
    std::size_t total_size = size * nmemb;
    if(total_size){
        out->append(in, total_size);
        return total_size;
    }
    return 0;
}

void JSonParser::request(){
    // Parse raw Json string
    std::string str_buffer;
    CURL *curl = curl_easy_init();
    if(curl){
        CURLcode res;
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &str_buffer);
        res = curl_easy_perform(curl);
        if(res != CURLE_OK){
            std::cerr << "curl_easy_perform() failed:" << curl_easy_strerror(res) << std::endl;
            return;
        }
        curl_easy_cleanup(curl);
    }else{
        std::cout << "Could not initialize curl" << std::endl;
    }

    // Convert string to Json::Value
    Json::Reader reader;
    bool parse_status = reader.parse(str_buffer.c_str(), root);
    if (!parse_status){
        std::cerr  << "parse() failed: " << reader.getFormattedErrorMessages() << std::endl;
        return;
    }
}

#endif //SAVIME_JSONPARSER_H
