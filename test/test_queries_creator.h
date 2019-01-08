
/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef SAVIME_TEST_STRINGS_H
#define SAVIME_TEST_STRINGS_H

#include <list>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <../core/include/util.h>
#include "mock_builder.h"

//extern std::list<std::string> SETUP_QUERIES
//extern std::list<std::list<std::string>> ONE_TAR_QUERIES
//extern std::list<std::list<std::string>> TWO_TAR_QUERIES

extern std::vector<std::string> TAR_NAMES;
extern std::vector<std::vector<std::string>> TAR_NAMES_PAIR;
extern std::list<std::string> SETUP_QUERIES;
extern std::list<std::list<std::string>> ONE_TAR_QUERIES;
extern std::list<std::list<std::string>> REDUCED_ONE_TAR_QUERIES_INNER;
extern std::list<std::list<std::string>> REDUCED_ONE_TAR_QUERIES_OUTER;
extern std::list<std::list<std::string>> TWO_TAR_QUERIES;
extern std::list<std::string> NESTED_TWO_TAR_QUERIES;
extern std::vector<std::string> TEST_QUERIES;
extern std::vector<std::string> TEST_QUERY_PLANS;
extern std::vector<std::string> TEST_ERROR_RESPONSES;
extern std::vector<std::map<std::string, std::vector<std::vector<uint8_t>>>> QUERY_RESULTS;
extern  std::vector<std::map<std::string, std::vector<std::vector<uint8_t>>>> QUERY_RESULTS_WITH_OPTIMIZER;

std::list<std::string> get_queries();
void parseQueries(QueryDataManagerPtr queryDataManager,
                   ParserPtr parser,
                   std::list<string>& queries,
                   std::list<string>& queryPlans,
                   std::list<string>& errorResponses);
void  get_parse_results();


#endif //SAVIME_TEST_STRINGS_H



