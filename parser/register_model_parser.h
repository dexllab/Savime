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

#ifndef SAVIME_REGISTERMODELPARSER_H
#define SAVIME_REGISTERMODELPARSER_H

#include <default_parser.h>

class RegisterModelParser {
 public:
   RegisterModelParser(DefaultParser *defaultParser);
   OperationPtr parse(QueryExpressionPtr queryExpressionNode,
                         QueryPlanPtr queryPlan, int &idCounter);
 private:
   DefaultParser *_parser;
   const char *error_msg = "Invalid parameter for operator REGISTER_MODEL. Expected "
                              "REGISTER_MODEL(model_name, tar, attribute, [\"dim\"], \"path\")";
   TARPtr _inputTAR;
  IdentifierChainPtr parseIdentifierChain(ValueExpressionPtr value);
  TARPtr parseTAR(ValueExpressionPtr value, QueryPlanPtr queryPlan, int &idCounter);
  CharacterStringLiteralPtr parseString(ValueExpressionPtr value);
  void parseModelName(list<ValueExpressionPtr> *params, OperationPtr operation);
  void parseTarName(list<ValueExpressionPtr> *params, OperationPtr operation,
                                         QueryPlanPtr queryPlan, int &idCounter);
  void parseAttribute(list<ValueExpressionPtr> *params, OperationPtr operation,
                                           QueryPlanPtr queryPlan, int &idCounter);
  void parseDimensionString(list<ValueExpressionPtr> *params, OperationPtr shared_ptr);
  void parseModelDirectory(list<ValueExpressionPtr> *params, OperationPtr shared_ptr);
};

#endif //SAVIME_REGISTERMODELPARSER_H
