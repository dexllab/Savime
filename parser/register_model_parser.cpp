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

#include "register_model_parser.h"

RegisterModelParser::RegisterModelParser(DefaultParser *parser) {
    this->_parser = parser;
}

OperationPtr RegisterModelParser::parse(QueryExpressionPtr queryExpressionNode,
                                        QueryPlanPtr queryPlan,
                                        int &idCounter) {
#define EXPECTED_REGISTER_MODEL_PARAMS_NUM 5
    QueryExpressionPtr queryExpression;

    IdentifierChainPtr identifierChain;
    UnsignedNumericLiteralPtr unsignedNumericLiteral;
    SignedNumericLiteralPtr signedNumericLiteral;
    CharacterStringLiteralPtr characterString, filler;

    OperationPtr operation = std::make_shared<Operation>(TAL_REGISTER_MODEL);
    operation->SetResultingTAR(nullptr);

    list<ValueExpressionPtr> params =
        queryExpressionNode->_value_expression_list->ParamsToList();

    if (params.size() == EXPECTED_REGISTER_MODEL_PARAMS_NUM) {
        this->parseModelName(&params, operation);
        this->parseTarName(&params, operation, queryPlan, idCounter);
        this->parseAttribute(&params, operation, queryPlan, idCounter);
        this->parseDimensionString(&params, operation);
        this->parseModelDirectory(&params, operation);
    } else {
        throw std::runtime_error(this->error_msg);
    }
    return operation;
}

void RegisterModelParser::parseModelName(list<ValueExpressionPtr> *params, OperationPtr operation){
    auto identifierChain = this->parseIdentifierChain(params->front());
    operation->AddParam("model_name", identifierChain->getIdentifier()->_identifierBody);
    params->pop_front();
}

void RegisterModelParser::parseTarName(list<ValueExpressionPtr> *params, OperationPtr operation,
                                         QueryPlanPtr queryPlan, int &idCounter) {
    auto identifierChain = this->parseIdentifierChain(params->front());
    string identifier = identifierChain->getIdentifier()->_identifierBody;
    this->_inputTAR = _parser->ParseTAR(params->front(), this->error_msg, queryPlan, idCounter);
    operation->AddParam("tar_name", identifierChain->getIdentifier()->_identifierBody);
    params->pop_front();
}

void RegisterModelParser::parseAttribute(list<ValueExpressionPtr> *params, OperationPtr operation,
                                       QueryPlanPtr queryPlan, int &idCounter) {
    auto identifierChain = this->parseIdentifierChain(params->front());
    string identifier = identifierChain->getIdentifier()->_identifierBody;

    if (this->_inputTAR->HasDataElement(identifier)) {
        operation->AddParam("target_attribute", identifierChain->getIdentifier()->_identifierBody);
        params->pop_front();
    } else {
        throw std::runtime_error("Schema element " + identifier + " is not a valid member.");
    }
}

void RegisterModelParser::parseDimensionString(list<ValueExpressionPtr> *params, OperationPtr operation) {
    auto characterString = this->parseString(params->front());
    operation->AddParam("dimension_string", characterString->getLiteralString());
    params->pop_front();
}

void RegisterModelParser::parseModelDirectory(list<ValueExpressionPtr> *params, OperationPtr operation) {
    auto characterString = this->parseString(params->front());
    operation->AddParam("model_path", characterString->getLiteralString());
    params->pop_front();
}

IdentifierChainPtr RegisterModelParser::parseIdentifierChain(ValueExpressionPtr value){
    if(auto identifierChain = PARSE(value, IdentifierChain)){
        return identifierChain;
    } else {
        throw std::runtime_error(this->error_msg);
    }
}

TARPtr RegisterModelParser::parseTAR(ValueExpressionPtr value, QueryPlanPtr queryPlan, int &idCounter){
    auto tar = this->_parser->ParseTAR(value, this->error_msg, queryPlan, idCounter);
    if (tar != nullptr) {
        return tar;
    } else {
        throw std::runtime_error(this->error_msg);
    }
}

CharacterStringLiteralPtr RegisterModelParser::parseString(ValueExpressionPtr value){
    auto s = PARSE(value, CharacterStringLiteral);
    if (s != nullptr) {
        return s;
    } else {
        throw std::runtime_error(this->error_msg);
    }
}
