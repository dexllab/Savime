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
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef PARSE_TREE_H
#define PARSE_TREE_H

#include <memory>
#include <stdio.h>
#include <typeinfo>
#include <string>
#include <vector>
#include <list>
#include <../core/include/symbols.h>

#define GET_IDENTIFER_BODY(x)  x->_identifier->_identifierBody
#define PARSE(x, y) std::dynamic_pointer_cast<y>(x)

class ParseTreeNode;
typedef std::shared_ptr<ParseTreeNode> ParseTreeNodePtr;
class ValueExpression;
typedef std::shared_ptr<ValueExpression> ValueExpressionPtr;
class BooleanValueExpression;
typedef std::shared_ptr<BooleanValueExpression> BooleanValueExpressionPtr;
class NumericValueExpression;
typedef std::shared_ptr<NumericValueExpression> NumericValueExpressionPtr;
class Predicate;
class QueryExpression;
typedef std::shared_ptr<QueryExpression> QueryExpressionPtr;
class Identifier;
typedef std::shared_ptr<Identifier> IdentifierPtr;
class UnsignedLiteral;
typedef std::shared_ptr<UnsignedLiteral> UnsignedLiteralPtr;
class IdentifierChain;
typedef std::shared_ptr<IdentifierChain> IdentifierChainPtr;
class SummationNumericExpression;
class SubtractionNumericalExpression;
class ProductNumericalExpression;
class DivisonNumericalExpression;
class ModulusNumericalExpression;
class PowerNumericalExpression;
class LogicalConjunction;
typedef std::shared_ptr<LogicalConjunction> LogicalConjunctionPtr;
class LogicalDisjunction;
typedef std::shared_ptr<LogicalDisjunction> LogicalDisjunctionPtr;
class ComparisonPredicate;
typedef std::shared_ptr<ComparisonPredicate> ComparisonPredicatePtr;
class SetFunctionType;
class GeneralSetFunction;
class ValueExpressionList;
class GeneralLiteral;
class TruthValue;
class SignedNumericLiteral;
typedef std::shared_ptr<SignedNumericLiteral> SignedNumericLiteralPtr;
class UnsignedNumericLiteral;
typedef std::shared_ptr<UnsignedNumericLiteral> UnsignedNumericLiteralPtr;
class CharacterStringLiteral;
typedef std::shared_ptr<CharacterStringLiteral> CharacterStringLiteralPtr;
class BitStringLiteral;
class HexStringLiteral;
class CompOp;
class Not;
class DefaultParser;

enum TruthValueEnum { TRUE_VALUE, FALSE_VALUE, UNKNOWN_VALUE };

enum CompOpEnum {
  EQUALS,
  NOT_EQUALS,
  LESS_THAN,
  GREATER_THAN,
  LESS_EQ_THAN,
  GREATER_EQ_THAN,
  LIKE_COMP
};

class ParseTreeNode {
public:
  std::vector<ParseTreeNodePtr> _children;

  virtual void printType() { printf("%s\n", typeid(*this).name()); }

  void printTreeNode(int level) {

    for (int i = 0; i < level; i++) {
      printf("\t");
    }

    printType();

    for (int i = 0; i < _children.size(); i++) {
      if (_children[i] != nullptr)
        _children[i]->printTreeNode(level + 1);
    }
  }
};

class ValueExpression : public ParseTreeNode {};

class BooleanValueExpression : public ValueExpression {
  BooleanValueExpressionPtr _parenthesized_boolean_value_expression;
  ValueExpressionPtr _notValueExpression;

public:
  friend DefaultParser;

  BooleanValueExpression() {}

  BooleanValueExpression(
      BooleanValueExpressionPtr parenthesized_boolean_value_expression) {
    _parenthesized_boolean_value_expression =
        parenthesized_boolean_value_expression;
    ParseTreeNode::_children.push_back(
        PARSE(parenthesized_boolean_value_expression, ParseTreeNode));
  }

  BooleanValueExpression(ValueExpressionPtr notValueExpression) {
    _notValueExpression = notValueExpression;
    ParseTreeNode::_children.push_back(
        PARSE(notValueExpression, ParseTreeNode));
  }
};

class NumericValueExpression : public ValueExpression {};

class Predicate : public ValueExpression {};

class ValueExpressionList : public ParseTreeNode {
  ValueExpressionPtr _valueExpression;
  std::shared_ptr<ValueExpressionList> _valueExpressionList;

public:
  friend DefaultParser;

  ValueExpressionList(
      ValueExpressionPtr valueExpression,
      std::shared_ptr<ValueExpressionList> valueExpressionList) {
    _valueExpression = valueExpression;
    _valueExpressionList = valueExpressionList;
    ParseTreeNode::_children.push_back(PARSE(valueExpression, ParseTreeNode));
    ParseTreeNode::_children.push_back(
        PARSE(valueExpressionList, ParseTreeNode));
  }

  std::list<ValueExpressionPtr> ParamsToList() {
    std::list<ValueExpressionPtr> params;
    std::shared_ptr<ValueExpressionList> list = _valueExpressionList;
    params.push_front(_valueExpression);
    while (list != nullptr) {
      params.push_front(list->_valueExpression);
      list = list->_valueExpressionList;
    }

    return params;
  }
};

class QueryExpression : public ValueExpression {
  IdentifierPtr _identifier;


public:
  friend DefaultParser;
  std::shared_ptr<ValueExpressionList> _value_expression_list;
  QueryExpression(IdentifierPtr identifier,
                  std::shared_ptr<ValueExpressionList> value_expression_list) {
    _identifier = identifier;
    if (value_expression_list != nullptr)
      _value_expression_list = value_expression_list;
    else
      _value_expression_list = std::shared_ptr<ValueExpressionList>(
          new ValueExpressionList(nullptr, nullptr));

    ParseTreeNode::_children.push_back(PARSE(identifier, ParseTreeNode));
    ParseTreeNode::_children.push_back(
        PARSE(value_expression_list, ParseTreeNode));
  }
};

class Identifier : public ParseTreeNode {
public:
  friend DefaultParser;
  std::string _identifierBody;
  Identifier(std::string identifierBody) { _identifierBody = identifierBody; }

  virtual void printType() {
    printf("%s %s\n", typeid(*this).name(), _identifierBody.c_str());
  }
};

class UnsignedLiteral : public ValueExpression {
  UnsignedNumericLiteralPtr _unsignedNumericLiteral;
  std::shared_ptr<GeneralLiteral> _generalLiteral;

public:
  friend DefaultParser;

  UnsignedLiteral(UnsignedNumericLiteralPtr unsignedNumericLiteral) {
    _unsignedNumericLiteral = unsignedNumericLiteral;
    ParseTreeNode::_children.push_back(
        PARSE(unsignedNumericLiteral, ParseTreeNode));
  }

  UnsignedLiteral(std::shared_ptr<GeneralLiteral> generalLiteral) {
    _generalLiteral = generalLiteral;
    ParseTreeNode::_children.push_back(PARSE(generalLiteral, ParseTreeNode));
  }
};

class IdentifierChain : public ValueExpression {
  IdentifierPtr _identifier;
  IdentifierChainPtr _identifierChain;

public:
  friend DefaultParser;

  IdentifierChain(IdentifierPtr identifier,
                  IdentifierChainPtr identifierChain) {
    _identifier = identifier;
    _identifierChain = identifierChain;
    ParseTreeNode::_children.push_back(PARSE(identifier, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(identifierChain, ParseTreeNode));
  }
  IdentifierPtr getIdentifier(){ return _identifier;  }
  IdentifierChainPtr getIdentifierChain(){ return _identifierChain;  }
};

class SetFunctionSpecification : public ValueExpression {
  bool _isCountStarExpression;
  std::shared_ptr<GeneralSetFunction> _generalSetFunction;

public:
  friend DefaultParser;

  SetFunctionSpecification(
      bool isCountStarExpression,
      std::shared_ptr<GeneralSetFunction> generalSetFunction) {
    _isCountStarExpression = isCountStarExpression;
    _generalSetFunction = generalSetFunction;
    ParseTreeNode::_children.push_back(
        PARSE(generalSetFunction, ParseTreeNode));
  }
};

class FieldReference : public ValueExpression {
  ValueExpressionPtr _valueExpression;
  IdentifierPtr _identifier;

public:
  friend DefaultParser;

  FieldReference(ValueExpressionPtr valueExpression, IdentifierPtr identifier) {
    _valueExpression = valueExpression;
    _identifier = identifier;
    ParseTreeNode::_children.push_back(PARSE(valueExpression, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(identifier, ParseTreeNode));
  }
};

class SummationNumericExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;
  double _literalNumericalOperand = 0;

public:
  friend DefaultParser;

  SummationNumericExpression(ValueExpressionPtr leftOperand,
                             ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }

  SummationNumericExpression(ValueExpressionPtr leftOperand,
                             double literalNumericalOperand) {
    _leftOperand = leftOperand;
    _rightOperand = nullptr;
    _literalNumericalOperand = literalNumericalOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
  }

  virtual void printType() {
    printf("%s %lf\n", typeid(*this).name(), _literalNumericalOperand);
  }
};

class SubtractionNumericalExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;
  double _literalNumericalOperando = 0;

public:
  friend DefaultParser;

  SubtractionNumericalExpression(ValueExpressionPtr leftOperand,
                                 ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }

  SubtractionNumericalExpression(ValueExpressionPtr leftOperand,
                                 double literalNumericalOperando) {
    _leftOperand = leftOperand;
    _literalNumericalOperando = literalNumericalOperando;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
  }

  virtual void printType() {
    printf("%s %lf\n", typeid(*this).name(), _literalNumericalOperando);
  }
};

class ProductNumericalExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  ProductNumericalExpression(ValueExpressionPtr leftOperand,
                             ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class DivisonNumericalExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  DivisonNumericalExpression(ValueExpressionPtr leftOperand,
                             ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class ModulusNumericalExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  ModulusNumericalExpression(ValueExpressionPtr leftOperand,
                             ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class PowerNumericalExpression : public NumericValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  PowerNumericalExpression(ValueExpressionPtr leftOperand,
                           ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class IsBooleanValueExpression : public BooleanValueExpression {
  ValueExpressionPtr _leftOperand;
  std::shared_ptr<TruthValue> _rightOperand;
  bool _notAfterIsoperand;

public:
  friend DefaultParser;

  IsBooleanValueExpression(ValueExpressionPtr leftOperand,
                           std::shared_ptr<TruthValue> rightOperand,
                           bool notAfterIsoperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    _notAfterIsoperand = notAfterIsoperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class LogicalConjunction : public BooleanValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  LogicalConjunction(ValueExpressionPtr leftOperand,
                     ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class LogicalDisjunction : public BooleanValueExpression {
  ValueExpressionPtr _leftOperand;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  LogicalDisjunction(ValueExpressionPtr leftOperand,
                     ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class ComparisonPredicate : public Predicate {
  ValueExpressionPtr _leftOperand;
  std::shared_ptr<CompOp> _comparisonOperator;
  ValueExpressionPtr _rightOperand;

public:
  friend DefaultParser;

  ComparisonPredicate(ValueExpressionPtr leftOperand,
                      std::shared_ptr<CompOp> comparisonOperator,
                      ValueExpressionPtr rightOperand) {
    _leftOperand = leftOperand;
    _comparisonOperator = comparisonOperator;
    _rightOperand = rightOperand;
    ParseTreeNode::_children.push_back(PARSE(leftOperand, ParseTreeNode));
    ParseTreeNode::_children.push_back(
        PARSE(comparisonOperator, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(rightOperand, ParseTreeNode));
  }
};

class SetFunctionType : public ParseTreeNode {
  std::string _functionName;

public:
  friend DefaultParser;

  SetFunctionType(std::string functionName) { _functionName = functionName; }

  virtual void printType() {
    printf("%s %s\n", typeid(*this).name(), _functionName.c_str());
  }
};

class DelimitedIdentifier : public Identifier {

  friend DefaultParser;

public:
  DelimitedIdentifier(std::string identifier) : Identifier(identifier) {}
};

class GeneralSetFunction : public ParseTreeNode {
  std::shared_ptr<SetFunctionType> _setFunctionType;
  ValueExpressionPtr _valueExpression;

public:
  friend DefaultParser;
  friend QueryExpression;
  GeneralSetFunction(std::shared_ptr<SetFunctionType> setFunctionType,
                     ValueExpressionPtr valueExpression) {
    _setFunctionType = setFunctionType;
    _valueExpression = valueExpression;
    ParseTreeNode::_children.push_back(PARSE(setFunctionType, ParseTreeNode));
    ParseTreeNode::_children.push_back(PARSE(valueExpression, ParseTreeNode));
  }
};

class GeneralLiteral : public ValueExpression {
  std::string _literalString;

public:
  friend DefaultParser;
  GeneralLiteral(std::string literalString) { _literalString = literalString; }
  std::string getLiteralString(){ return _literalString; }
  virtual void printType() {
    printf("%s %s\n", typeid(*this).name(), _literalString.c_str());
  }
};

class TruthValue : public GeneralLiteral {
  TruthValueEnum value;

public:
  friend DefaultParser;

  TruthValue(std::string literalString) : GeneralLiteral(literalString) {}
};

class ParenthesizedBooleanValueExpression : public Predicate {
  ValueExpressionPtr _booleanValueExpression;

public:
  friend DefaultParser;

  ParenthesizedBooleanValueExpression(
      ValueExpressionPtr booleanValueExpression) {
    _booleanValueExpression = booleanValueExpression;
    ParseTreeNode::_children.push_back(
        PARSE(booleanValueExpression, ParseTreeNode));
  }
};

class SignedNumericLiteral : public NumericValueExpression {
  double _doubleValue;

public:
  friend DefaultParser;

  SignedNumericLiteral(double doubleValue) { _doubleValue = doubleValue; }

  virtual void printType() {
    printf("%s %lf\n", typeid(*this).name(), _doubleValue);
  }
};

class UnsignedNumericLiteral : public NumericValueExpression {
  double _doubleValue;

public:
  friend DefaultParser;

  UnsignedNumericLiteral(double doubleValue) { _doubleValue = doubleValue; }

  virtual void printType() {
    printf("%s %lf\n", typeid(*this).name(), _doubleValue);
  }
};

class CharacterStringLiteral : public GeneralLiteral {
public:
  friend DefaultParser;

  CharacterStringLiteral(std::string literalString)
      : GeneralLiteral(literalString) {}
};

class BitStringLiteral : public GeneralLiteral {
public:
  friend DefaultParser;

  BitStringLiteral(std::string literalString) : GeneralLiteral(literalString) {}
};

class HexStringLiteral : public GeneralLiteral {
public:
  friend DefaultParser;

  HexStringLiteral(std::string literalString) : GeneralLiteral(literalString) {}
};

class CompOp : public ParseTreeNode {
  CompOpEnum _value;

public:
  friend DefaultParser;

  CompOp(CompOpEnum value) { _value = value; }

  std::string toString() {
    switch (_value) {
    case EQUALS:
      return std::string(_EQ);
      break;
    case NOT_EQUALS:
      return std::string(_NEQ);
      break;
    case GREATER_THAN:
      return std::string(_GE);
      break;
    case LESS_THAN:
      return std::string(_LE);
      break;
    case GREATER_EQ_THAN:
      return std::string(_GEQ);
      break;
    case LESS_EQ_THAN:
      return std::string(_LEQ);
      break;
    case LIKE_COMP:
      return std::string(_LIKE);
      break;
    }

    return std::string("");
  }
};

class Not : public ParseTreeNode {
public:
};

#endif /* PARSE_TREE_H */
