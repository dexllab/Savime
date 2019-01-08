%{
 #include <stdio.h>
 #include <string.h>
 #include <string>
 #include <list>
 #include <memory>
 #include "tree.h"
 #include "bison.h"

 #define YYDEBUG 1

typedef void* yyscan_t;
void yyerror(void *, void **, const char *);
typedef struct yy_buffer_state * YY_BUFFER_STATE;
YY_BUFFER_STATE yy_scan_string(const char * str, void * scanner);
void yyfree_mem(void * buffer, void * scanner);
/*
 * Following declarations were obtained in the flex.h file genereated by flex!
 */
int yylex_init (yyscan_t* scanner);
int yylex_destroy (yyscan_t yyscanner );
int yyget_debug (yyscan_t yyscanner );

extern int yylex \
               (YYSTYPE * yylval_param ,yyscan_t yyscanner);

#define YY_DECL int yylex \
               (YYSTYPE * yylval_param , yyscan_t yyscanner)

%}

%output  "bison.cpp"
%defines "bison.h"
%define parse.error verbose
%define api.pure full //For reentrant code
%lex-param   {void * scanner}
%parse-param {void * scanner}
%parse-param {void ** ret}

%token ASTERISK
%token COMMA
%token PERIOD
%token COLON
%token SEMICOLON
%token PLUS_SIGN
%token MINUS_SIGN
%token SOLIDUS
%token MODULUS
%token POWER
%token DIV
%token OR
%token AND
%token LIKE
%token LEFT_PAREN;
%token RIGHT_PAREN;
%token LEFT_ANGLE_BRACKETS;
%token RIGHT_ANGLE_BRACKETS;
%token DOUBLE_QUOTE
%token IS
%token DOUBLE_PIPE;
%token RIGHT_ARROW
%token IDENTIFIER_BODY 
%token TRUE
%token FALSE
%token UNKNOWN
%token B
%token BITS
%token X
%token HEX
%token BETWEEN
%token IN
%token EXACT_NUMBER
%token SIGNED_EXACT_NUMBER
%token APROX_NUMBER
%token SIGNED_APROX_NUMBER
%token STRING
%token QUOTE
%token EQ
%token NEQ
%token LE
%token GE
%token LEQ
%token GEQ
%token NEWLINE


%union {
	float num;
	char * value;
	ParseTreeNodePtr  * node;
	QueryExpressionPtr   * queryExpression;
	ValueExpressionPtr* valueExpression;
	IdentifierPtr  * identifier;
	UnsignedLiteralPtr  * unsignedLiteral;
	std::shared_ptr<ValueExpressionList> * valueExpressionList;
	std::shared_ptr<GeneralLiteral> * generalLiteral;
	IdentifierChainPtr  * identifierChain;
	UnsignedNumericLiteralPtr  * unsignedNumericLiteral;
        SignedNumericLiteralPtr  * signedNumericLiteral;
	CharacterStringLiteralPtr  * characterStringLiteral;
	std::shared_ptr<BitStringLiteral> * bitStringLiteral;
	std::shared_ptr<HexStringLiteral> * hexStringLiteral;
	std::shared_ptr<CompOp> * compOp;
}

%type <queryExpression> query_expression
%type <valueExpression> value_expression
%type <identifier> identifier
%type <valueExpressionList> value_expression_list
%type <generalLiteral> general_literal
%type <identifierChain> identifier_chain
%type <unsignedNumericLiteral> unsigned_numeric_literal
%type <signedNumericLiteral> signed_numeric_literal
%type <characterStringLiteral> character_string_literal
%type <bitStringLiteral> bit_string_literal
%type <hexStringLiteral> hex_string_literal
%type <value> BITS
%type <value> EXACT_NUMBER
%type <value> SIGNED_EXACT_NUMBER
%type <value> APROX_NUMBER
%type <value> SIGNED_APROX_NUMBER
%type <value> STRING
%type <value> IDENTIFIER_BODY
%type <value> HEX

//Operations Precedence
%left AND OR NOT
%nonassoc EQ NEQ LE GE LEQ GEQ LIKE
%left PLUS_SIGN MINUS_SIGN 
%nonassoc SIGNED_EXACT_NUMBER
%left ASTERISK SOLIDUS MODULUS
%right POWER

%debug
%%

expression: 
expression_bulk {}
| expression_bulk SEMICOLON {}
| expression_bulk SEMICOLON NEWLINE {}
| NEWLINE {}
;

expression_bulk :
query_expression {}
;

query_expression:
 identifier LEFT_PAREN value_expression_list RIGHT_PAREN {$$ = new QueryExpressionPtr  (new QueryExpression(*$1, *$3)); delete $1; delete $3; *ret = (void*)$$;}
|identifier LEFT_PAREN RIGHT_PAREN {$$ = new QueryExpressionPtr  (new QueryExpression(*$1, NULL)); delete $1; *ret = (void*)$$;}
;

value_expression_list :
value_expression {$$ = new std::shared_ptr<ValueExpressionList>(new ValueExpressionList(*$1, NULL)); delete $1;}
| value_expression_list COMMA value_expression {$$ = new std::shared_ptr<ValueExpressionList>(new ValueExpressionList(*$3, *$1)); delete $1; delete $3;}
;

value_expression:
identifier_chain  {$$ = new ValueExpressionPtr (); *$$ = PARSE(*$1, ValueExpression); delete $1;}
| query_expression {$$ = new ValueExpressionPtr (); *$$ = PARSE(*$1, ValueExpression); delete $1;}
| LEFT_PAREN value_expression RIGHT_PAREN {$$ = new ValueExpressionPtr (); *$$ = PARSE(*$2, ValueExpression); delete $2;}
| unsigned_numeric_literal{$$ = new ValueExpressionPtr (); *$$ = PARSE(*$1, ValueExpression); delete $1;}
| signed_numeric_literal{$$ = new ValueExpressionPtr (); *$$ = PARSE(*$1, ValueExpression); delete $1;}
| general_literal {$$ = new ValueExpressionPtr (); *$$ = PARSE(*$1, ValueExpression); delete $1;}
| value_expression SIGNED_EXACT_NUMBER {$$ = new ValueExpressionPtr (new SummationNumericExpression(*$1, strtod($2, NULL))); delete $1;} 
| value_expression PLUS_SIGN value_expression {$$ = new ValueExpressionPtr (new SummationNumericExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression MINUS_SIGN value_expression {$$ = new ValueExpressionPtr (new SubtractionNumericalExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression POWER value_expression {$$ = new ValueExpressionPtr (new PowerNumericalExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression SOLIDUS value_expression {$$ = new ValueExpressionPtr (new DivisonNumericalExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression MODULUS value_expression {$$ = new ValueExpressionPtr (new ModulusNumericalExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression ASTERISK value_expression {$$ = new ValueExpressionPtr (new ProductNumericalExpression(*$1, *$3)); delete $1; delete $3;}
| value_expression EQ value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(EQUALS)), *$3)); delete $1; delete $3;}
| value_expression NEQ value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(NOT_EQUALS)), *$3)); delete $1; delete $3;}
| value_expression LE value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(LESS_THAN)), *$3)); delete $1; delete $3;}
| value_expression GE value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(GREATER_THAN)), *$3)); delete $1; delete $3;}
| value_expression LEQ value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(LESS_EQ_THAN)), *$3)); delete $1; delete $3;}
| value_expression GEQ value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(GREATER_EQ_THAN)), *$3)); delete $1; delete $3;}
| value_expression LIKE value_expression {$$ = new ValueExpressionPtr (new ComparisonPredicate(*$1, std::shared_ptr<CompOp>(new CompOp(LIKE_COMP)), *$3)); delete $1; delete $3;}
| NOT value_expression {$$ = new ValueExpressionPtr (new BooleanValueExpression(*$2)); delete $2;}
| value_expression OR value_expression {$$ = new ValueExpressionPtr (new LogicalConjunction(*$1, *$3)); delete $1; delete $3;}
| value_expression AND value_expression {$$ = new ValueExpressionPtr (new LogicalDisjunction(*$1, *$3)); delete $1; delete $3;}
;

identifier_chain:
identifier {$$ = new IdentifierChainPtr (new IdentifierChain(*$1, NULL)); delete $1;}
| identifier_chain PERIOD identifier  {$$ = new IdentifierChainPtr (new IdentifierChain(*$3, *$1)); delete $1; delete $3;}
;

identifier: 
IDENTIFIER_BODY {std::string literal($1); $$ = new IdentifierPtr (new Identifier(literal));}
;

general_literal:
character_string_literal {$$ = new std::shared_ptr<GeneralLiteral>(); *$$ = PARSE(*$1, GeneralLiteral); delete $1;}
| bit_string_literal {$$ = new std::shared_ptr<GeneralLiteral>(); *$$ = PARSE(*$1, GeneralLiteral); delete $1;}
| hex_string_literal {$$ = new std::shared_ptr<GeneralLiteral>(); *$$ = PARSE(*$1, GeneralLiteral); delete $1;}
;

unsigned_numeric_literal: 
EXACT_NUMBER  {$$ = new UnsignedNumericLiteralPtr (new UnsignedNumericLiteral(strtod($1,NULL)));}
| APROX_NUMBER {$$ = new UnsignedNumericLiteralPtr (new UnsignedNumericLiteral(strtod($1,NULL)));}
;

signed_numeric_literal:
 SIGNED_EXACT_NUMBER {$$ = new SignedNumericLiteralPtr (new SignedNumericLiteral(strtod($1, NULL)));}
| SIGNED_APROX_NUMBER {$$ = new SignedNumericLiteralPtr (new SignedNumericLiteral(strtod($1, NULL)));}
;

character_string_literal:
STRING {std::string literal($1); $$ = new CharacterStringLiteralPtr (new CharacterStringLiteral(literal));}
;

bit_string_literal:
B QUOTE BITS QUOTE {std::string literal($3); $$ = new std::shared_ptr<BitStringLiteral>(new BitStringLiteral(literal));}
;

hex_string_literal:
X QUOTE HEX QUOTE {std::string literal($3); $$ = new std::shared_ptr<HexStringLiteral>(new HexStringLiteral(literal));}
;

%%
//Copy error msg from Bison internals to a buffer
void yyerror(void * scanner,void ** ret, const char *s) 
{
    size_t len = strlen(s);
    *ret = malloc(len+1);
    strncpy((char*)*ret, s, len);
}

//Change from __main to main to create standalone parser for debugging
//Change back from main to __main for building SAVIME
//TO build: g++ bison.cpp flex.cpp -std=c++0x -o bison
int __main(int argc, char * args[]) 
{
    //for(int i = 0; i < 3; i++){
    //yydebug=1;
    void * ret;
    ParseTreeNodePtr  node; 
    QueryExpressionPtr   * aux; 
    
    void * scanner ; int error;
    yylex_init(&scanner);
    void * state = yy_scan_string(args[1], scanner); 
    
    
    error = yyparse(scanner, &ret);

    if(error)
    {
        printf("Could not parse text: %s\n", (char*)ret);
    }
    else
    {
       aux = (QueryExpressionPtr   *)ret;     
       node = PARSE(*aux, ParseTreeNode);
       node->printTreeNode(1);
       delete aux;
    }
    
    yylex_destroy(scanner);
    //yyfree_mem(state, scanner);     
    //}
    
    return 0;
}






