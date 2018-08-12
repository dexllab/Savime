%option outfile="flex.cpp" header-file="flex.h"
%option reentrant bison-bridge
%option noyywrap nounput noinput

%{
#include <stdlib.h>
#include <string.h>
#include "tree.h"
#include "bison.h"

void free_mem(void * buffer);
%}


%%


[*] 			 {
			    return ASTERISK;
	       		 }

[,] 			 {
			    return COMMA;
	       		 }

[.] 			 {
			    return PERIOD;
	       		 }

[:] 			 {
			    return COLON;
	       		 }

[+] 			 {
			    return PLUS_SIGN;
	       		 }

[-] 			 {
			    return MINUS_SIGN;
	       		 }

[/] 			 {
			    return SOLIDUS;
	       		 }

[\%] 			 {
			    return MODULUS;
	       		 }

[\^] 			 {
			    return POWER;
	       		 }

[oO][rR] 		 {
			    return OR;
	       		 }

[aA][nN][dD] 		 {
			    return AND;
	       		 }

[nN][oO][tT] 		 {
			    return NOT;
	       		 }

[(]			 {
			    return LEFT_PAREN;
	       		 }

[)]			 {
			    return RIGHT_PAREN;
	       		 }

["]			 {
			    return DOUBLE_QUOTE;
	       		 }

[;]			 {
			    return SEMICOLON;
                         }
  
[|][|] 		 	 {
			    return DOUBLE_PIPE;
	       		 }


[tT][rR][uU][eE]	 {
			    return TRUE;
       			 }

[fF][aA][lL][sS][eE]	 {
			    return FALSE;
       			 }

[bB]\"[01]+\" 	 	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;
			    return BITS;
	       		 }  

[hH]\"[0-9a-fA-F]+\" 	 {
			    //yylval->value = strdup(yytext);	
			    yylval->value = yytext;
                            return HEX;
	       		 }

[\']	 	 	 {
			    return QUOTE;
	       		 }

[=]	 	 	 {
			    return EQ;
	       		 }

\<\>	 	 	 {
			    return NEQ;
	       		 }

\<	 	 	 {
			    return LE;
	       		 }

\>	 	 	 {
			    return GE;
	       		 }

\<=	 	 	 {
			    return LEQ;
	       		 }

\>=	 	 	 {
			    return GEQ;
	       		 }

[0-9]+		 	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;
			    return EXACT_NUMBER;
	       		 }

[0-9]*\.[0-9]+	 	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;    
			    return EXACT_NUMBER;
	       		 }

[+-][0-9]+		 {
			    //yylval->value = strdup(yytext);	
                            yylval->value = yytext;    
			    return SIGNED_EXACT_NUMBER;
	       		 }

[+-][0-9]*\.[0-9]+	 {
			    //yylval->value = strdup(yytext);	
                            yylval->value = yytext;    
			    return SIGNED_EXACT_NUMBER;
	       		 }

[+-]*[0-9]*\.[0-9]+	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;
			    return EXACT_NUMBER;
	       		 }

[0-9]+[eE][+-]*[0-9]+ 	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;    
			    return APROX_NUMBER;
	       		 }

[+-][0-9]+[eE][+-]*[0-9]+ {
			      //yylval->value = strdup(yytext);	
                              yylval->value = yytext;
			      return SIGNED_APROX_NUMBER;
	       		  }
   
L?\"(\\.|[^\\"])*\"	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;    
			    return STRING;
	       		 }

L?\'(\\.|[^\\'])*\'	 {
			    //yylval->value = strdup(yytext);
                            yylval->value = yytext;    
			    return STRING;
	       		 }

[a-zA-Z_][a-zA-Z0-9_]*(:[0-9]+)? { 
			          //yylval->value = strdup(yytext);
                                  yylval->value = yytext;
			          return IDENTIFIER_BODY;
	       		         }

       
[\n] 	{
		return NEWLINE;
	}


[ \t] 	{
		/*Ignore whitespaces*/
	}

.  	{
	
 	}
%%


void yyfree_mem(void * buffer, void * scanner)
{
    YY_BUFFER_STATE _buffer = (YY_BUFFER_STATE) buffer;
    free(_buffer->yy_ch_buf);
    free(_buffer);

    struct yyguts_t * _yyscanner = (struct yyguts_t *) scanner;
    free(_yyscanner->yy_buffer_stack);
    free(_yyscanner);
}

void test(){
	/*int code;
	while((code = yylex()) > 0){
		printf("%d ", code);
	}*/
	printf("\n");
}
    