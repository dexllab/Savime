#!/bin/bash
BASEDIR=$(dirname "$0")
cd $BASEDIR
bison -t -d $BASEDIR/grammar.y && lex $BASEDIR/lexer.l