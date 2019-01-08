/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.0.4"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* Copy the first part of user declarations.  */
#line 1 "./grammar.y" /* yacc.c:339  */

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


#line 97 "bison.cpp" /* yacc.c:339  */

# ifndef YY_NULLPTR
#  if defined __cplusplus && 201103L <= __cplusplus
#   define YY_NULLPTR nullptr
#  else
#   define YY_NULLPTR 0
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* In a future release of Bison, this section will be replaced
   by #include "bison.h".  */
#ifndef YY_YY_BISON_H_INCLUDED
# define YY_YY_BISON_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    ASTERISK = 258,
    COMMA = 259,
    PERIOD = 260,
    COLON = 261,
    SEMICOLON = 262,
    PLUS_SIGN = 263,
    MINUS_SIGN = 264,
    SOLIDUS = 265,
    MODULUS = 266,
    POWER = 267,
    DIV = 268,
    OR = 269,
    AND = 270,
    LIKE = 271,
    LEFT_PAREN = 272,
    RIGHT_PAREN = 273,
    LEFT_ANGLE_BRACKETS = 274,
    RIGHT_ANGLE_BRACKETS = 275,
    DOUBLE_QUOTE = 276,
    IS = 277,
    DOUBLE_PIPE = 278,
    RIGHT_ARROW = 279,
    IDENTIFIER_BODY = 280,
    TRUE = 281,
    FALSE = 282,
    UNKNOWN = 283,
    B = 284,
    BITS = 285,
    X = 286,
    HEX = 287,
    BETWEEN = 288,
    IN = 289,
    EXACT_NUMBER = 290,
    SIGNED_EXACT_NUMBER = 291,
    APROX_NUMBER = 292,
    SIGNED_APROX_NUMBER = 293,
    STRING = 294,
    QUOTE = 295,
    EQ = 296,
    NEQ = 297,
    LE = 298,
    GE = 299,
    LEQ = 300,
    GEQ = 301,
    NEWLINE = 302,
    NOT = 303
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 87 "./grammar.y" /* yacc.c:355  */

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

#line 205 "bison.cpp" /* yacc.c:355  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int yyparse (void * scanner, void ** ret);

#endif /* !YY_YY_BISON_H_INCLUDED  */

/* Copy the second part of user declarations.  */

#line 221 "bison.cpp" /* yacc.c:358  */

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE
# if (defined __GNUC__                                               \
      && (2 < __GNUC__ || (__GNUC__ == 2 && 96 <= __GNUC_MINOR__)))  \
     || defined __SUNPRO_C && 0x5110 <= __SUNPRO_C
#  define YY_ATTRIBUTE(Spec) __attribute__(Spec)
# else
#  define YY_ATTRIBUTE(Spec) /* empty */
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# define YY_ATTRIBUTE_PURE   YY_ATTRIBUTE ((__pure__))
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# define YY_ATTRIBUTE_UNUSED YY_ATTRIBUTE ((__unused__))
#endif

#if !defined _Noreturn \
     && (!defined __STDC_VERSION__ || __STDC_VERSION__ < 201112)
# if defined _MSC_VER && 1200 <= _MSC_VER
#  define _Noreturn __declspec (noreturn)
# else
#  define _Noreturn YY_ATTRIBUTE ((__noreturn__))
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN \
    _Pragma ("GCC diagnostic push") \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")\
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif


#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYSIZE_T yynewbytes;                                            \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / sizeof (*yyptr);                          \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, (Count) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYSIZE_T yyi;                         \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  7
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   186

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  49
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  14
/* YYNRULES -- Number of rules.  */
#define YYNRULES  46
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  77

/* YYTRANSLATE[YYX] -- Symbol number corresponding to YYX as returned
   by yylex, with out-of-bounds checking.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   303

#define YYTRANSLATE(YYX)                                                \
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, without out-of-bounds checking.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,   138,   138,   139,   140,   141,   145,   149,   150,   154,
     155,   159,   160,   161,   162,   163,   164,   165,   166,   167,
     168,   169,   170,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   185,   186,   190,   194,   195,   196,
     200,   201,   205,   206,   210,   214,   218
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 1
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "ASTERISK", "COMMA", "PERIOD", "COLON",
  "SEMICOLON", "PLUS_SIGN", "MINUS_SIGN", "SOLIDUS", "MODULUS", "POWER",
  "DIV", "OR", "AND", "LIKE", "LEFT_PAREN", "RIGHT_PAREN",
  "LEFT_ANGLE_BRACKETS", "RIGHT_ANGLE_BRACKETS", "DOUBLE_QUOTE", "IS",
  "DOUBLE_PIPE", "RIGHT_ARROW", "IDENTIFIER_BODY", "TRUE", "FALSE",
  "UNKNOWN", "B", "BITS", "X", "HEX", "BETWEEN", "IN", "EXACT_NUMBER",
  "SIGNED_EXACT_NUMBER", "APROX_NUMBER", "SIGNED_APROX_NUMBER", "STRING",
  "QUOTE", "EQ", "NEQ", "LE", "GE", "LEQ", "GEQ", "NEWLINE", "NOT",
  "$accept", "expression", "expression_bulk", "query_expression",
  "value_expression_list", "value_expression", "identifier_chain",
  "identifier", "general_literal", "unsigned_numeric_literal",
  "signed_numeric_literal", "character_string_literal",
  "bit_string_literal", "hex_string_literal", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303
};
# endif

#define YYPACT_NINF -37

#define yypact_value_is_default(Yystate) \
  (!!((Yystate) == (-37)))

#define YYTABLE_NINF -1

#define yytable_value_is_error(Yytable_value) \
  (!!((Yytable_value) == (-1)))

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     -20,   -37,   -37,     2,    -4,   -37,   -13,   -37,   -36,   123,
     -37,   138,   -37,   -25,   -22,   -37,   -37,   -37,   -37,   -37,
     138,   -37,    15,    14,    16,   -13,   -37,   -37,   -37,   -37,
     -37,   -37,    -2,   -10,    -1,    58,   138,   -37,   138,   138,
     138,   138,   138,   138,   138,   138,   138,   -37,   138,   138,
     138,   138,   138,   138,     7,   -37,    -5,    -3,    14,    24,
      35,    35,    24,    24,    24,    58,    58,    69,    69,    69,
      69,    69,    69,    69,   -37,   -37,   -37
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,    36,     5,     0,     2,     6,     0,     1,     3,     0,
       4,     0,     8,     0,     0,    40,    42,    41,    43,    44,
       0,    12,     0,     9,    11,    34,    16,    14,    15,    37,
      38,    39,     0,     0,     0,    31,     0,     7,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    17,     0,     0,
       0,     0,     0,     0,     0,    13,     0,     0,    10,    23,
      18,    19,    21,    22,    20,    32,    33,    30,    24,    25,
      26,    27,    28,    29,    35,    45,    46
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -37,   -37,   -37,    48,   -37,    86,   -37,     0,   -37,   -37,
     -37,   -37,   -37,   -37
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
      -1,     3,     4,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
       6,    38,     7,     8,     9,     1,    39,    40,    41,    42,
      43,    10,    44,    45,    46,    33,    55,    38,    34,    36,
      56,    54,    39,    40,    41,    42,    43,     2,    44,    45,
      46,    57,     1,    37,    47,    75,    43,    76,    38,    48,
      49,    50,    51,    52,    53,    41,    42,    43,     5,     0,
      47,     0,     0,     0,    74,    48,    49,    50,    51,    52,
      53,    38,     0,     0,     0,     0,    39,    40,    41,    42,
      43,    47,    38,     0,    46,     0,     0,    39,    40,    41,
      42,    43,     0,     0,     0,    -1,     0,     0,     0,     0,
       0,     0,     0,     0,    47,     0,     0,    32,     0,    48,
      49,    50,    51,    52,    53,    47,    35,     0,     0,     0,
      -1,    -1,    -1,    -1,    -1,    -1,     0,     0,     0,     0,
       0,     0,    58,     0,    59,    60,    61,    62,    63,    64,
      65,    66,    67,     0,    68,    69,    70,    71,    72,    73,
      11,    12,     0,     0,     0,     0,     0,     0,     1,     0,
       0,     0,    13,     0,    14,    11,     0,     0,    15,    16,
      17,    18,    19,     1,     0,     0,     0,    13,     0,    14,
       0,    20,     0,    15,    16,    17,    18,    19,     0,     0,
       0,     0,     0,     0,     0,     0,    20
};

static const yytype_int8 yycheck[] =
{
       0,     3,     0,     7,    17,    25,     8,     9,    10,    11,
      12,    47,    14,    15,    16,    40,    18,     3,    40,     4,
      30,     5,     8,     9,    10,    11,    12,    47,    14,    15,
      16,    32,    25,    18,    36,    40,    12,    40,     3,    41,
      42,    43,    44,    45,    46,    10,    11,    12,     0,    -1,
      36,    -1,    -1,    -1,    54,    41,    42,    43,    44,    45,
      46,     3,    -1,    -1,    -1,    -1,     8,     9,    10,    11,
      12,    36,     3,    -1,    16,    -1,    -1,     8,     9,    10,
      11,    12,    -1,    -1,    -1,    16,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    36,    -1,    -1,    11,    -1,    41,
      42,    43,    44,    45,    46,    36,    20,    -1,    -1,    -1,
      41,    42,    43,    44,    45,    46,    -1,    -1,    -1,    -1,
      -1,    -1,    36,    -1,    38,    39,    40,    41,    42,    43,
      44,    45,    46,    -1,    48,    49,    50,    51,    52,    53,
      17,    18,    -1,    -1,    -1,    -1,    -1,    -1,    25,    -1,
      -1,    -1,    29,    -1,    31,    17,    -1,    -1,    35,    36,
      37,    38,    39,    25,    -1,    -1,    -1,    29,    -1,    31,
      -1,    48,    -1,    35,    36,    37,    38,    39,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    48
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    25,    47,    50,    51,    52,    56,     0,     7,    17,
      47,    17,    18,    29,    31,    35,    36,    37,    38,    39,
      48,    52,    53,    54,    55,    56,    57,    58,    59,    60,
      61,    62,    54,    40,    40,    54,     4,    18,     3,     8,
       9,    10,    11,    12,    14,    15,    16,    36,    41,    42,
      43,    44,    45,    46,     5,    18,    30,    32,    54,    54,
      54,    54,    54,    54,    54,    54,    54,    54,    54,    54,
      54,    54,    54,    54,    56,    40,    40
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    49,    50,    50,    50,    50,    51,    52,    52,    53,
      53,    54,    54,    54,    54,    54,    54,    54,    54,    54,
      54,    54,    54,    54,    54,    54,    54,    54,    54,    54,
      54,    54,    54,    54,    55,    55,    56,    57,    57,    57,
      58,    58,    59,    59,    60,    61,    62
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     2,     3,     1,     1,     4,     3,     1,
       3,     1,     1,     3,     1,     1,     1,     2,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     2,     3,     3,     1,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     4,     4
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                  \
do                                                              \
  if (yychar == YYEMPTY)                                        \
    {                                                           \
      yychar = (Token);                                         \
      yylval = (Value);                                         \
      YYPOPSTACK (yylen);                                       \
      yystate = *yyssp;                                         \
      goto yybackup;                                            \
    }                                                           \
  else                                                          \
    {                                                           \
      yyerror (scanner, ret, YY_("syntax error: cannot back up")); \
      YYERROR;                                                  \
    }                                                           \
while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, scanner, ret); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*----------------------------------------.
| Print this symbol's value on YYOUTPUT.  |
`----------------------------------------*/

static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, void * scanner, void ** ret)
{
  FILE *yyo = yyoutput;
  YYUSE (yyo);
  YYUSE (scanner);
  YYUSE (ret);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# endif
  YYUSE (yytype);
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, void * scanner, void ** ret)
{
  YYFPRINTF (yyoutput, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep, scanner, ret);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yytype_int16 *yyssp, YYSTYPE *yyvsp, int yyrule, void * scanner, void ** ret)
{
  unsigned long int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[yyssp[yyi + 1 - yynrhs]],
                       &(yyvsp[(yyi + 1) - (yynrhs)])
                                              , scanner, ret);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, scanner, ret); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
yystrlen (const char *yystr)
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            /* Fall through.  */
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYSIZE_T yysize1 = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (! (yysize <= yysize1
                         && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                    return 2;
                  yysize = yysize1;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    YYSIZE_T yysize1 = yysize + yystrlen (yyformat);
    if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
      return 2;
    yysize = yysize1;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, void * scanner, void ** ret)
{
  YYUSE (yyvaluep);
  YYUSE (scanner);
  YYUSE (ret);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void * scanner, void ** ret)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        YYSTYPE *yyvs1 = yyvs;
        yytype_int16 *yyss1 = yyss;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * sizeof (*yyssp),
                    &yyvs1, yysize * sizeof (*yyvsp),
                    &yystacksize);

        yyss = yyss1;
        yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yytype_int16 *yyss1 = yyss;
        union yyalloc *yyptr =
          (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
                  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 138 "./grammar.y" /* yacc.c:1646  */
    {}
#line 1385 "bison.cpp" /* yacc.c:1646  */
    break;

  case 3:
#line 139 "./grammar.y" /* yacc.c:1646  */
    {}
#line 1391 "bison.cpp" /* yacc.c:1646  */
    break;

  case 4:
#line 140 "./grammar.y" /* yacc.c:1646  */
    {}
#line 1397 "bison.cpp" /* yacc.c:1646  */
    break;

  case 5:
#line 141 "./grammar.y" /* yacc.c:1646  */
    {}
#line 1403 "bison.cpp" /* yacc.c:1646  */
    break;

  case 6:
#line 145 "./grammar.y" /* yacc.c:1646  */
    {}
#line 1409 "bison.cpp" /* yacc.c:1646  */
    break;

  case 7:
#line 149 "./grammar.y" /* yacc.c:1646  */
    {(yyval.queryExpression) = new QueryExpressionPtr  (new QueryExpression(*(yyvsp[-3].identifier), *(yyvsp[-1].valueExpressionList))); delete (yyvsp[-3].identifier); delete (yyvsp[-1].valueExpressionList); *ret = (void*)(yyval.queryExpression);}
#line 1415 "bison.cpp" /* yacc.c:1646  */
    break;

  case 8:
#line 150 "./grammar.y" /* yacc.c:1646  */
    {(yyval.queryExpression) = new QueryExpressionPtr  (new QueryExpression(*(yyvsp[-2].identifier), NULL)); delete (yyvsp[-2].identifier); *ret = (void*)(yyval.queryExpression);}
#line 1421 "bison.cpp" /* yacc.c:1646  */
    break;

  case 9:
#line 154 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpressionList) = new std::shared_ptr<ValueExpressionList>(new ValueExpressionList(*(yyvsp[0].valueExpression), NULL)); delete (yyvsp[0].valueExpression);}
#line 1427 "bison.cpp" /* yacc.c:1646  */
    break;

  case 10:
#line 155 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpressionList) = new std::shared_ptr<ValueExpressionList>(new ValueExpressionList(*(yyvsp[0].valueExpression), *(yyvsp[-2].valueExpressionList))); delete (yyvsp[-2].valueExpressionList); delete (yyvsp[0].valueExpression);}
#line 1433 "bison.cpp" /* yacc.c:1646  */
    break;

  case 11:
#line 159 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[0].identifierChain), ValueExpression); delete (yyvsp[0].identifierChain);}
#line 1439 "bison.cpp" /* yacc.c:1646  */
    break;

  case 12:
#line 160 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[0].queryExpression), ValueExpression); delete (yyvsp[0].queryExpression);}
#line 1445 "bison.cpp" /* yacc.c:1646  */
    break;

  case 13:
#line 161 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[-1].valueExpression), ValueExpression); delete (yyvsp[-1].valueExpression);}
#line 1451 "bison.cpp" /* yacc.c:1646  */
    break;

  case 14:
#line 162 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[0].unsignedNumericLiteral), ValueExpression); delete (yyvsp[0].unsignedNumericLiteral);}
#line 1457 "bison.cpp" /* yacc.c:1646  */
    break;

  case 15:
#line 163 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[0].signedNumericLiteral), ValueExpression); delete (yyvsp[0].signedNumericLiteral);}
#line 1463 "bison.cpp" /* yacc.c:1646  */
    break;

  case 16:
#line 164 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (); *(yyval.valueExpression) = PARSE(*(yyvsp[0].generalLiteral), ValueExpression); delete (yyvsp[0].generalLiteral);}
#line 1469 "bison.cpp" /* yacc.c:1646  */
    break;

  case 17:
#line 165 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new SummationNumericExpression(*(yyvsp[-1].valueExpression), strtod((yyvsp[0].value), NULL))); delete (yyvsp[-1].valueExpression);}
#line 1475 "bison.cpp" /* yacc.c:1646  */
    break;

  case 18:
#line 166 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new SummationNumericExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1481 "bison.cpp" /* yacc.c:1646  */
    break;

  case 19:
#line 167 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new SubtractionNumericalExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1487 "bison.cpp" /* yacc.c:1646  */
    break;

  case 20:
#line 168 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new PowerNumericalExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1493 "bison.cpp" /* yacc.c:1646  */
    break;

  case 21:
#line 169 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new DivisonNumericalExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1499 "bison.cpp" /* yacc.c:1646  */
    break;

  case 22:
#line 170 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ModulusNumericalExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1505 "bison.cpp" /* yacc.c:1646  */
    break;

  case 23:
#line 171 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ProductNumericalExpression(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1511 "bison.cpp" /* yacc.c:1646  */
    break;

  case 24:
#line 172 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(EQUALS)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1517 "bison.cpp" /* yacc.c:1646  */
    break;

  case 25:
#line 173 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(NOT_EQUALS)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1523 "bison.cpp" /* yacc.c:1646  */
    break;

  case 26:
#line 174 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(LESS_THAN)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1529 "bison.cpp" /* yacc.c:1646  */
    break;

  case 27:
#line 175 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(GREATER_THAN)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1535 "bison.cpp" /* yacc.c:1646  */
    break;

  case 28:
#line 176 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(LESS_EQ_THAN)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1541 "bison.cpp" /* yacc.c:1646  */
    break;

  case 29:
#line 177 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(GREATER_EQ_THAN)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1547 "bison.cpp" /* yacc.c:1646  */
    break;

  case 30:
#line 178 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new ComparisonPredicate(*(yyvsp[-2].valueExpression), std::shared_ptr<CompOp>(new CompOp(LIKE_COMP)), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1553 "bison.cpp" /* yacc.c:1646  */
    break;

  case 31:
#line 179 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new BooleanValueExpression(*(yyvsp[0].valueExpression))); delete (yyvsp[0].valueExpression);}
#line 1559 "bison.cpp" /* yacc.c:1646  */
    break;

  case 32:
#line 180 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new LogicalConjunction(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1565 "bison.cpp" /* yacc.c:1646  */
    break;

  case 33:
#line 181 "./grammar.y" /* yacc.c:1646  */
    {(yyval.valueExpression) = new ValueExpressionPtr (new LogicalDisjunction(*(yyvsp[-2].valueExpression), *(yyvsp[0].valueExpression))); delete (yyvsp[-2].valueExpression); delete (yyvsp[0].valueExpression);}
#line 1571 "bison.cpp" /* yacc.c:1646  */
    break;

  case 34:
#line 185 "./grammar.y" /* yacc.c:1646  */
    {(yyval.identifierChain) = new IdentifierChainPtr (new IdentifierChain(*(yyvsp[0].identifier), NULL)); delete (yyvsp[0].identifier);}
#line 1577 "bison.cpp" /* yacc.c:1646  */
    break;

  case 35:
#line 186 "./grammar.y" /* yacc.c:1646  */
    {(yyval.identifierChain) = new IdentifierChainPtr (new IdentifierChain(*(yyvsp[0].identifier), *(yyvsp[-2].identifierChain))); delete (yyvsp[-2].identifierChain); delete (yyvsp[0].identifier);}
#line 1583 "bison.cpp" /* yacc.c:1646  */
    break;

  case 36:
#line 190 "./grammar.y" /* yacc.c:1646  */
    {std::string literal((yyvsp[0].value)); (yyval.identifier) = new IdentifierPtr (new Identifier(literal));}
#line 1589 "bison.cpp" /* yacc.c:1646  */
    break;

  case 37:
#line 194 "./grammar.y" /* yacc.c:1646  */
    {(yyval.generalLiteral) = new std::shared_ptr<GeneralLiteral>(); *(yyval.generalLiteral) = PARSE(*(yyvsp[0].characterStringLiteral), GeneralLiteral); delete (yyvsp[0].characterStringLiteral);}
#line 1595 "bison.cpp" /* yacc.c:1646  */
    break;

  case 38:
#line 195 "./grammar.y" /* yacc.c:1646  */
    {(yyval.generalLiteral) = new std::shared_ptr<GeneralLiteral>(); *(yyval.generalLiteral) = PARSE(*(yyvsp[0].bitStringLiteral), GeneralLiteral); delete (yyvsp[0].bitStringLiteral);}
#line 1601 "bison.cpp" /* yacc.c:1646  */
    break;

  case 39:
#line 196 "./grammar.y" /* yacc.c:1646  */
    {(yyval.generalLiteral) = new std::shared_ptr<GeneralLiteral>(); *(yyval.generalLiteral) = PARSE(*(yyvsp[0].hexStringLiteral), GeneralLiteral); delete (yyvsp[0].hexStringLiteral);}
#line 1607 "bison.cpp" /* yacc.c:1646  */
    break;

  case 40:
#line 200 "./grammar.y" /* yacc.c:1646  */
    {(yyval.unsignedNumericLiteral) = new UnsignedNumericLiteralPtr (new UnsignedNumericLiteral(strtod((yyvsp[0].value),NULL)));}
#line 1613 "bison.cpp" /* yacc.c:1646  */
    break;

  case 41:
#line 201 "./grammar.y" /* yacc.c:1646  */
    {(yyval.unsignedNumericLiteral) = new UnsignedNumericLiteralPtr (new UnsignedNumericLiteral(strtod((yyvsp[0].value),NULL)));}
#line 1619 "bison.cpp" /* yacc.c:1646  */
    break;

  case 42:
#line 205 "./grammar.y" /* yacc.c:1646  */
    {(yyval.signedNumericLiteral) = new SignedNumericLiteralPtr (new SignedNumericLiteral(strtod((yyvsp[0].value), NULL)));}
#line 1625 "bison.cpp" /* yacc.c:1646  */
    break;

  case 43:
#line 206 "./grammar.y" /* yacc.c:1646  */
    {(yyval.signedNumericLiteral) = new SignedNumericLiteralPtr (new SignedNumericLiteral(strtod((yyvsp[0].value), NULL)));}
#line 1631 "bison.cpp" /* yacc.c:1646  */
    break;

  case 44:
#line 210 "./grammar.y" /* yacc.c:1646  */
    {std::string literal((yyvsp[0].value)); (yyval.characterStringLiteral) = new CharacterStringLiteralPtr (new CharacterStringLiteral(literal));}
#line 1637 "bison.cpp" /* yacc.c:1646  */
    break;

  case 45:
#line 214 "./grammar.y" /* yacc.c:1646  */
    {std::string literal((yyvsp[-1].value)); (yyval.bitStringLiteral) = new std::shared_ptr<BitStringLiteral>(new BitStringLiteral(literal));}
#line 1643 "bison.cpp" /* yacc.c:1646  */
    break;

  case 46:
#line 218 "./grammar.y" /* yacc.c:1646  */
    {std::string literal((yyvsp[-1].value)); (yyval.hexStringLiteral) = new std::shared_ptr<HexStringLiteral>(new HexStringLiteral(literal));}
#line 1649 "bison.cpp" /* yacc.c:1646  */
    break;


#line 1653 "bison.cpp" /* yacc.c:1646  */
      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (scanner, ret, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (scanner, ret, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, scanner, ret);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, scanner, ret);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (scanner, ret, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, scanner, ret);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[*yyssp], yyvsp, scanner, ret);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
#line 221 "./grammar.y" /* yacc.c:1906  */

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






