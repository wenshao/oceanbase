/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison implementation for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
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
#define YYBISON_VERSION "2.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1



/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"

#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     BINARY = 260,
     INTNUM = 261,
     DATE_VALUE = 262,
     HINT_VALUE = 263,
     BOOL = 264,
     APPROXNUM = 265,
     DECIMAL = 266,
     NULLX = 267,
     TRUE = 268,
     FALSE = 269,
     UNKNOWN = 270,
     EXCEPT = 271,
     UNION = 272,
     INTERSECT = 273,
     OR = 274,
     AND = 275,
     NOT = 276,
     COMP_NE = 277,
     COMP_GE = 278,
     COMP_GT = 279,
     COMP_EQ = 280,
     COMP_LT = 281,
     COMP_LE = 282,
     CNNOP = 283,
     LIKE = 284,
     BETWEEN = 285,
     IN = 286,
     IS = 287,
     MOD = 288,
     UMINUS = 289,
     ADD = 290,
     ANY = 291,
     ALL = 292,
     AS = 293,
     ASC = 294,
     BY = 295,
     CASE = 296,
     CHAR = 297,
     CREATE = 298,
     DATE = 299,
     DATETIME = 300,
     DELETE = 301,
     DESC = 302,
     DISTINCT = 303,
     DOUBLE = 304,
     ELSE = 305,
     END = 306,
     END_P = 307,
     ERROR = 308,
     EXISTS = 309,
     EXPLAIN = 310,
     FAVG = 311,
     FCOUNT = 312,
     FLOAT = 313,
     FMAX = 314,
     FMIN = 315,
     FROM = 316,
     FSUM = 317,
     FULL = 318,
     SYSFUNC = 319,
     GROUP = 320,
     HAVING = 321,
     IF = 322,
     INNER = 323,
     INTEGER = 324,
     INSERT = 325,
     INTO = 326,
     JOIN = 327,
     KEY = 328,
     LEFT = 329,
     LIMIT = 330,
     OFFSET = 331,
     ON = 332,
     ORDER = 333,
     OUTER = 334,
     PRIMARY = 335,
     RIGHT = 336,
     SELECT = 337,
     SET = 338,
     SMALLINT = 339,
     TABLE = 340,
     THEN = 341,
     UPDATE = 342,
     VALUES = 343,
     VARCHAR = 344,
     WHERE = 345,
     WHEN = 346
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  struct _ParseNode* node;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */


#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "sql_parser.lex.h"

#define YYLEX_PARAM result->yyscan_info_

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

extern ParseNode* merge_tree(void *malloc_pool, ObItemType node_tag, ParseNode* source_tree);

extern ParseNode* new_node(void *malloc_pool, ObItemType type, int num);

extern ParseNode* new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);

extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);




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
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
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
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
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
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
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
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
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
#   if ! defined malloc && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  29
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   966

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  103
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  55
/* YYNRULES -- Number of rules.  */
#define YYNRULES  187
/* YYNRULES -- Number of states.  */
#define YYNSTATES  356

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   346

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    37,     2,     2,
      41,    42,    35,    33,   102,    34,    43,    36,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   101,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    39,     2,     2,     2,     2,     2,
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
      25,    26,    27,    28,    29,    30,    31,    32,    38,    40,
      44,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      22,    23,    25,    29,    31,    35,    39,    41,    43,    45,
      47,    49,    51,    53,    55,    57,    59,    63,    69,    71,
      73,    75,    78,    80,    83,    86,    90,    94,    98,   102,
     106,   110,   114,   116,   119,   122,   126,   130,   134,   138,
     142,   146,   150,   154,   158,   162,   166,   170,   174,   178,
     183,   187,   191,   194,   198,   203,   207,   212,   216,   221,
     225,   230,   236,   243,   247,   252,   256,   258,   262,   268,
     270,   271,   273,   276,   281,   284,   285,   290,   296,   302,
     308,   314,   320,   324,   329,   334,   340,   342,   346,   350,
     358,   360,   364,   370,   374,   375,   378,   381,   383,   385,
     387,   392,   394,   395,   398,   405,   410,   418,   422,   423,
     425,   429,   433,   439,   441,   445,   447,   449,   453,   457,
     459,   462,   466,   468,   470,   479,   484,   489,   494,   495,
     498,   502,   507,   512,   515,   518,   523,   524,   528,   530,
     531,   535,   537,   541,   544,   545,   547,   549,   550,   553,
     554,   556,   558,   560,   563,   567,   569,   571,   575,   577,
     581,   583,   587,   590,   592,   596,   599,   601,   607,   609,
     613,   620,   626,   629,   632,   635,   637,   639
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     104,     0,    -1,   105,    61,    -1,   105,   101,   106,    -1,
     106,    -1,   135,    -1,   130,    -1,   124,    -1,   121,    -1,
     120,    -1,    -1,   112,    -1,   107,   102,   112,    -1,     3,
      -1,     3,    43,     3,    -1,     3,    43,    35,    -1,     4,
      -1,     5,    -1,     7,    -1,     6,    -1,    10,    -1,    11,
      -1,     9,    -1,    12,    -1,   108,    -1,   109,    -1,    41,
     112,    42,    -1,    41,   107,   102,   112,    42,    -1,   114,
      -1,   119,    -1,   136,    -1,    63,   136,    -1,   110,    -1,
      33,   111,    -1,    34,   111,    -1,   111,    33,   111,    -1,
     111,    34,   111,    -1,   111,    35,   111,    -1,   111,    36,
     111,    -1,   111,    37,   111,    -1,   111,    39,   111,    -1,
     111,    38,   111,    -1,   110,    -1,    33,   112,    -1,    34,
     112,    -1,   112,    33,   112,    -1,   112,    34,   112,    -1,
     112,    35,   112,    -1,   112,    36,   112,    -1,   112,    37,
     112,    -1,   112,    39,   112,    -1,   112,    38,   112,    -1,
     112,    27,   112,    -1,   112,    26,   112,    -1,   112,    25,
     112,    -1,   112,    23,   112,    -1,   112,    24,   112,    -1,
     112,    22,   112,    -1,   112,    29,   112,    -1,   112,    21,
      29,   112,    -1,   112,    20,   112,    -1,   112,    19,   112,
      -1,    21,   112,    -1,   112,    32,    12,    -1,   112,    32,
      21,    12,    -1,   112,    32,    13,    -1,   112,    32,    21,
      13,    -1,   112,    32,    14,    -1,   112,    32,    21,    14,
      -1,   112,    32,    15,    -1,   112,    32,    21,    15,    -1,
     112,    30,   111,    20,   111,    -1,   112,    21,    30,   111,
      20,   111,    -1,   112,    31,   113,    -1,   112,    21,    31,
     113,    -1,   112,    28,   112,    -1,   136,    -1,    41,   107,
      42,    -1,    50,   115,   116,   118,    60,    -1,   112,    -1,
      -1,   117,    -1,   116,   117,    -1,   100,   112,    95,   112,
      -1,    59,   112,    -1,    -1,    66,    41,    35,    42,    -1,
      66,    41,   149,   112,    42,    -1,    71,    41,   149,   112,
      42,    -1,    65,    41,   149,   112,    42,    -1,    69,    41,
     149,   112,    42,    -1,    68,    41,   149,   112,    42,    -1,
      73,    41,    42,    -1,    73,    41,   107,    42,    -1,    55,
      70,   154,   140,    -1,    96,   154,    92,   122,   140,    -1,
     123,    -1,   122,   102,   123,    -1,     3,    25,   112,    -1,
      52,    94,   129,   153,    41,   125,    42,    -1,   126,    -1,
     125,   102,   126,    -1,    89,    82,    41,   132,    42,    -1,
       3,   128,   127,    -1,    -1,    21,    12,    -1,    89,    82,
      -1,    78,    -1,    67,    -1,    58,    -1,    98,    41,     6,
      42,    -1,    54,    -1,    -1,    76,    63,    -1,    79,    80,
     154,   131,    97,   133,    -1,    79,    80,   154,   135,    -1,
      79,    80,   154,    41,   132,    42,   135,    -1,    41,   132,
      42,    -1,    -1,     3,    -1,   132,   102,     3,    -1,    41,
     134,    42,    -1,   133,   102,    41,   134,    42,    -1,   112,
      -1,   134,   102,   112,    -1,   137,    -1,   136,    -1,    41,
     137,    42,    -1,    41,   136,    42,    -1,   139,    -1,   138,
     144,    -1,   138,   143,   141,    -1,   139,    -1,   136,    -1,
      91,   149,   151,    70,   152,   140,   142,   148,    -1,   138,
      17,   149,   138,    -1,   138,    18,   149,   138,    -1,   138,
      16,   149,   138,    -1,    -1,    99,   112,    -1,    99,     8,
     112,    -1,    84,     6,    85,     6,    -1,    85,     6,    84,
       6,    -1,    84,     6,    -1,    85,     6,    -1,    84,     6,
     102,     6,    -1,    -1,    74,    49,   107,    -1,   144,    -1,
      -1,    87,    49,   145,    -1,   146,    -1,   145,   102,   146,
      -1,   112,   147,    -1,    -1,    48,    -1,    56,    -1,    -1,
      75,   112,    -1,    -1,    46,    -1,    57,    -1,   112,    -1,
     112,     3,    -1,   112,    47,     3,    -1,    35,    -1,   150,
      -1,   151,   102,   150,    -1,   153,    -1,   152,   102,   153,
      -1,   154,    -1,   154,    47,     3,    -1,   154,     3,    -1,
     136,    -1,   136,    47,     3,    -1,   136,     3,    -1,   155,
      -1,    41,   155,    42,    47,     3,    -1,     3,    -1,    41,
     155,    42,    -1,   153,   156,    81,   153,    86,   112,    -1,
     153,    81,   153,    86,   112,    -1,    72,   157,    -1,    83,
     157,    -1,    90,   157,    -1,    77,    -1,    88,    -1,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   169,   169,   177,   184,   191,   192,   193,   194,   195,
     196,   207,   211,   218,   220,   225,   233,   234,   235,   236,
     237,   238,   239,   240,   244,   246,   248,   250,   254,   262,
     266,   270,   278,   279,   283,   287,   288,   289,   290,   291,
     292,   293,   296,   297,   301,   305,   306,   307,   308,   309,
     310,   311,   312,   313,   314,   315,   316,   317,   318,   319,
     320,   324,   328,   332,   336,   340,   344,   348,   352,   356,
     360,   364,   368,   372,   376,   380,   387,   391,   396,   403,
     404,   408,   410,   415,   422,   423,   427,   432,   436,   440,
     444,   448,   452,   458,   474,   488,   496,   500,   507,   522,
     530,   532,   539,   544,   552,   553,   559,   564,   566,   568,
     570,   572,   578,   579,   590,   600,   609,   622,   627,   631,
     632,   639,   643,   649,   650,   664,   666,   671,   672,   676,
     677,   686,   706,   707,   711,   732,   750,   769,   791,   792,
     796,   803,   807,   811,   815,   819,   827,   828,   835,   836,
     840,   847,   849,   854,   862,   863,   865,   871,   872,   880,
     883,   887,   894,   899,   905,   911,   920,   924,   931,   933,
     938,   942,   946,   950,   956,   960,   964,   968,   977,   985,
     989,   993,  1001,  1007,  1013,  1019,  1026,  1027
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "BINARY", "INTNUM",
  "DATE_VALUE", "HINT_VALUE", "BOOL", "APPROXNUM", "DECIMAL", "NULLX",
  "TRUE", "FALSE", "UNKNOWN", "EXCEPT", "UNION", "INTERSECT", "OR", "AND",
  "NOT", "COMP_NE", "COMP_GE", "COMP_GT", "COMP_EQ", "COMP_LT", "COMP_LE",
  "CNNOP", "LIKE", "BETWEEN", "IN", "IS", "'+'", "'-'", "'*'", "'/'",
  "'%'", "MOD", "'^'", "UMINUS", "'('", "')'", "'.'", "ADD", "ANY", "ALL",
  "AS", "ASC", "BY", "CASE", "CHAR", "CREATE", "DATE", "DATETIME",
  "DELETE", "DESC", "DISTINCT", "DOUBLE", "ELSE", "END", "END_P", "ERROR",
  "EXISTS", "EXPLAIN", "FAVG", "FCOUNT", "FLOAT", "FMAX", "FMIN", "FROM",
  "FSUM", "FULL", "SYSFUNC", "GROUP", "HAVING", "IF", "INNER", "INTEGER",
  "INSERT", "INTO", "JOIN", "KEY", "LEFT", "LIMIT", "OFFSET", "ON",
  "ORDER", "OUTER", "PRIMARY", "RIGHT", "SELECT", "SET", "SMALLINT",
  "TABLE", "THEN", "UPDATE", "VALUES", "VARCHAR", "WHERE", "WHEN", "';'",
  "','", "$accept", "sql_stmt", "stmt_list", "stmt", "expr_list",
  "column_ref", "expr_const", "simple_expr", "arith_expr", "expr",
  "in_expr", "case_expr", "case_arg", "when_clause_list", "when_clause",
  "case_default", "func_expr", "delete_stmt", "update_stmt",
  "update_asgn_list", "update_asgn_factor", "create_table_stmt",
  "create_col_list", "create_definition", "column_atts", "data_type",
  "opt_if_not_exists", "insert_stmt", "opt_insert_columns", "column_list",
  "insert_vals_list", "insert_vals", "select_stmt", "select_with_parens",
  "select_no_parens", "select_clause", "simple_select", "opt_where",
  "select_limit", "opt_groupby", "opt_order_by", "order_by", "sort_list",
  "sort_key", "opt_asc_desc", "opt_having", "opt_distinct", "projection",
  "select_expr_list", "from_list", "table_factor", "relation_factor",
  "joined_table", "join_type", "join_outer", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,    43,    45,    42,    47,    37,   288,    94,
     289,    40,    41,    46,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,   314,   315,
     316,   317,   318,   319,   320,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,    59,    44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,   103,   104,   105,   105,   106,   106,   106,   106,   106,
     106,   107,   107,   108,   108,   108,   109,   109,   109,   109,
     109,   109,   109,   109,   110,   110,   110,   110,   110,   110,
     110,   110,   111,   111,   111,   111,   111,   111,   111,   111,
     111,   111,   112,   112,   112,   112,   112,   112,   112,   112,
     112,   112,   112,   112,   112,   112,   112,   112,   112,   112,
     112,   112,   112,   112,   112,   112,   112,   112,   112,   112,
     112,   112,   112,   112,   112,   112,   113,   113,   114,   115,
     115,   116,   116,   117,   118,   118,   119,   119,   119,   119,
     119,   119,   119,   119,   120,   121,   122,   122,   123,   124,
     125,   125,   126,   126,   127,   127,   127,   128,   128,   128,
     128,   128,   129,   129,   130,   130,   130,   131,   131,   132,
     132,   133,   133,   134,   134,   135,   135,   136,   136,   137,
     137,   137,   138,   138,   139,   139,   139,   139,   140,   140,
     140,   141,   141,   141,   141,   141,   142,   142,   143,   143,
     144,   145,   145,   146,   147,   147,   147,   148,   148,   149,
     149,   149,   150,   150,   150,   150,   151,   151,   152,   152,
     153,   153,   153,   153,   153,   153,   153,   153,   154,   155,
     155,   155,   156,   156,   156,   156,   157,   157
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     1,
       0,     1,     3,     1,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     3,     5,     1,     1,
       1,     2,     1,     2,     2,     3,     3,     3,     3,     3,
       3,     3,     1,     2,     2,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     4,
       3,     3,     2,     3,     4,     3,     4,     3,     4,     3,
       4,     5,     6,     3,     4,     3,     1,     3,     5,     1,
       0,     1,     2,     4,     2,     0,     4,     5,     5,     5,
       5,     5,     3,     4,     4,     5,     1,     3,     3,     7,
       1,     3,     5,     3,     0,     2,     2,     1,     1,     1,
       4,     1,     0,     2,     6,     4,     7,     3,     0,     1,
       3,     3,     5,     1,     3,     1,     1,     3,     3,     1,
       2,     3,     1,     1,     8,     4,     4,     4,     0,     2,
       3,     4,     4,     2,     2,     4,     0,     3,     1,     0,
       3,     1,     3,     2,     0,     1,     1,     0,     2,     0,
       1,     1,     1,     2,     3,     1,     1,     3,     1,     3,
       1,     3,     2,     1,     3,     2,     1,     5,     1,     3,
       6,     5,     2,     2,     2,     1,     1,     0
};

/* YYDEFACT[STATE-NAME] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
      10,     0,     0,     0,     0,   159,     0,     0,     0,     4,
       9,     8,     7,     6,     5,   133,   125,   149,   132,   133,
       0,   112,     0,     0,   160,   161,     0,   178,     0,     1,
       2,    10,   159,   159,   159,     0,     0,   130,   128,   127,
       0,     0,   138,   118,    13,    16,    17,    19,    18,    22,
      20,    21,    23,     0,     0,     0,   165,     0,    80,     0,
       0,     0,     0,     0,     0,     0,    24,    25,    42,   162,
      28,    29,    30,   166,     0,     0,     3,     0,     0,     0,
       0,     0,     0,   131,   113,     0,   173,     0,   170,   176,
       0,    94,     0,     0,   115,     0,    62,    43,    44,     0,
      11,    30,    79,     0,    31,   159,   159,   159,   159,   159,
       0,   163,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   138,    96,   133,
     137,   132,   135,   136,   154,   150,   151,   143,   144,   133,
       0,   176,   175,     0,     0,   187,   185,     0,   187,   187,
       0,   172,     0,     0,   139,   119,     0,     0,    14,    15,
       0,    26,     0,    85,    81,     0,     0,     0,     0,     0,
       0,    92,     0,    11,    61,    60,     0,     0,     0,    57,
      55,    56,    54,    53,    52,    75,    58,     0,     0,    32,
       0,     0,    73,    76,    63,    65,    67,    69,     0,    45,
      46,    47,    48,    49,    51,    50,   164,   138,   168,   167,
       0,     0,    95,   155,   156,   153,     0,     0,     0,     0,
     179,   174,     0,     0,     0,   100,   186,   182,     0,   183,
     184,     0,   171,   140,   117,     0,     0,   114,    12,     0,
       0,    82,     0,     0,    86,     0,     0,     0,     0,    93,
       0,    59,     0,    74,    33,    34,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    64,    66,    68,    70,     0,
     146,    98,    97,   152,   141,   145,   142,     0,   111,   109,
     108,   107,     0,   104,     0,    99,     0,     0,     0,   116,
     120,   123,     0,     0,    27,     0,    84,    78,    89,    87,
      91,    90,    88,    12,     0,    71,    35,    36,    37,    38,
      39,    41,    40,    77,   169,     0,   157,   177,     0,     0,
       0,   103,     0,   101,   181,     0,   121,     0,     0,    83,
      72,     0,     0,   134,     0,   105,   106,     0,   180,   124,
       0,   147,   158,   110,   102,   122
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,     7,     8,     9,    99,    66,    67,    68,   200,   183,
     202,    70,   103,   173,   174,   252,    71,    10,    11,   137,
     138,    12,   234,   235,   331,   293,    41,    13,    93,   166,
     247,   302,    14,    72,    20,    17,    18,    91,    83,   326,
      36,    37,   145,   146,   225,   343,    26,    73,    74,   217,
      87,    88,    89,   160,   237
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -134
static const yytype_int16 yypact[] =
{
      86,   -15,   -50,   -14,   -29,    90,    72,   128,   -48,  -134,
    -134,  -134,  -134,  -134,  -134,   -27,  -134,    -2,   -31,   106,
     125,   105,    72,    72,  -134,  -134,   365,  -134,    92,  -134,
    -134,    86,    90,    90,    90,   137,    46,    91,  -134,  -134,
     127,    19,    84,    -3,   150,  -134,  -134,  -134,  -134,  -134,
    -134,  -134,  -134,   534,   534,   534,  -134,   255,   534,   147,
     158,   160,   162,   165,   166,   180,  -134,  -134,  -134,   631,
    -134,  -134,  -134,  -134,    10,   219,  -134,   -15,   -15,   -15,
     534,   217,   221,  -134,  -134,    16,    20,   154,    32,  -134,
     436,  -134,    17,   136,  -134,   110,   905,  -134,  -134,   123,
     698,   145,   866,   139,  -134,    90,     4,    90,    90,    90,
     485,  -134,   534,   534,    85,   534,   534,   534,   534,   534,
     534,   534,   534,   576,   199,   390,   534,   534,   534,   534,
     534,   534,   534,   239,    19,   365,   218,   -56,  -134,  -134,
     227,  -134,   227,  -134,   660,   153,  -134,   -60,   173,   108,
     311,   226,  -134,   260,    21,   182,  -134,    19,   182,   182,
     191,  -134,   270,   534,   866,  -134,   -39,   234,  -134,  -134,
     534,  -134,   534,   -38,  -134,   534,   235,   534,   534,   534,
     534,  -134,   -36,   866,   886,   905,   534,   576,   199,   917,
     917,   917,   917,   917,   917,   429,   927,   576,   576,  -134,
     265,   255,  -134,  -134,  -134,  -134,  -134,  -134,   144,   134,
     134,   243,   243,   243,   243,  -134,  -134,   -47,   311,  -134,
     534,   219,  -134,  -134,  -134,  -134,   534,   280,   284,   285,
     245,  -134,   138,   211,   -34,  -134,  -134,  -134,   197,  -134,
    -134,    19,  -134,   866,   -15,   291,   534,   195,   722,   328,
     534,  -134,   246,   746,  -134,   770,   794,   818,   842,  -134,
     534,   927,   297,  -134,  -134,  -134,   576,   576,   576,   576,
     576,   576,   576,   576,   -33,  -134,  -134,  -134,  -134,    19,
     233,   866,  -134,  -134,  -134,  -134,  -134,   305,  -134,  -134,
    -134,  -134,   269,   -16,   272,  -134,    21,   534,   301,  -134,
    -134,   866,   -30,   273,  -134,   534,   866,  -134,  -134,  -134,
    -134,  -134,  -134,   866,   576,   304,   381,   381,   276,   276,
     276,   276,  -134,  -134,   311,   278,   247,  -134,   319,   317,
     262,  -134,   342,  -134,   866,   534,  -134,   534,   534,   866,
     304,   534,   534,  -134,   337,  -134,  -134,   -25,   866,   866,
     -24,   279,   866,  -134,  -134,  -134
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -134,  -134,  -134,   349,  -103,  -134,  -134,   -58,   -19,   -26,
     201,  -134,  -134,  -134,   212,  -134,  -134,  -134,  -134,  -134,
     169,  -134,  -134,    97,  -134,  -134,  -134,  -134,  -134,    63,
    -134,    58,   -42,    40,     2,    56,   140,  -133,  -134,  -134,
    -134,  -134,  -134,   171,  -134,  -134,    15,   274,  -134,  -134,
     -75,   120,   322,  -134,  -122
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -174
static const yytype_int16 yytable[] =
{
      69,    94,    16,   244,   222,   329,   259,   182,   295,   323,
     150,  -129,   336,    30,    32,    33,    34,   354,   355,    27,
     165,   250,    27,   152,   232,   227,     1,    96,    97,    98,
    -129,   100,   102,    16,  -126,   161,   239,   240,    92,   176,
      15,    19,   228,    90,    21,    16,   221,    77,    78,    79,
      24,    23,    90,    31,   144,   279,    22,    85,     1,   218,
      85,    25,   172,   245,   164,   199,   260,   153,   296,   260,
    -129,    15,   337,   330,  -126,    27,     5,   245,   337,   162,
     134,    86,   238,    15,   280,    35,   184,   185,     5,   189,
     190,   191,   192,   193,   194,   195,   196,   101,   274,   104,
     209,   210,   211,   212,   213,   214,   215,     5,     5,    69,
     233,   152,   135,   168,   186,   187,   188,   139,   139,   139,
     175,   177,   178,   179,   180,   149,    28,     1,    29,   199,
      81,    82,    19,   140,   142,   143,    24,   243,     2,   199,
     199,     3,    42,    43,   248,   169,   249,    25,    38,   253,
      38,   255,   256,   257,   258,   153,   275,   276,   277,   278,
     261,  -133,  -133,  -133,   203,     4,   298,    39,   262,   128,
     129,   130,   131,   132,    86,  -148,  -148,     5,   264,   265,
    -173,    40,     6,    90,    75,  -173,    80,    38,     1,  -173,
      84,  -173,   288,    95,   281,   154,   289,    86,  -173,   105,
     144,   106,   299,   107,   324,   290,   108,   109,   199,   199,
     199,   199,   199,   199,   199,   199,   291,   141,   141,   141,
     301,   110,   136,   147,   306,   170,   155,   148,   203,  -133,
    -133,   156,  -133,   167,   313,   157,   292,   158,   351,   172,
     201,   101,   216,   220,   159,    34,    16,   315,   316,   317,
     318,   319,   320,   321,   322,   226,   199,   229,    44,    45,
      46,    47,    48,   231,    49,    50,    51,    52,   230,   155,
     236,   334,   241,   242,   156,   246,    53,   254,   157,   339,
     158,    86,   132,   297,    15,   266,   284,   159,    54,    55,
     285,   286,   287,   294,   300,   340,    57,   303,   267,   268,
     269,   270,   271,   272,   273,    58,   307,   325,   327,   348,
     328,   349,   301,   332,   338,   273,   352,   314,    59,    86,
      60,    61,   342,    62,    63,   344,    64,   341,    65,   345,
     267,   268,   269,   270,   271,   272,   273,   267,   268,   269,
     270,   271,   272,   273,   346,   165,     5,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,    44,    45,
      46,    47,    48,   155,    49,    50,    51,    52,   156,   353,
      76,   260,   157,   155,   158,   251,    53,   335,   156,   263,
     282,   159,   157,   333,   158,   347,   350,   283,    54,    55,
      56,   159,   204,   205,   206,   207,    57,   151,     0,   219,
       0,   208,     0,     0,     0,    58,   269,   270,   271,   272,
     273,     0,     0,   305,     0,     0,     0,     0,    59,     0,
      60,    61,     0,    62,    63,     0,    64,     0,    65,    44,
      45,    46,    47,    48,   163,    49,    50,    51,    52,     0,
       0,     0,     0,     0,     0,     0,     0,    53,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,    54,
      55,     0,     0,     0,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,    58,     0,    44,    45,
      46,    47,    48,     0,    49,    50,    51,    52,     0,    59,
       0,    60,    61,     0,    62,    63,    53,    64,     0,    65,
       0,     0,     0,     0,     0,     0,     0,     0,    54,    55,
       0,     0,     0,     0,     0,     0,    57,   181,     0,     0,
       0,     0,     0,     0,     0,    58,     0,    44,    45,    46,
      47,    48,     0,    49,    50,    51,    52,     0,    59,     0,
      60,    61,     0,    62,    63,    53,    64,     0,    65,     0,
       0,     0,     0,     0,     0,     0,     0,    54,    55,     0,
       0,     0,     0,     0,     0,    57,     0,     0,     0,    44,
      45,    46,    47,    48,    58,    49,    50,    51,    52,     0,
       0,     0,     0,     0,     0,     0,     0,    59,     0,    60,
      61,     0,    62,    63,     0,    64,     0,    65,     0,   197,
     198,     0,     0,     0,     0,     0,     0,    57,     0,     0,
       0,     0,     0,     0,     0,     0,    58,     0,     0,     0,
       0,     0,     0,     0,   111,     0,     0,     0,     0,    59,
       0,    60,    61,     0,    62,    63,     0,    64,     0,    65,
     112,   113,   114,   115,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,   131,
     132,     0,     0,     0,     0,     0,     0,     0,   133,   112,
     113,   114,   115,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,   131,   132,
       0,     0,     0,     0,     0,     0,     0,     0,   223,     0,
       0,     0,     0,     0,     0,     0,   224,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,     0,     0,
     171,   112,   113,   114,   115,   116,   117,   118,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,     0,     0,   304,   112,   113,   114,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,     0,     0,   308,   112,
     113,   114,   115,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,   131,   132,
       0,     0,   309,   112,   113,   114,   115,   116,   117,   118,
     119,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,   131,   132,     0,     0,   310,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,     0,     0,
     311,   112,   113,   114,   115,   116,   117,   118,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
     131,   132,     0,     0,   312,   112,   113,   114,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   113,   114,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   114,   115,   116,   117,
     118,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,   131,   132,   121,   122,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132,   123,   124,   125,
     126,   127,   128,   129,   130,   131,   132
};

#define yypact_value_is_default(yystate) \
  ((yystate) == (-134))

#define yytable_value_is_error(yytable_value) \
  YYID (0)

static const yytype_int16 yycheck[] =
{
      26,    43,     0,    42,   137,    21,    42,   110,    42,    42,
      85,    42,    42,    61,    16,    17,    18,    42,    42,     3,
       3,    59,     3,     3,     3,    85,    41,    53,    54,    55,
      61,    57,    58,    31,    61,     3,   158,   159,    41,    35,
       0,     1,   102,    99,    94,    43,   102,    32,    33,    34,
      46,    80,    99,   101,    80,   102,    70,    41,    41,   134,
      41,    57,   100,   102,    90,   123,   102,    47,   102,   102,
     101,    31,   102,    89,   101,     3,    91,   102,   102,    47,
      70,    41,   157,    43,   217,    87,   112,   113,    91,   115,
     116,   117,   118,   119,   120,   121,   122,    57,   201,    59,
     126,   127,   128,   129,   130,   131,   132,    91,    91,   135,
      89,     3,   102,     3,    29,    30,    31,    77,    78,    79,
     105,   106,   107,   108,   109,    85,     6,    41,     0,   187,
      84,    85,    92,    77,    78,    79,    46,   163,    52,   197,
     198,    55,    22,    23,   170,    35,   172,    57,    42,   175,
      42,   177,   178,   179,   180,    47,    12,    13,    14,    15,
     186,    16,    17,    18,   124,    79,   241,    42,   187,    35,
      36,    37,    38,    39,   134,    84,    85,    91,   197,   198,
      72,    76,    96,    99,    92,    77,    49,    42,    41,    81,
      63,    83,    54,    43,   220,    41,    58,   157,    90,    41,
     226,    41,   244,    41,   279,    67,    41,    41,   266,   267,
     268,   269,   270,   271,   272,   273,    78,    77,    78,    79,
     246,    41,     3,     6,   250,   102,    72,     6,   188,    84,
      85,    77,    87,    97,   260,    81,    98,    83,   341,   100,
      41,   201,     3,    25,    90,    18,   244,   266,   267,   268,
     269,   270,   271,   272,   273,   102,   314,    84,     3,     4,
       5,     6,     7,     3,     9,    10,    11,    12,    42,    72,
      88,   297,    81,     3,    77,    41,    21,    42,    81,   305,
      83,   241,    39,    86,   244,    20,     6,    90,    33,    34,
       6,     6,    47,    82,     3,   314,    41,   102,    33,    34,
      35,    36,    37,    38,    39,    50,    60,    74,     3,   335,
      41,   337,   338,    41,    41,    39,   342,    20,    63,   279,
      65,    66,    75,    68,    69,     6,    71,    49,    73,    12,
      33,    34,    35,    36,    37,    38,    39,    33,    34,    35,
      36,    37,    38,    39,    82,     3,    91,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,     3,     4,
       5,     6,     7,    72,     9,    10,    11,    12,    77,    42,
      31,   102,    81,    72,    83,   173,    21,    86,    77,   188,
     221,    90,    81,   296,    83,   332,   338,   226,    33,    34,
      35,    90,    12,    13,    14,    15,    41,    85,    -1,   135,
      -1,    21,    -1,    -1,    -1,    50,    35,    36,    37,    38,
      39,    -1,    -1,    95,    -1,    -1,    -1,    -1,    63,    -1,
      65,    66,    -1,    68,    69,    -1,    71,    -1,    73,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    21,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    39,    33,
      34,    -1,    -1,    -1,    -1,    -1,    -1,    41,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    50,    -1,     3,     4,
       5,     6,     7,    -1,     9,    10,    11,    12,    -1,    63,
      -1,    65,    66,    -1,    68,    69,    21,    71,    -1,    73,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    33,    34,
      -1,    -1,    -1,    -1,    -1,    -1,    41,    42,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    50,    -1,     3,     4,     5,
       6,     7,    -1,     9,    10,    11,    12,    -1,    63,    -1,
      65,    66,    -1,    68,    69,    21,    71,    -1,    73,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    33,    34,    -1,
      -1,    -1,    -1,    -1,    -1,    41,    -1,    -1,    -1,     3,
       4,     5,     6,     7,    50,     9,    10,    11,    12,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    63,    -1,    65,
      66,    -1,    68,    69,    -1,    71,    -1,    73,    -1,    33,
      34,    -1,    -1,    -1,    -1,    -1,    -1,    41,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    50,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    63,
      -1,    65,    66,    -1,    68,    69,    -1,    71,    -1,    73,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    47,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    39,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    48,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    56,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    -1,    -1,
      42,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    39,    -1,    -1,    42,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    39,    -1,    -1,    42,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    39,
      -1,    -1,    42,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    39,    -1,    -1,    42,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    -1,    -1,
      42,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    39,    -1,    -1,    42,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    39,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    39,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    39,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    39
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    41,    52,    55,    79,    91,    96,   104,   105,   106,
     120,   121,   124,   130,   135,   136,   137,   138,   139,   136,
     137,    94,    70,    80,    46,    57,   149,     3,   154,     0,
      61,   101,    16,    17,    18,    87,   143,   144,    42,    42,
      76,   129,   154,   154,     3,     4,     5,     6,     7,     9,
      10,    11,    12,    21,    33,    34,    35,    41,    50,    63,
      65,    66,    68,    69,    71,    73,   108,   109,   110,   112,
     114,   119,   136,   150,   151,    92,   106,   149,   149,   149,
      49,    84,    85,   141,    63,    41,   136,   153,   154,   155,
      99,   140,    41,   131,   135,    43,   112,   112,   112,   107,
     112,   136,   112,   115,   136,    41,    41,    41,    41,    41,
      41,     3,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    39,    47,    70,   102,     3,   122,   123,   136,
     138,   139,   138,   138,   112,   145,   146,     6,     6,   136,
     153,   155,     3,    47,    41,    72,    77,    81,    83,    90,
     156,     3,    47,     8,   112,     3,   132,    97,     3,    35,
     102,    42,   100,   116,   117,   149,    35,   149,   149,   149,
     149,    42,   107,   112,   112,   112,    29,    30,    31,   112,
     112,   112,   112,   112,   112,   112,   112,    33,    34,   110,
     111,    41,   113,   136,    12,    13,    14,    15,    21,   112,
     112,   112,   112,   112,   112,   112,     3,   152,   153,   150,
      25,   102,   140,    48,    56,   147,   102,    85,   102,    84,
      42,     3,     3,    89,   125,   126,    88,   157,   153,   157,
     157,    81,     3,   112,    42,   102,    41,   133,   112,   112,
      59,   117,   118,   112,    42,   112,   112,   112,   112,    42,
     102,   112,   111,   113,   111,   111,    20,    33,    34,    35,
      36,    37,    38,    39,   107,    12,    13,    14,    15,   102,
     140,   112,   123,   146,     6,     6,     6,    47,    54,    58,
      67,    78,    98,   128,    82,    42,   102,    86,   153,   135,
       3,   112,   134,   102,    42,    95,   112,    60,    42,    42,
      42,    42,    42,   112,    20,   111,   111,   111,   111,   111,
     111,   111,   111,    42,   153,    74,   142,     3,    41,    21,
      89,   127,    41,   126,   112,    86,    42,   102,    41,   112,
     111,    49,    75,   148,     6,    12,    82,   132,   112,   112,
     134,   107,   112,    42,    42,    42
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  However,
   YYFAIL appears to be in use.  Nevertheless, it is formally deprecated
   in Bison 2.4.2's NEWS entry, where a plan to phase it out is
   discussed.  */

#define YYFAIL		goto yyerrlab
#if defined YYFAIL
  /* This is here to suppress warnings from the GCC cpp's
     -Wunused-macros.  Normally we don't worry about that warning, but
     some users do, and we want to make it easy for users to remove
     YYFAIL uses, which will produce warnings from Bison 2.5.  */
#endif

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

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
#ifndef	YYINITDEPTH
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
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
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
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
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
  YYSIZE_T yysize0 = yytnamerr (0, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  YYSIZE_T yysize1;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = 0;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - Assume YYFAIL is not used.  It's too flawed to consider.  See
       <http://lists.gnu.org/archive/html/bison-patches/2009-12/msg00024.html>
       for details.  YYERROR is fine as it does not invoke this
       function.
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
                yysize1 = yysize + yytnamerr (0, yytname[yyx]);
                if (! (yysize <= yysize1
                       && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                  return 2;
                yysize = yysize1;
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

  yysize1 = yysize + yystrlen (yyformat);
  if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
    return 2;
  yysize = yysize1;

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

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "NAME" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 4: /* "STRING" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 5: /* "BINARY" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 6: /* "INTNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 7: /* "DATE_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 8: /* "HINT_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 9: /* "BOOL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 10: /* "APPROXNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 11: /* "DECIMAL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 12: /* "NULLX" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 13: /* "TRUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 14: /* "FALSE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 15: /* "UNKNOWN" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 73: /* "SYSFUNC" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 104: /* "sql_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 105: /* "stmt_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 106: /* "stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 107: /* "expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 108: /* "column_ref" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 109: /* "expr_const" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 110: /* "simple_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 111: /* "arith_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 112: /* "expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 113: /* "in_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 114: /* "case_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 115: /* "case_arg" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 116: /* "when_clause_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 117: /* "when_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 118: /* "case_default" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 119: /* "func_expr" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 120: /* "delete_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 121: /* "update_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 122: /* "update_asgn_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 123: /* "update_asgn_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 124: /* "create_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 125: /* "create_col_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 126: /* "create_definition" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 127: /* "column_atts" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 128: /* "data_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 129: /* "opt_if_not_exists" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 130: /* "insert_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 131: /* "opt_insert_columns" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 132: /* "column_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 133: /* "insert_vals_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 134: /* "insert_vals" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 135: /* "select_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 136: /* "select_with_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 137: /* "select_no_parens" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 138: /* "select_clause" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 139: /* "simple_select" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 140: /* "opt_where" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 141: /* "select_limit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 142: /* "opt_groupby" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 143: /* "opt_order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 144: /* "order_by" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 145: /* "sort_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 146: /* "sort_key" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 147: /* "opt_asc_desc" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 148: /* "opt_having" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 149: /* "opt_distinct" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 150: /* "projection" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 151: /* "select_expr_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 152: /* "from_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 153: /* "table_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 154: /* "relation_factor" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 155: /* "joined_table" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 156: /* "join_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 157: /* "join_outer" */

	{destroy_tree((yyvaluep->node));};

	break;

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

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
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
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
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

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
      yychar = YYLEX;
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
  *++yyvsp = yylval;
  *++yylsp = yylloc;
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
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      (yyval.node) = result->result_tree_ = merge_tree(result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      YYACCEPT;
    }
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    }
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    }
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 10:

    { (yyval.node) = NULL; }
    break;

  case 11:

    { 
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 12:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 14:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 15:

    {
      ParseNode* node = new_node(result->malloc_pool_, T_STAR, 0);
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), node);
    }
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 26:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 27:

    { 
      (yyval.node) = merge_tree(result->malloc_pool_, T_EXPR_LIST, new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)));
    }
    break;

  case 28:

    { 
      (yyval.node) = (yyvsp[(1) - (1)].node); 
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
    break;

  case 29:

    { 
      (yyval.node) = (yyvsp[(1) - (1)].node); 
    }
    break;

  case 30:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 31:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 32:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 33:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 34:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 35:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 36:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 37:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 38:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 39:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 40:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 41:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 42:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 43:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 44:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 45:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 46:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 47:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 48:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 49:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 50:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 51:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 52:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 53:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 54:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 55:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 56:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 57:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 58:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 59:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); }
    break;

  case 60:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 61:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 62:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 63:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 64:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); 
    }
    break;

  case 65:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 66:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); 
    }
    break;

  case 67:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 68:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); 
    }
    break;

  case 69:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 70:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); 
    }
    break;

  case 71:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 72:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 73:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 74:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 75:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 76:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 77:

    { (yyval.node) = merge_tree(result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); }
    break;

  case 78:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), merge_tree(result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node)), (yyvsp[(4) - (5)].node));
    }
    break;

  case 79:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 80:

    { (yyval.node) = NULL; }
    break;

  case 81:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 82:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 83:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 84:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 85:

    { (yyval.node) = new_node(result->malloc_pool_, T_NULL, 0); }
    break;

  case 86:

    { 
      ParseNode* node = new_node(result->malloc_pool_, T_STAR, 0);
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_COUNT, 1, node); 
    }
    break;

  case 87:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node)); 
    }
    break;

  case 88:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node)); 
    }
    break;

  case 89:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node)); 
    }
    break;

  case 90:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node)); 
    }
    break;

  case 91:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node)); 
    }
    break;

  case 92:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node)); 
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
    break;

  case 93:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), merge_tree(result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node))); 
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
    break;

  case 94:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_DELETE, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 95:

    {
      ParseNode* assign_list = merge_tree(result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(4) - (5)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_UPDATE, 3, (yyvsp[(2) - (5)].node), assign_list, (yyvsp[(5) - (5)].node));
    }
    break;

  case 96:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 97:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 98:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 99:

    {
      (yyvsp[(6) - (7)].node) = merge_tree(result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (7)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_CREATE_TABLE, 3, (yyvsp[(3) - (7)].node), (yyvsp[(4) - (7)].node), (yyvsp[(6) - (7)].node));
    }
    break;

  case 100:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 101:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 102:

    {
      (yyvsp[(4) - (5)].node) = merge_tree(result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_PRIMARY_KEY, 1, (yyvsp[(4) - (5)].node));
    }
    break;

  case 103:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_COLUMN_DEF, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 104:

    { (yyval.node) = NULL; }
    break;

  case 105:

    { 
      /* make bison mute */
      if ((yyvsp[(2) - (2)].node));
      (yyval.node) = new_node(result->malloc_pool_, T_NOT_NULL, 0); 
    }
    break;

  case 106:

    { (yyval.node) = new_node(result->malloc_pool_, T_PRIMARY_KEY, 0); }
    break;

  case 107:

    { (yyval.node) = new_node(result->malloc_pool_, T_TYPE_INTEGER , 0); }
    break;

  case 108:

    { (yyval.node) = new_node(result->malloc_pool_, T_TYPE_FLOAT, 0); }
    break;

  case 109:

    { (yyval.node) = new_node(result->malloc_pool_, T_TYPE_DOUBLE, 0); }
    break;

  case 110:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(3) - (4)].node)); }
    break;

  case 111:

    { (yyval.node) = new_node(result->malloc_pool_, T_TYPE_DATETIME, 0); }
    break;

  case 112:

    { (yyval.node) = NULL;}
    break;

  case 113:

    { (yyval.node) = new_node(result->malloc_pool_, T_IF_EXISTS, 0); }
    break;

  case 114:

    {
    	ParseNode* val_list = merge_tree(result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (6)].node));
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              (yyvsp[(3) - (6)].node),           /* target relation */
                              (yyvsp[(4) - (6)].node),           /* column list */
                              val_list,      /* value list */
                              NULL          /* value from sub-query */
                              );
    }
    break;

  case 115:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,           /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node)            /* value from sub-query */
                              );
    }
    break;

  case 116:

    {
      ParseNode* col_list = merge_tree(result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node)            /* value from sub-query */
                              );
    }
    break;

  case 117:

    { 
      (yyval.node) = merge_tree(result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    }
    break;

  case 118:

    { (yyval.node) = NULL; }
    break;

  case 119:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 120:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 121:

    { 
      (yyval.node) = merge_tree(result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    }
    break;

  case 122:

    {
    (yyvsp[(4) - (5)].node) = merge_tree(result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  }
    break;

  case 123:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 124:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 125:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 126:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 127:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 128:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 129:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 130:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (2)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (2)].node);
      (yyval.node) = select;
    }
    break;

  case 131:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if ((yyvsp[(2) - (3)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (3)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    }
    break;

  case 132:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 133:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 134:

    {
      ParseNode* project_list = merge_tree(result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(3) - (8)].node));
      ParseNode* from_list = merge_tree(result->malloc_pool_, T_FROM_LIST, (yyvsp[(5) - (8)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              (yyvsp[(2) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(6) - (8)].node),             /* 4. where */
                              (yyvsp[(7) - (8)].node),             /* 5. group by */
                              (yyvsp[(8) - (8)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
    break;

  case 135:

    {
      ParseNode* set_op = new_node(result->malloc_pool_, T_SET_UNION, 0);
	    (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
    break;

  case 136:

    {
      ParseNode* set_op = new_node(result->malloc_pool_, T_SET_INTERSECT, 0);
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );

    }
    break;

  case 137:

    {
    	ParseNode* set_op = new_node(result->malloc_pool_, T_SET_EXCEPT, 0);
	    (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
    break;

  case 138:

    {(yyval.node) = NULL;}
    break;

  case 139:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 140:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    }
    break;

  case 141:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node)); 
    }
    break;

  case 142:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node)); 
    }
    break;

  case 143:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL); 
    }
    break;

  case 144:

    { 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 145:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node)); 
    }
    break;

  case 146:

    { (yyval.node) = NULL; }
    break;

  case 147:

    {
      (yyval.node) = merge_tree(result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 148:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 149:

    { (yyval.node) = NULL; }
    break;

  case 150:

    { 
      (yyval.node) = merge_tree(result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 151:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 152:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 153:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 154:

    { (yyval.node) = new_node(result->malloc_pool_, T_SORT_ASC, 0); }
    break;

  case 155:

    { (yyval.node) = new_node(result->malloc_pool_, T_SORT_ASC, 0); }
    break;

  case 156:

    { (yyval.node) = new_node(result->malloc_pool_, T_SORT_DESC, 0); }
    break;

  case 157:

    { (yyval.node) = 0; }
    break;

  case 158:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 159:

    {
      (yyval.node) = new_node(result->malloc_pool_, T_ALL, 0);
    }
    break;

  case 160:

    {
      (yyval.node) = new_node(result->malloc_pool_, T_ALL, 0); 
    }
    break;

  case 161:

    { 
      (yyval.node) = new_node(result->malloc_pool_, T_DISTINCT, 0);
    }
    break;

  case 162:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node)); 
      (yyval.node)->str_value_ = copy_expr_string(result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 163:

    { 
      ParseNode* alias_node = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); 
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, alias_node); 
      (yyval.node)->str_value_ = copy_expr_string(result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
    }
    break;

  case 164:

    { 
      ParseNode* alias_node = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, alias_node); 
      (yyval.node)->str_value_ = copy_expr_string(result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
    }
    break;

  case 165:

    {
      ParseNode* star_node = new_node(result->malloc_pool_, T_STAR, 0);
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, star_node); 
      (yyval.node)->str_value_ = copy_expr_string(result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 166:

    { 
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 167:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 168:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 169:

    { (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 170:

    { 
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 171:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 172:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 173:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      yyerror(&(yylsp[(1) - (1)]), result, "sub-select must has alias name in from clause!");
      YYABORT;
    }
    break;

  case 174:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); 
    }
    break;

  case 175:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); 
    }
    break;

  case 176:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 177:

    {
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node)); 
    	yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    }
    break;

  case 178:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 179:

    {
    	(yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 180:

    {
      (yyval.node) = new_non_terminal_node(result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 181:

    {
      ParseNode* node = new_node(result->malloc_pool_, T_JOIN_INNER, 0);
    	(yyval.node) = new_non_terminal_node(result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 182:

    { 
      /* make bison mute */
      if ((yyvsp[(2) - (2)].node));
      (yyval.node) = new_node(result->malloc_pool_, T_JOIN_FULL, 0); 
    }
    break;

  case 183:

    { 
      /* make bison mute */
      if ((yyvsp[(2) - (2)].node));
      (yyval.node) = new_node(result->malloc_pool_, T_JOIN_LEFT, 0); 
    }
    break;

  case 184:

    { 
      /* make bison mute */
      if ((yyvsp[(2) - (2)].node));
      (yyval.node) = new_node(result->malloc_pool_, T_JOIN_RIGHT, 0); 
    }
    break;

  case 185:

    { 
      (yyval.node) = new_node(result->malloc_pool_, T_JOIN_INNER, 0); 
    }
    break;

  case 186:

    { (yyval.node) = NULL; }
    break;

  case 187:

    { (yyval.node) = NULL; }
    break;



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
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
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
        yyerror (&yylloc, result, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;

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
		      yytoken, &yylval, &yylloc, result);
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

  yyerror_range[1] = yylsp[1-yylen];
  /* Do not reclaim the symbols of the rule which action triggered
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
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

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

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
  *++yylsp = yyloc;

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

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
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
                  yytoken, &yylval, &yylloc, result);
    }
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
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
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}





void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s, ...)
{
  p->result_tree_ = 0;
  va_list ap;
  va_start(ap, s);
  vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
  p->start_col_ = yylloc->first_column;
  p->end_col_ = yylloc->last_column;
  p->line_ = yylloc->first_line;
}

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (!p || !p->malloc_pool_)
  {
    ret = -1;
    if (p)
    {
      snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }
  if (ret == 0)
  {
    p->yycolumn_ = 1;
    p->yylineno_ = 1;
    ret = yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_terminate(ParseResult* p)
{
  return yylex_destroy(p->yyscan_info_);
}

void parse_sql(ParseResult* p, const char* buf, size_t len)
{
  p->result_tree_ = 0;
  p->error_msg_[0] = 0;
  p->input_sql_ = buf;
  
  p->yycolumn_ = 1;
  p->yylineno_ = 1;

  if (buf == NULL || len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    return;
  }

  while(len > 0 && isspace(buf[len - 1]))
    --len;

  if (len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be while space only");
    return;
  }

  YY_BUFFER_STATE bp;

  //bp = yy_scan_string(buf, p->yyscan_info_);
  bp = yy_scan_bytes(buf, len, p->yyscan_info_);
  yy_switch_to_buffer(bp, p->yyscan_info_);
  yyparse(p);
  yy_delete_buffer(bp, p->yyscan_info_);
}


