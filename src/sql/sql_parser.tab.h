/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison interface for Yacc-like parsers in C
   
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



