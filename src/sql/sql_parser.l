%option noyywrap nounput noinput nodefault yylineno case-insensitive 
%option noyyalloc noyyrealloc noyyfree
%option reentrant bison-bridge bison-locations
%option header-file="sql_parser.lex.h"
%{
#include "sql_parser.tab.h"
#include <stdarg.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "parse_node.h"
#include "parse_malloc.h"

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

#define YY_USER_ACTION yylloc->first_line = yylloc->last_line = ((ParseResult*)yyextra)->yylineno_;\
  yylloc->first_column = ((ParseResult*)yyextra)->yycolumn_;\
  yylloc->last_column = yylloc->first_column + yyleng - 1;\
  ((ParseResult*)yyextra)->yycolumn_ += yyleng;

extern ParseNode* new_node(void *malloc_pool, ObItemType type, int num);

int64_t parse_string(const char* src, char* dest, int64_t len)
{
  int64_t i;
  int64_t index = 0;
  for(i = 0; i < len; ++i)
  {
    char c = src[i];
    if(c == '\\')
    {
      ++i;
      c = src[i];
      switch(c)
      {
      case 'n':
        c = '\n';
        break;
      case 'r':
        c = '\r';
        break;
      case 't':
        c = '\t';
        break;
      default:
        break;
      }
    }
    dest[index++] = c;
  }  
  assert(index <= len);
  dest[index] = '\0';
  return index;
}

int64_t parse_binary(const char* src, char* dest, int64_t len)
{
  int64_t i;
  int64_t index = 0;
  for(i = 0; i < len; i += 2)
  {
    char buf[3];
    buf[0] = src[i];
    buf[1] = src[i+1];
    buf[2] = 0;
    unsigned char a = strtol(buf,0, 16);
    dest[index++] = (char)a;
  }
  return index;
}

%}

space         [ \t\n\r\f]
non_newline   [^\n\r]
comment       ("--"{non_newline}*)
whitespace    ({space}+|{comment})

%%
ADD       { return ADD; }
AND       { return AND; }
ANY       { return ANY; }
ALL       { return ALL; }
AS        { return AS; }
ASC       { return ASC; }
BETWEEN   { return BETWEEN; }
BY        { return BY; }
CASE      { return CASE; }
CHAR(ACTER)? { return CHAR; }
CREATE    { return CREATE; }
DATE      { return DATE; }
DATETIME  { return DATETIME; }
DELETE    { return DELETE; }
DESC      { return DESC; }
DISTINCT  { return DISTINCT; }
ELSE      { return ELSE; }
END       { return END; }
ERROR     { return ERROR; }
EXCEPT    { return EXCEPT; }
EXISTS    { return EXISTS; }
EXPLAIN   { return EXPLAIN; }
FLOAT4    { return FLOAT; }
FLOAT8 | DOUBLE { return DOUBLE; }
FROM      { return FROM; }
FULL      { return FULL; }
GROUP     { return GROUP; }
HAVING    { return HAVING; }
IF        { return IF; }
IN        { return IN; }
INNER     { return INNER; }
INT       { return INTEGER; }
INT2      { return SMALLINT; }
INT4      { return INTEGER; }
INTEGER   { return INTEGER; }
INTERSECT { return INTERSECT; }
INSERT    { return INSERT; }
INTO      { return INTO; }
IS        { return IS; }
JOIN      { return JOIN; }
KEY       { return KEY; }
LEFT      { return LEFT; }
LIMIT     { return LIMIT; }
LIKE      { return LIKE; }
MOD       { return MOD; }
NOT       { return NOT; }
OFFSET    { return OFFSET; }
ON        { return ON; }
OR        { return OR; }
ORDER     { return ORDER; }
OUTER     { return OUTER; }
PRIMARY   { return PRIMARY; }
RIGHT     { return RIGHT; }
SELECT    { return SELECT; }
SET       { return SET; }
SMALLINT  { return SMALLINT; }
TABLE     { return TABLE; }
THEN      { return THEN; }
UNION     { return UNION; }
UPDATE    { return UPDATE; }
VALUES    { return VALUES; }
VARCHAR(ACTER)? {return VARCHAR; }
WHERE     { return WHERE; }
WHEN      { return WHEN; }

NULL   { 
  yylval->node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0);
  return NULLX;
}

[0-9]+ {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_INT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  node->value_ = atoll(node->str_value_);
  return INTNUM;
}

[0-9]+E[-+]?[0-9]+ |
[0-9]+"."[0-9]*E[-+]?[0-9]+ |
"."[0-9]+E[-+]?[0-9]+ {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_FLOAT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  return APPROXNUM;
}

[0-9]+"."[0-9]* |
"."[0-9]+ {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DECIMAL, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  return DECIMAL;
}

TRUE {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0);
  node->value_ = 1;
  yylval->node = node;
  return TRUE;
}
UNKNOWN {
  /* Unknown is can only appears in grammer 'expr is unknown'
     * and it is equal to NULL semanticly
     * so we set its value to to NULL directly
     */
  yylval->node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_NULL, 0);
  return UNKNOWN;
}
FALSE {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BOOL, 0);
  node->value_ = 0;
  yylval->node = node;
  return FALSE;
}

'(\\.|''|[^'\n])*'|\"(\\.|\"\"|[^"\n])*\" {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_STRING, 0);
  yylval->node = node;

  char* src = yytext+1;
  int len = strlen(src) - 1; //remove last quote charactor
  //char* dest = (char*) malloc(len + 1);
  char* dest = (char*) parse_malloc(len + 1, ((ParseResult*)yyextra)->malloc_pool_);

  node->str_value_ = dest;
  node->value_ = parse_string(src, dest, len);

  return STRING;
}

'(\\.|[^'\n])*$ {yyerror(yylloc, yyextra, "Unterminated string %s", yytext);}

\"(\\.|[^"\n])*$ {yyerror(yylloc, yyextra, "Unterminated string %s", yytext);}

X'([0-9A-F]{2})+'|0X([0-9A-F]{2})+ {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_BINARY, 0);
  char* src = yytext + 2; 
  char* dest = 0;
  
  size_t len = strlen(src);
  if(src[len - 1] == '\'')
  {
    --len;
  }
  assert((len % 2) == 0);

  //dest = (char*) malloc(len/2);
  dest = (char*) parse_malloc(len / 2, ((ParseResult*)yyextra)->malloc_pool_);
  node->str_value_ = dest;
  node->value_ = parse_binary(src, dest, len);

  yylval->node = node;
  return BINARY;
}

(D|d)(A|a)(T|t)(E|e){whitespace}?'[0-9]{4}(-[0-9]{2}){2}' {
  int year, month, day;
  struct  tm time;
  int ret = 0;

  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_); // skip left quote
  size_t len = strlen(dest);
  dest[len - 1] = '\0'; //remove final '

  node->str_value_ = dest;

  ret = sscanf(dest, "%4d-%2d-%2d", &year, &month, &day);
  assert(ret == 3);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = 0;
  time.tm_min = 0;
  time.tm_sec = 0;

  node->value_ = mktime(&time) * 1000000;//change to micro seconds
  yylval->node = node;
  return DATE_VALUE;
}

(T|t)(I|i)(M|m)(E|e){whitespace}?'[0-9]{2}(:[0-9]{2}){2}[.][0-9]{1,6}' {
  int hour, minute, second, micro_sec;
  struct  tm time;
  int ret = 0;

  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);  // skip left quote
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%2d:%2d:%2d.%d", 
    &hour, &minute, &second, &micro_sec);
  assert(ret == 4);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = 0;
  time.tm_mon = 0;
  time.tm_mday = 0;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;

  node->value_ = mktime(&time) * micro_sec;// unit is microseconds
  yylval->node = node;
  return DATE_VALUE;
}

(T|t)(I|i)(M|m)(E|e){whitespace}?'[0-9]{4}(-[0-9]{2}){2}[ ][0-9]{2}(:[0-9]{2}){2}[.]?' {
  int hour, minute, second;
  struct  tm time;
  int ret = 0;

  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);  // skip left quote
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%2d:%2d:%2d", 
    &hour, &minute, &second);
  assert(ret == 3);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = 0;
  time.tm_mon = 0;
  time.tm_mday = 0;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;

  node->value_ = mktime(&time) * 1000000;// unit is microseconds
  yylval->node = node;
  return DATE_VALUE;
}


(T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p){whitespace}?'[0-9]{4}(-[0-9]{2}){2}[ ][0-9]{2}(:[0-9]{2}){2}[.][0-9]{1,6}' {
  int year, month, day, hour, minute, second, micro_sec;
  struct  tm time;
  int ret = 0;

  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest = strchr(yytext, '\'');
  dest =  parse_strdup(dest + 1, ((ParseResult*)yyextra)->malloc_pool_);;  // skip left quote
  size_t len = strlen(dest);
  dest[len - 1] = '\0';//remove final '
  node->str_value_ = dest;

  ret = sscanf(dest, "%4d-%2d-%2d %2d:%2d:%2d.%d", 
    &year, &month, &day,
    &hour, &minute, &second, &micro_sec);
  assert(ret == 7);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;

  node->value_ = mktime(&time) * micro_sec;// unit is microseconds
  yylval->node = node;
  return DATE_VALUE;
}

(T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p){whitespace}?'[0-9]{4}(-[0-9]{2}){2}[ ][0-9]{2}(:[0-9]{2}){2}[.]?' {
  int year, month, day, hour, minute, second;
  struct  tm time;
  int ret = 0;

  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_DATE, 0);
  char* dest =  parse_strdup(yytext + 2, ((ParseResult*)yyextra)->malloc_pool_);
  size_t len = strlen(dest);
  --len;
  dest[len] = '\0';//remove final '

  node->str_value_ = dest;

  ret = sscanf(dest, "%4d-%2d-%2d %2d:%2d:%2d", 
    &year, &month, &day,
    &hour, &minute, &second);
  assert(ret == 6);

  memset(&time, 0, sizeof(struct tm));
  time.tm_year = year - 1900;
  time.tm_mon = month - 1;
  time.tm_mday = day;
  time.tm_hour = hour;
  time.tm_min = minute;
  time.tm_sec = second;

  node->value_ = mktime(&time) * 1000000;// unit is microseconds
  yylval->node = node;
  return DATE_VALUE;
}

\/\*\+[^*]+\*\/    { 
  size_t len = 0;
  char* dest;
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_HINT, 0);
  dest = parse_strdup(yytext + 3, ((ParseResult*)yyextra)->malloc_pool_);
  len = strlen(dest);
  dest[len - 2] = '\0';

  node->str_value_ = dest;
  node->value_ = len - 1;

  yylval->node = node;
  return HINT_VALUE;
}
[-+&~|^/%*(),.;!] { return yytext[0];}

"||" {return CNNOP;}
"=" {return COMP_EQ;}
">=" {return COMP_GE;}
">" {return COMP_GT;}
"<=" {return COMP_LE;}
"<" {return COMP_LT;}
"!="|"<>" {return COMP_NE;}

COUNT/"(" { return FCOUNT;}
SUM/"(" { return FSUM;}
AVG/"(" { return FAVG;}
MIN/"(" { return FMIN;}
MAX/"(" { return FMAX;}

[A-Za-z][A_Za-z0-9_]*/"(" {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_IDENT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  node->value_ = 0;
  return SYSFUNC;
}

[A-Za-z][A_Za-z0-9_]* {
  ParseNode* node = new_node(((ParseResult*)yyextra)->malloc_pool_, T_IDENT, 0);
  yylval->node = node;
  node->str_value_ = parse_strdup(yytext, ((ParseResult*)yyextra)->malloc_pool_);
  node->value_ = 0;
  return NAME;
}
[ \t\r\n]    {}
"--"[ \t].*;

<<EOF>>    {return END_P;}
.    {
  yyerror(yylloc, yyextra, "mystery charactor '%c'", *yytext);
  return ERROR;
}
%%

void * yyalloc (size_t bytes, void* yyscanner)
{
  ParseResult *p = yyget_extra(yyscanner);
  return parse_malloc(bytes, p->malloc_pool_);
}

void * yyrealloc (void * ptr, size_t bytes, void* yyscanner)
{
  ParseResult *p = yyget_extra(yyscanner);
  return parse_realloc(ptr, bytes, p->malloc_pool_);
}

void yyfree (void * ptr, void * yyscanner) 
{
  /* Do nothing -- we leave it to the garbage collector. */
  parse_free(ptr);
}

