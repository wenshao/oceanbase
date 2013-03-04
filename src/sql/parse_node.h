#ifndef OCEANBASE_SQL_PARSENODE_H_
#define OCEANBASE_SQL_PARSENODE_H_

#include "ob_item_type.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#define MAX_ERROR_MSG 1024

typedef struct
{
  int   err_code_;
  char  err_msg_[MAX_ERROR_MSG];
} ErrStat;

typedef struct
{
  void* plan_tree_;
  void* name_pool_; // ObStringBuf
  void* schema_checker_; // ObSchemaChecker
  ErrStat err_stat_;
} ResultPlan;

struct _ParseNode;

typedef struct _ParseNode
{
  ObItemType   type_;

  /* attributes for terminal node, it is real value */
  int64_t      value_;
  const char*  str_value_;

  /* attributes for non-terninal node, which has children */
  int32_t      num_child_;
  struct _ParseNode** children_;
  
  // BuildPlanFunc m_fnBuildPlan;
} ParseNode;

typedef struct _ParseResult
{
  void*   yyscan_info_;
  ParseNode* result_tree_;
  const char*   input_sql_;
  void*   malloc_pool_; // ObStringBuf
  char    error_msg_[MAX_ERROR_MSG];
  int     start_col_;
  int     end_col_;
  int     line_;
  int     yycolumn_;
  int     yylineno_;
} ParseResult;


#ifdef __cplusplus
extern "C" {
#endif

extern int parse_init(ParseResult* p);

extern int parse_terminate(ParseResult* p);

extern void parse_sql(ParseResult *p, const char* pszSql, size_t iLen);

extern void print_tree(ParseNode* pRoot, int level);

extern void destroy_tree(ParseNode* pRoot);

#ifdef __cplusplus
}
#endif

#endif //OCEANBASE_SQL_PARSENODE_H_


