#ifndef OCEANBASE_SQL_OB_ITEM_TYPE_H_
#define OCEANBASE_SQL_OB_ITEM_TYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum ObItemType
{
  T_INVALID = 0,  // Min tag

  /* Literal data type tags */
  T_INT,
  T_STRING,
  T_BINARY,
  T_DATE,     // WE may need time and timestamp here
  T_FLOAT,
  T_DOUBLE,
  T_DECIMAL,
  T_BOOL,
  T_NULL,
  T_UNKNOWN,

  /* Reference type tags*/
  T_REF_COLUMN,
  T_REF_EXPR,
  T_REF_QUERY,

  T_HINT,     // Hint message from rowkey
  T_IDENT,
  T_STAR,

  /* Data type tags */
  T_TYPE_INTEGER,
  T_TYPE_FLOAT,
  T_TYPE_DOUBLE,
  T_TYPE_VARCHAR,
  T_TYPE_DATETIME,

  // @note !! the order of the following tags between T_MIN_OP and T_MAX_OP SHOULD NOT be changed
  /* Operator tags */
  T_MIN_OP = 100,
  /* 1. arithmetic operators */
  T_OP_NEG,   // negative
  T_OP_POS,   // positive
  T_OP_ADD,
  T_OP_MINUS,
  T_OP_MUL,
  T_OP_DIV,
  T_OP_POW,
  T_OP_REM,   // remainder
  T_OP_MOD,
  T_OP_EQ,      /* 2. Bool operators */
  T_OP_LE,
  T_OP_LT,
  T_OP_GE,
  T_OP_GT,
  T_OP_NE,
  T_OP_IS,
  T_OP_IS_NOT,
  T_OP_BTW,
  T_OP_NOT_BTW,
  T_OP_LIKE,
  T_OP_NOT_LIKE,
  T_OP_NOT,
  T_OP_AND,
  T_OP_OR,
  T_OP_IN,
  T_OP_NOT_IN,
  T_OP_ARG_CASE,
  T_OP_CASE,
  T_OP_ROW,
  T_OP_EXISTS,

  T_OP_CNN,  /* 3. String operators */

  T_FUN_SYS,                    // system functions, CHAR_LENGTH, ROUND, etc.

  T_MAX_OP,

  /* 4. name filed specificator */
  T_OP_NAME_FIELD,

  /* Function tags */
  T_FUN_MAX,
  T_FUN_MIN,
  T_FUN_SUM,
  T_FUN_COUNT,
  T_FUN_AVG,

  /* parse tree node tags */
  T_DELETE,
  T_SELECT,
  T_UPDATE,
  T_INSERT,
  T_LINK_NODE,
  T_ASSIGN_LIST,
  T_ASSIGN_ITEM,
  T_STMT_LIST,
  T_EXPR_LIST,
  T_WHEN_LIST,
  T_PROJECT_LIST,
  T_PROJECT_ITEM,
  T_FROM_LIST,
  T_SET_UNION,
  T_SET_INTERSECT,
  T_SET_EXCEPT,
  T_WHERE_CLAUSE,
  T_LIMIT_CLAUSE,
  T_SORT_LIST,
  T_SORT_KEY,
  T_SORT_ASC,
  T_SORT_DESC,
  T_ALL,
  T_DISTINCT,
  T_ALIAS,
  T_PROJECT_STRING,
  T_COLUMN_LIST,
  T_VALUE_LIST,
  T_VALUE_VECTOR,
  T_JOINED_TABLE,
  T_JOIN_INNER,
  T_JOIN_FULL,
  T_JOIN_LEFT,
  T_JOIN_RIGHT,
  T_CASE,
  T_WHEN,

  T_CREATE_TABLE,
  T_TABLE_ELEMENT_LIST,
  T_PRIMARY_KEY,
  T_COLUMN_DEF,
  T_NOT_NULL,
  T_IF_EXISTS,

  T_MAX,

} ObItemType;

#ifdef __cplusplus
}
#endif

#endif //OCEANBASE_SQL_OB_ITEM_TYPE_H_
