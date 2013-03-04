#include "parse_node.h"
const char* get_type_name(int type)
{
	switch(type){
	case T_INT : return "T_INT";
	case T_STRING : return "T_STRING";
	case T_BINARY : return "T_BINARY";
	case T_DATE : return "T_DATE";     // WE may need time and timestamp here
	case T_FLOAT : return "T_FLOAT";
	case T_DOUBLE : return "T_DOUBLE";
	case T_DECIMAL : return "T_DECIMAL";
	case T_BOOL : return "T_BOOL";
	case T_NULL : return "T_NULL";
	case T_UNKNOWN : return "T_UNKNOWN";
	case T_REF_COLUMN : return "T_REF_COLUMN";
	case T_REF_EXPR : return "T_REF_EXPR";
	case T_REF_QUERY : return "T_REF_QUERY";
	case T_HINT : return "T_HINT";     // Hint message from rowkey
	case T_IDENT : return "T_IDENT";
	case T_STAR : return "T_STAR";
	case T_TYPE_INTEGER : return "T_TYPE_INTEGER";
	case T_TYPE_FLOAT : return "T_TYPE_FLOAT";
	case T_TYPE_DOUBLE : return "T_TYPE_DOUBLE";
	case T_TYPE_VARCHAR : return "T_TYPE_VARCHAR";
	case T_TYPE_DATETIME : return "T_TYPE_DATETIME";
	case T_OP_NEG : return "T_OP_NEG";   // negative
	case T_OP_POS : return "T_OP_POS";   // positive
	case T_OP_ADD : return "T_OP_ADD";
	case T_OP_MINUS : return "T_OP_MINUS";
	case T_OP_MUL : return "T_OP_MUL";
	case T_OP_DIV : return "T_OP_DIV";
	case T_OP_POW : return "T_OP_POW";
	case T_OP_REM : return "T_OP_REM";   // remainder
	case T_OP_MOD : return "T_OP_MOD";
	case T_OP_EQ : return "T_OP_EQ";      /* 2. Bool operators */
	case T_OP_LE : return "T_OP_LE";
	case T_OP_LT : return "T_OP_LT";
	case T_OP_GE : return "T_OP_GE";
	case T_OP_GT : return "T_OP_GT";
	case T_OP_NE : return "T_OP_NE";
	case T_OP_IS : return "T_OP_IS";
	case T_OP_IS_NOT : return "T_OP_IS_NOT";
	case T_OP_BTW : return "T_OP_BTW";
	case T_OP_NOT_BTW : return "T_OP_NOT_BTW";
	case T_OP_LIKE : return "T_OP_LIKE";
	case T_OP_NOT_LIKE : return "T_OP_NOT_LIKE";
	case T_OP_NOT : return "T_OP_NOT";
	case T_OP_AND : return "T_OP_AND";
	case T_OP_OR : return "T_OP_OR";
	case T_OP_IN : return "T_OP_IN";
	case T_OP_NOT_IN : return "T_OP_NOT_IN";
	case T_OP_ARG_CASE : return "T_OP_ARG_CASE";
	case T_OP_CASE : return "T_OP_CASE";
	case T_OP_ROW : return "T_OP_ROW";
	case T_OP_EXISTS : return "T_OP_EXISTS";
	case T_OP_CNN : return "T_OP_CNN";  /* 3. String operators */
	case T_FUN_SYS : return "T_FUN_SYS";                    // system functions, CHAR_LENGTH, ROUND, etc.
	case T_MAX_OP : return "T_MAX_OP";
	case T_OP_NAME_FIELD : return "T_OP_NAME_FIELD";
	case T_FUN_MAX : return "T_FUN_MAX";
	case T_FUN_MIN : return "T_FUN_MIN";
	case T_FUN_SUM : return "T_FUN_SUM";
	case T_FUN_COUNT : return "T_FUN_COUNT";
	case T_FUN_AVG : return "T_FUN_AVG";
	case T_DELETE : return "T_DELETE";
	case T_SELECT : return "T_SELECT";
	case T_UPDATE : return "T_UPDATE";
	case T_INSERT : return "T_INSERT";
	case T_LINK_NODE : return "T_LINK_NODE";
	case T_ASSIGN_LIST : return "T_ASSIGN_LIST";
	case T_ASSIGN_ITEM : return "T_ASSIGN_ITEM";
	case T_STMT_LIST : return "T_STMT_LIST";
	case T_EXPR_LIST : return "T_EXPR_LIST";
	case T_WHEN_LIST : return "T_WHEN_LIST";
	case T_PROJECT_LIST : return "T_PROJECT_LIST";
	case T_PROJECT_ITEM : return "T_PROJECT_ITEM";
	case T_FROM_LIST : return "T_FROM_LIST";
	case T_SET_UNION : return "T_SET_UNION";
	case T_SET_INTERSECT : return "T_SET_INTERSECT";
	case T_SET_EXCEPT : return "T_SET_EXCEPT";
	case T_WHERE_CLAUSE : return "T_WHERE_CLAUSE";
	case T_LIMIT_CLAUSE : return "T_LIMIT_CLAUSE";
	case T_SORT_LIST : return "T_SORT_LIST";
	case T_SORT_KEY : return "T_SORT_KEY";
	case T_SORT_ASC : return "T_SORT_ASC";
	case T_SORT_DESC : return "T_SORT_DESC";
	case T_ALL : return "T_ALL";
	case T_DISTINCT : return "T_DISTINCT";
	case T_ALIAS : return "T_ALIAS";
	case T_PROJECT_STRING : return "T_PROJECT_STRING";
	case T_COLUMN_LIST : return "T_COLUMN_LIST";
	case T_VALUE_LIST : return "T_VALUE_LIST";
	case T_VALUE_VECTOR : return "T_VALUE_VECTOR";
	case T_JOINED_TABLE : return "T_JOINED_TABLE";
	case T_JOIN_INNER : return "T_JOIN_INNER";
	case T_JOIN_FULL : return "T_JOIN_FULL";
	case T_JOIN_LEFT : return "T_JOIN_LEFT";
	case T_JOIN_RIGHT : return "T_JOIN_RIGHT";
	case T_CASE : return "T_CASE";
	case T_WHEN : return "T_WHEN";
	case T_CREATE_TABLE : return "T_CREATE_TABLE";
	case T_TABLE_ELEMENT_LIST : return "T_TABLE_ELEMENT_LIST";
	case T_PRIMARY_KEY : return "T_PRIMARY_KEY";
	case T_COLUMN_DEF : return "T_COLUMN_DEF";
	case T_NOT_NULL : return "T_NOT_NULL";
	case T_IF_EXISTS : return "T_IF_EXISTS";
	case T_MAX : return "T_MAX";
	default:return 0;
	}
}
