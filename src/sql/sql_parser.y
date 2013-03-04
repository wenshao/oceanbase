%define api.pure
%parse-param {ParseResult* result}
%locations
%no-lines
%verbose
%{
#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"

#define YYDEBUG 1
%}

%union{
  struct _ParseNode* node;
}

%{
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

%}

%destructor {destroy_tree($$);}<node>
%destructor {oceanbase::common::ob_free($$);}<str_value_>

%token <node> NAME
%token <node> STRING
%token <node> BINARY
%token <node> INTNUM
%token <node> DATE_VALUE
%token <node> HINT_VALUE
%token <node> BOOL
%token <node> APPROXNUM
%token <node> DECIMAL
%token <node> NULLX
%token <node> TRUE
%token <node> FALSE
%token <node> UNKNOWN

%left	UNION EXCEPT
%left	INTERSECT
%left	OR
%left	AND
%right NOT
%left COMP_LE COMP_LT COMP_EQ COMP_GT COMP_GE COMP_NE
%left CNNOP
%left LIKE
%nonassoc BETWEEN
%nonassoc IN
%nonassoc IS NULLX TRUE FALSE UNKNOWN
%left '+' '-'
%left '*' '/' '%' MOD
%left '^'
%right UMINUS
%left '(' ')'
%left '.'

%token ADD
%token AND
%token ANY
%token ALL
%token AS
%token ASC
%token BETWEEN
%token BY
%token CASE
%token CHAR
%token CNNOP
%token CREATE
%token DATE
%token DATETIME
%token DELETE
%token DESC
%token DISTINCT
%token DOUBLE
%token ELSE
%token END
%token END_P
%token ERROR
%token EXCEPT
%token EXISTS
%token EXPLAIN
%token FAVG
%token FCOUNT
%token FLOAT
%token FMAX
%token FMIN
%token FROM
%token FSUM
%token FULL
%token <node> SYSFUNC
%token GROUP
%token HAVING
%token IF
%token IN
%token INNER
%token INTEGER
%token INTERSECT
%token INSERT
%token INTO
%token IS
%token JOIN
%token KEY
%token LEFT
%token LIMIT
%token LIKE
%token MOD
%token NOT
%token OFFSET
%token ON
%token OR
%token ORDER
%token OUTER
%token PRIMARY
%token RIGHT
%token SELECT
%token SET
%token SMALLINT
%token TABLE
%token THEN
%token UNION
%token UPDATE
%token VALUES
%token VARCHAR
%token WHERE
%token WHEN

%type <node> sql_stmt stmt_list stmt
%type <node> select_stmt insert_stmt update_stmt delete_stmt
%type <node> create_table_stmt
%type <node> expr_list expr expr_const arith_expr simple_expr
%type <node> column_ref
%type <node> case_expr func_expr in_expr
%type <node> case_arg when_clause_list when_clause case_default
%type <node> update_asgn_list update_asgn_factor
%type <node> create_col_list create_definition column_atts
%type <node> data_type opt_if_not_exists
%type <node> opt_insert_columns column_list insert_vals_list insert_vals
%type <node> select_with_parens select_no_parens select_clause
%type <node> simple_select select_limit select_expr_list
%type <node> opt_where opt_groupby opt_order_by order_by opt_having
%type <node> sort_list sort_key opt_asc_desc
%type <node> opt_distinct projection
%type <node> from_list table_factor relation_factor joined_table
%type <node> join_type join_outer

%start sql_stmt
%%

sql_stmt: 
    stmt_list END_P
    {
      $$ = result->result_tree_ = merge_tree(result->malloc_pool_, T_STMT_LIST, $1);
      YYACCEPT;
    }
  ;

stmt_list: 
    stmt_list ';' stmt 
    {
      if ($3 != NULL)
        $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
      else
        $$ = $1;
    }
  | stmt 
    {
      $$ = ($1 != NULL) ? $1 : NULL;
    }
  ;

stmt: 
    select_stmt { $$ = $1;}
  | insert_stmt { $$ = $1;}
  | create_table_stmt { $$ = $1;}
  | update_stmt { $$ = $1;}
  | delete_stmt { $$ = $1;}
  | /*EMPTY*/   { $$ = NULL; }
  ;


/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

expr_list: 
    expr 
    { 
      $$ = $1;
    }
  | expr_list ',' expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

column_ref:
    NAME
    { $$ = $1; }
  | NAME '.' NAME 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NAME_FIELD, 2, $1, $3);
    }
  |
    NAME '.' '*'
    {
      ParseNode* node = new_node(result->malloc_pool_, T_STAR, 0);
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NAME_FIELD, 2, $1, node);
    }
  ;

expr_const:
    STRING { $$ = $1; }
  | BINARY { $$ = $1; }
  | DATE_VALUE { $$ = $1; }
  | INTNUM { $$ = $1; }
  | APPROXNUM { $$ = $1; }
  | DECIMAL { $$ = $1; }
  | BOOL { $$ = $1; }
  | NULLX { $$ = $1; }
  ;

simple_expr: 
    column_ref 
    { $$ = $1; }
  | expr_const
    { $$ = $1; }
  | '(' expr ')'
    { $$ = $2; }
  | '(' expr_list ',' expr ')'
    { 
      $$ = merge_tree(result->malloc_pool_, T_EXPR_LIST, new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $2, $4));
    }
  | case_expr
    { 
      $$ = $1; 
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
  | func_expr
    { 
      $$ = $1; 
    }
  | select_with_parens	    %prec UMINUS
    {
    	$$ = $1;
    }
  | EXISTS select_with_parens
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_OP_EXISTS, 1, $2);
    }
  ;

/* used by the expression that use range value, e.g. between and */
arith_expr: 
    simple_expr   { $$ = $1; }
  | '+' arith_expr %prec UMINUS
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_POS, 1, $2); 
    }
  | '-' arith_expr %prec UMINUS
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NEG, 1, $2); 
    }
  | arith_expr '+' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_ADD, 2, $1, $3); }
  | arith_expr '-' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MINUS, 2, $1, $3); }
  | arith_expr '*' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MUL, 2, $1, $3); }
  | arith_expr '/' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_DIV, 2, $1, $3); }
  | arith_expr '%' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_REM, 2, $1, $3); }
  | arith_expr '^' arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_POW, 2, $1, $3); }
  | arith_expr MOD arith_expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MOD, 2, $1, $3); }

expr: 
    simple_expr   { $$ = $1; }
  | '+' expr %prec UMINUS
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_POS, 1, $2); 
    }
  | '-' expr %prec UMINUS
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NEG, 1, $2); 
    }
  | expr '+' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_ADD, 2, $1, $3); }
  | expr '-' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MINUS, 2, $1, $3); }
  | expr '*' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MUL, 2, $1, $3); }
  | expr '/' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_DIV, 2, $1, $3); }
  | expr '%' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_REM, 2, $1, $3); }
  | expr '^' expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_POW, 2, $1, $3); }
  | expr MOD expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_MOD, 2, $1, $3); }
  | expr COMP_LE expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_LE, 2, $1, $3); }
  | expr COMP_LT expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_LT, 2, $1, $3); }
  | expr COMP_EQ expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_EQ, 2, $1, $3); }
  | expr COMP_GE expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_GE, 2, $1, $3); }
  | expr COMP_GT expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_GT, 2, $1, $3); }
  | expr COMP_NE expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NE, 2, $1, $3); }
  | expr LIKE expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_LIKE, 2, $1, $3); }
  | expr NOT LIKE expr { $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_LIKE, 2, $1, $4); }
  | expr AND expr %prec AND
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_AND, 2, $1, $3); 
    }
  | expr OR expr %prec OR
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_OR, 2, $1, $3); 
    }
  | NOT expr
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NOT, 1, $2); 
    }
  | expr IS NULLX
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, $1, $3); 
    }
  | expr IS NOT NULLX
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4); 
    }
  | expr IS TRUE
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, $1, $3); 
    }
  | expr IS NOT TRUE
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4); 
    }
  | expr IS FALSE
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, $1, $3); 
    }
  | expr IS NOT FALSE
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4); 
    }
  | expr IS UNKNOWN
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS, 2, $1, $3); 
    }
  | expr IS NOT UNKNOWN
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4); 
    }
  | expr BETWEEN arith_expr AND arith_expr	    %prec BETWEEN
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_BTW, 3, $1, $3, $5);
    }
  | expr NOT BETWEEN arith_expr AND arith_expr	  %prec BETWEEN
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_BTW, 3, $1, $4, $6);
    }
  | expr IN in_expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_IN, 2, $1, $3);
    }
  | expr NOT IN in_expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_NOT_IN, 2, $1, $4);
    }
  | expr CNNOP expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_OP_CNN, 2, $1, $3);
    }
  ;

in_expr:  
    select_with_parens
    {
    	$$ = $1;
    }
  | '(' expr_list ')'            
    { $$ = merge_tree(result->malloc_pool_, T_EXPR_LIST, $2); }
  ;

case_expr:  
    CASE case_arg when_clause_list case_default END
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_CASE, 3, $2, merge_tree(result->malloc_pool_, T_WHEN_LIST, $3), $4);
    }
  ;

case_arg:  
    expr                  { $$ = $1; }
  | /*EMPTY*/             { $$ = NULL; }
  ;

when_clause_list:
  	when_clause	              
    { $$ = $1; }
  | when_clause_list when_clause	    
    { $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
  ;

when_clause:
  	WHEN expr THEN expr
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_WHEN, 2, $2, $4);
    }
  ;

case_default:
  	ELSE expr                { $$ = $2; }
  | /*EMPTY*/                { $$ = new_node(result->malloc_pool_, T_NULL, 0); }
  ;

func_expr:
    FCOUNT '(' '*' ')' 
    { 
      ParseNode* node = new_node(result->malloc_pool_, T_STAR, 0);
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_COUNT, 1, node); 
    }
  | FCOUNT '(' opt_distinct expr ')' 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_COUNT, 2, $3, $4); 
    }
  | FSUM '(' opt_distinct expr ')' 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_SUM, 2, $3, $4); 
    }
  | FAVG '(' opt_distinct expr ')' 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_AVG, 2, $3, $4); 
    }
  | FMIN '(' opt_distinct expr ')' 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_MIN, 2, $3, $4); 
    }
  | FMAX '(' opt_distinct expr ')' 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_MAX, 2, $3, $4); 
    }
  | SYSFUNC '(' ')'
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_SYS, 1, $1); 
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
  | SYSFUNC '(' expr_list ')'
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_FUN_SYS, 2, $1, merge_tree(result->malloc_pool_, T_EXPR_LIST, $3)); 
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
  ;


/*****************************************************************************
 *
 *	delete grammar
 *
 *****************************************************************************/

delete_stmt: 
    DELETE FROM relation_factor opt_where
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_DELETE, 2, $3, $4);
    }
  ;


/*****************************************************************************
 *
 *	update grammar
 *
 *****************************************************************************/

update_stmt: 
    UPDATE relation_factor SET update_asgn_list opt_where
    {
      ParseNode* assign_list = merge_tree(result->malloc_pool_, T_ASSIGN_LIST, $4);
      $$ = new_non_terminal_node(result->malloc_pool_, T_UPDATE, 3, $2, assign_list, $5);
    }
  ;

update_asgn_list: 
    update_asgn_factor
    {
      $$ = $1;
    }
  | update_asgn_list ',' update_asgn_factor
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

update_asgn_factor:
    NAME COMP_EQ expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
    }
  ;


/*****************************************************************************
 *
 *	create grammar
 *
 *****************************************************************************/
/* TBD */

create_table_stmt: 
    CREATE TABLE opt_if_not_exists table_factor '(' create_col_list ')'
    {
      $6 = merge_tree(result->malloc_pool_, T_TABLE_ELEMENT_LIST, $6);
      $$ = new_non_terminal_node(result->malloc_pool_, T_CREATE_TABLE, 3, $3, $4, $6);
    }
  ;

create_col_list: 
    create_definition 
    { $$ = $1; }
  | create_col_list ',' create_definition 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

create_definition: 
    PRIMARY KEY '(' column_list ')' 
    {
      $4 = merge_tree(result->malloc_pool_, T_COLUMN_LIST, $4);
      $$ = new_non_terminal_node(result->malloc_pool_, T_PRIMARY_KEY, 1, $4);
    }
  | NAME data_type column_atts
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_COLUMN_DEF, 3, $1, $2, $3);
    }
  ;

column_atts: 
    /* EMPTY */
    { $$ = NULL; }
  | NOT NULLX 
    { 
      /* make bison mute */
      if ($2);
      $$ = new_node(result->malloc_pool_, T_NOT_NULL, 0); 
    }
  | PRIMARY KEY 
    { $$ = new_node(result->malloc_pool_, T_PRIMARY_KEY, 0); }
  ;

data_type: 
    INTEGER 
    { $$ = new_node(result->malloc_pool_, T_TYPE_INTEGER , 0); } 
  | FLOAT 
    { $$ = new_node(result->malloc_pool_, T_TYPE_FLOAT, 0); } 
  | DOUBLE 
    { $$ = new_node(result->malloc_pool_, T_TYPE_DOUBLE, 0); } 
  | VARCHAR '(' INTNUM ')' 
    { $$ = new_non_terminal_node(result->malloc_pool_, T_TYPE_VARCHAR, 1, $3); } 
  | DATETIME  
    { $$ = new_node(result->malloc_pool_, T_TYPE_DATETIME, 0); } 
  ;
  
opt_if_not_exists: 
    /* EMPTY */
    { $$ = NULL;} 
  | IF EXISTS
    { $$ = new_node(result->malloc_pool_, T_IF_EXISTS, 0); }
  ;


/*****************************************************************************
 *
 *	insert grammar
 *
 *****************************************************************************/
insert_stmt:
  	INSERT INTO relation_factor opt_insert_columns VALUES insert_vals_list
    {
    	ParseNode* val_list = merge_tree(result->malloc_pool_, T_VALUE_LIST, $6);
    	$$ = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              $3,           /* target relation */
                              $4,           /* column list */
                              val_list,      /* value list */
                              NULL          /* value from sub-query */
                              );
    }
  | INSERT INTO relation_factor select_stmt
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              $3,           /* target relation */
                              NULL,           /* column list */
                              NULL,         /* value list */
                              $4            /* value from sub-query */
                              );
    }
  | INSERT INTO relation_factor '(' column_list ')' select_stmt
    {
      ParseNode* col_list = merge_tree(result->malloc_pool_, T_COLUMN_LIST, $5);
      $$ = new_non_terminal_node(result->malloc_pool_, T_INSERT, 4, 
                              $3,           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              $7            /* value from sub-query */
                              );
    }
  ;

opt_insert_columns:
    '(' column_list ')' 
    { 
      $$ = merge_tree(result->malloc_pool_, T_COLUMN_LIST, $2);
    }
  | /* EMPTY */
    { $$ = NULL; }
  ;

column_list:
    NAME { $$ = $1; }
  | column_list ',' NAME 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

insert_vals_list: 
    '(' insert_vals ')' 
    { 
      $$ = merge_tree(result->malloc_pool_, T_VALUE_VECTOR, $2);
    }
  | insert_vals_list ',' '(' insert_vals ')' {
    $4 = merge_tree(result->malloc_pool_, T_VALUE_VECTOR, $4);
    $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $4);
  }

insert_vals: 
    expr { $$ = $1; }
  | insert_vals ',' expr 
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;


/*****************************************************************************
 *
 *	select grammar
 *
 *****************************************************************************/

select_stmt: 
    select_no_parens    %prec UMINUS
    { $$ = $1; }
  | select_with_parens    %prec UMINUS
    { $$ = $1; }
  ;

select_with_parens:
    '(' select_no_parens ')'      { $$ = $2; }
  | '(' select_with_parens ')'    { $$ = $2; }
  ;

select_no_parens:
    simple_select	          { $$ = $1; }
  | select_clause order_by
    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)$1;
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = $2;
      $$ = select;
    }
  | select_clause opt_order_by select_limit
    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)$1;
      if ($2)
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = $2;
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = $3;
      $$ = select;
    }
  ;

select_clause:
    simple_select	              { $$ = $1; }
  | select_with_parens	        { $$ = $1; }
  ;

simple_select: 
    SELECT opt_distinct select_expr_list 
    FROM from_list
    opt_where opt_groupby opt_having
    {
      ParseNode* project_list = merge_tree(result->malloc_pool_, T_PROJECT_LIST, $3);
      ParseNode* from_list = merge_tree(result->malloc_pool_, T_FROM_LIST, $5);
      $$ = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              $2,             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              $6,             /* 4. where */
                              $7,             /* 5. group by */
                              $8,             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
  | select_clause UNION opt_distinct select_clause
    {
      ParseNode* set_op = new_node(result->malloc_pool_, T_SET_UNION, 0);
	    $$ = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
  | select_clause INTERSECT opt_distinct select_clause
    {
      ParseNode* set_op = new_node(result->malloc_pool_, T_SET_INTERSECT, 0);
      $$ = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );

    }
  | select_clause EXCEPT opt_distinct select_clause
    {
    	ParseNode* set_op = new_node(result->malloc_pool_, T_SET_EXCEPT, 0);
	    $$ = new_non_terminal_node(result->malloc_pool_, T_SELECT, 12,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL            /* 12. limit */
                              );
    }
  ;
  
opt_where:
    /* EMPTY */
    {$$ = NULL;}
  | WHERE expr 
    {
      $$ = $2;
    }
  | WHERE HINT_VALUE expr 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_WHERE_CLAUSE, 2, $3, $2);
    }
  ;

select_limit:
  	LIMIT INTNUM OFFSET INTNUM
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, $4); 
    }
  | OFFSET INTNUM LIMIT INTNUM
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, $4, $2); 
    }
  | LIMIT INTNUM
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, NULL); 
    }
  | OFFSET INTNUM
    { 
      $$ = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, $2); 
    }
  | LIMIT INTNUM ',' INTNUM
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_LIMIT_CLAUSE, 2, $4, $2); 
    }
  ;

opt_groupby:
    /* EMPTY */
    { $$ = NULL; }
  | GROUP BY expr_list 
    {
      $$ = merge_tree(result->malloc_pool_, T_EXPR_LIST, $3);
    }
  ;

opt_order_by:
  	order_by	              { $$ = $1;}
  | /*EMPTY*/             { $$ = NULL; }
  ;

order_by:
  	ORDER BY sort_list	        
    { 
      $$ = merge_tree(result->malloc_pool_, T_SORT_LIST, $3);
    }
  ;

sort_list:
  	sort_key	                  
    { $$ = $1; }
  | sort_list ',' sort_key	      
    { $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
  ;

sort_key:  
    expr opt_asc_desc
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_SORT_KEY, 2, $1, $2); 
    }
  ;

opt_asc_desc:
    /* EMPTY */
    { $$ = new_node(result->malloc_pool_, T_SORT_ASC, 0); }
  | ASC 
    { $$ = new_node(result->malloc_pool_, T_SORT_ASC, 0); }
  | DESC 
    { $$ = new_node(result->malloc_pool_, T_SORT_DESC, 0); }
  ;

opt_having:
    /* EMPTY */
    { $$ = 0; }
  | HAVING expr 
    {
      $$ = $2;
    }
  ;

opt_distinct: 
    /* EMPTY */
    {
      $$ = new_node(result->malloc_pool_, T_ALL, 0);
    }
  | ALL 
    {
      $$ = new_node(result->malloc_pool_, T_ALL, 0); 
    }
  | DISTINCT 
    { 
      $$ = new_node(result->malloc_pool_, T_DISTINCT, 0);
    }
  ;
  
projection: 
    expr 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, $1); 
      $$->str_value_ = copy_expr_string(result, @1.first_column, @1.last_column);
    }
  | expr NAME 
    { 
      ParseNode* alias_node = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $2); 
      $$ = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, alias_node); 
      $$->str_value_ = copy_expr_string(result, @1.first_column, @1.last_column);
    }
  | expr AS NAME 
    { 
      ParseNode* alias_node = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $3);
      $$ = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, alias_node); 
      $$->str_value_ = copy_expr_string(result, @1.first_column, @1.last_column);
    }
  | '*' 
    {
      ParseNode* star_node = new_node(result->malloc_pool_, T_STAR, 0);
      $$ = new_non_terminal_node(result->malloc_pool_, T_PROJECT_STRING, 1, star_node); 
      $$->str_value_ = copy_expr_string(result, @1.first_column, @1.last_column);
    }
  ;

select_expr_list: 
    projection 
    { 
      $$ = $1;
    }
  | select_expr_list ',' projection 
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

from_list:
  	table_factor	              
    { $$ = $1; }
  | from_list ',' table_factor	      
    { $$ = new_non_terminal_node(result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
  ;

table_factor: 
    relation_factor 
    { 
      $$ = $1;
    }
  | relation_factor AS NAME
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $3); 
    }
  | relation_factor NAME
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $2); 
    }
  | select_with_parens
    {
      $$ = $1;
      yyerror(&@1, result, "sub-select must has alias name in from clause!");
      YYABORT;
    }
  | select_with_parens AS NAME
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $3); 
    }
  | select_with_parens NAME
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $1, $2); 
    }
  | joined_table
    {
    	$$ = $1;
    }
  | '(' joined_table ')' AS NAME
    {
    	$$ = new_non_terminal_node(result->malloc_pool_, T_ALIAS, 2, $2, $5); 
    	yyerror(&@1, result, "qualied joined table can not be aliased!");
      YYABORT;
    }
  ;

relation_factor:
    NAME
    { $$ = $1; }
  ;

joined_table:
  /* we do not support cross join and natural join
    * using clause is not supported either
    */
    '(' joined_table ')'
    {
    	$$ = $2;
    }
  | table_factor join_type JOIN table_factor ON expr
    {
      $$ = new_non_terminal_node(result->malloc_pool_, T_JOINED_TABLE, 4, $2, $1, $4, $6);
    }
  | table_factor JOIN table_factor ON expr
    {
      ParseNode* node = new_node(result->malloc_pool_, T_JOIN_INNER, 0);
    	$$ = new_non_terminal_node(result->malloc_pool_, T_JOINED_TABLE, 4, node, $1, $3, $5);
    }
  ;

join_type:  
    FULL join_outer           
    { 
      /* make bison mute */
      if ($2);
      $$ = new_node(result->malloc_pool_, T_JOIN_FULL, 0); 
    }
  | LEFT join_outer
    { 
      /* make bison mute */
      if ($2);
      $$ = new_node(result->malloc_pool_, T_JOIN_LEFT, 0); 
    }
  | RIGHT join_outer
    { 
      /* make bison mute */
      if ($2);
      $$ = new_node(result->malloc_pool_, T_JOIN_RIGHT, 0); 
    }
  | INNER
    { 
      $$ = new_node(result->malloc_pool_, T_JOIN_INNER, 0); 
    }
  ;

join_outer: 
    OUTER                    { $$ = NULL; }
  | /* EMPTY */               { $$ = NULL; }
  ;

%%

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

