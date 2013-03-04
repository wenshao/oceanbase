/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_postfix_expression_test.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
//#include "sql/ob_postfix_expression.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "sql/ob_postfix_expression.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

class ObPostfixExpressionTest: public ::testing::Test
{
  public:
    ObPostfixExpressionTest();
    virtual ~ObPostfixExpressionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObPostfixExpressionTest(const ObPostfixExpressionTest &other);
    ObPostfixExpressionTest& operator=(const ObPostfixExpressionTest &other);
  protected:
    // data members
    void test_btw(int64_t int_a, int64_t int_b, int64_t int_c, bool res, bool is_not_btw);
};

ObPostfixExpressionTest::ObPostfixExpressionTest()
{
}

ObPostfixExpressionTest::~ObPostfixExpressionTest()
{
}

void ObPostfixExpressionTest::SetUp()
{
}

void ObPostfixExpressionTest::TearDown()
{
}

// testcase:
// 19 + 2
TEST_F(ObPostfixExpressionTest, basic_test)
{
  ASSERT_EQ(0, 0);
  ObPostfixExpression p;
  ExprItem item_a, item_b, item_add;
  item_a.type_ = T_INT;
  item_b.type_ = T_INT;
  item_add.type_ = T_OP_ADD;
  item_a.value_.int_ = 19;
  item_b.value_.int_ = 2;
  item_add.value_.int_ = 2;

  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  ObRow row;
  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);

  int64_t re;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 21);
}


// testcase:
// c1 + c2
TEST_F(ObPostfixExpressionTest, row_add_test)
{
  ASSERT_EQ(0, 0);
  ObPostfixExpression p;
  ExprItem item_a, item_b, item_add;
  ObObj obj_a, obj_b;

  item_a.type_ = T_REF_COLUMN;
  item_b.type_ = T_REF_COLUMN;
  item_add.type_ = T_OP_ADD;
  item_a.value_.cell_.tid = 1001;
  item_a.value_.cell_.cid = 16;
  item_b.value_.cell_.tid = 1001;
  item_b.value_.cell_.cid = 17;
  item_add.value_.int_ = 2;


  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  ObRow row;
  ObRowDesc row_desc;

  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
  row.set_row_desc(row_desc);
  obj_a.set_int(19);
  obj_b.set_int(2);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);

  int64_t re;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 21);
}

// testcase:
// c1 + 2
TEST_F(ObPostfixExpressionTest, row_int_add_test)
{
  ASSERT_EQ(0, 0);
  ObPostfixExpression p;
  ExprItem item_a, item_b, item_add;
  ObObj obj_a, obj_b;

  item_a.type_ = T_REF_COLUMN;
  item_b.type_ = T_INT;
  item_add.type_ = T_OP_ADD;
  item_a.value_.cell_.tid = 1001;
  item_a.value_.cell_.cid = 16;
  item_b.value_.int_ = 2;
  item_add.value_.int_ = 2;


  p.add_expr_item(item_a);
  p.add_expr_item(item_b);
  p.add_expr_item(item_add);

  ObRow row;
  ObRowDesc row_desc;

  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
  row.set_row_desc(row_desc);
  obj_a.set_int(19);
  obj_b.set_int(2);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj_b));

  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);

  int64_t re;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 21);
}


// testcase:
TEST_F(ObPostfixExpressionTest, single_element_test)
{
  ASSERT_EQ(0, 0);
  ObPostfixExpression p;
  ExprItem item_a;
  item_a.type_ = T_INT;
  item_a.value_.int_ = 21;

  p.add_expr_item(item_a);

  ObRow row;
  const ObObj *result = NULL;
  p.add_expr_item_end();
  p.calc(row, result);

  int64_t re;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 21);
}

// length(c1)
TEST_F(ObPostfixExpressionTest, sys_func_length_test)
{
  ObPostfixExpression p;
  ExprItem item_a, item_length;

  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = 1001;
  item_a.value_.cell_.cid = 16;

  item_length.type_ = T_FUN_SYS;
  item_length.value_.int_ = 1;       // one parameter
  item_length.string_.assign_ptr(const_cast<char*>("LenGth"), static_cast<int32_t>(strlen("LenGth")));

  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_length));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item_end());

  char buff[1204];
  p.to_string(buff, 1024);
  printf("%s\n", buff);

  ObRow row;
  ObRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  row.set_row_desc(row_desc);
  ObString str_a;
  str_a.assign_ptr(const_cast<char*>("hello world"), static_cast<int32_t>(strlen("hello world")));
  ObObj obj_a;
  obj_a.set_varchar(str_a);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj_a));

  const ObObj *result = NULL;
  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
  ASSERT_TRUE(result);
  int64_t re;
  ASSERT_EQ(OB_SUCCESS, result->get_int(re));
  ASSERT_EQ(re, 11);
}

TEST_F(ObPostfixExpressionTest, sys_func_unknown_test)
{
  ObPostfixExpression p;
  ExprItem item_a, item_length;

  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = 1001;
  item_a.value_.cell_.cid = 16;

  item_length.type_ = T_FUN_SYS;
  item_length.value_.int_ = 1;       // one parameter
  item_length.string_.assign_ptr(const_cast<char*>("kaka"), static_cast<int32_t>(strlen("kaka")));

  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
  ASSERT_EQ(OB_ERR_UNKNOWN_SYS_FUNC, p.add_expr_item(item_length));
}

void ObPostfixExpressionTest::test_btw(int64_t int_a, int64_t int_b, int64_t int_c, bool res, bool is_not_btw)
{
  ObPostfixExpression p;
  ExprItem item_a, item_b, item_c;
  ExprItem item_op;

  item_a.type_ = T_REF_COLUMN;
  item_a.value_.cell_.tid = 1001;
  item_a.value_.cell_.cid = 16;

  item_b.type_ = T_REF_COLUMN;
  item_b.value_.cell_.tid = 1001;
  item_b.value_.cell_.cid = 17;

  item_c.type_ = T_INT;
  item_c.value_.int_ = int_c;

  item_op.type_ = is_not_btw ? T_OP_NOT_BTW : T_OP_BTW;
  item_op.value_.int_ = 3;       // three parameter

  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_a));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_b));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_c));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item(item_op));
  ASSERT_EQ(OB_SUCCESS, p.add_expr_item_end());

  char buff[1204];
  p.to_string(buff, 1024);
  printf("%s\n", buff);

  ObRow row;
  ObRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 16));
  ASSERT_EQ(OB_SUCCESS, row_desc.add_column_desc(1001, 17));
  row.set_row_desc(row_desc);
  ObObj obj;
  obj.set_int(int_a);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 16, obj));
  obj.set_int(int_b);
  ASSERT_EQ(OB_SUCCESS, row.set_cell(1001, 17, obj));

  const ObObj *result = NULL;
  ASSERT_EQ(OB_SUCCESS, p.calc(row, result));
  ASSERT_TRUE(result);
  bool re = false;
  ASSERT_EQ(OB_SUCCESS, result->get_bool(re));
  ASSERT_EQ(re, res);
}

TEST_F(ObPostfixExpressionTest, btw_test)
{
  test_btw(1, 1, 100, true, false);
  test_btw(50, 1, 100, true, false);
  test_btw(100, 1, 100, true, false);
  test_btw(0, 1, 100, false, false);
  test_btw(101, 1, 100, false, false);
}

TEST_F(ObPostfixExpressionTest, not_btw_test)
{
  test_btw(1, 1, 100, false, true);
  test_btw(50, 1, 100, false, true);
  test_btw(100, 1, 100, false, true);
  test_btw(0, 1, 100, true, true);
  test_btw(101, 1, 100, true, true);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
