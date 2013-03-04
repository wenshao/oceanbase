/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: bigquery_writer.cpp,v 0.1 2012/03/31 16:49:36 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "bigquery_writer.h"
#include "oceanbase.h"

static const char* BIGQUERY_TABLE = "bigquery_table";
static const char* COLUMN_PREFIX = "col";

BigqueryWriter::BigqueryWriter()
{
  ob_ = NULL;
  req_ = NULL;
}

BigqueryWriter::~BigqueryWriter()
{
  if (NULL != ob_)
  {
    ob_close(ob_);
    ob_api_destroy(ob_);
    ob_ = NULL;
  }
  req_ = NULL;
}

int BigqueryWriter::init(ObSqlClient& ob_client, PrefixInfo& prefix_info, const char* rs_ip, int32_t rs_port)
{
  int err = 0;

  ob_ = ob_api_init();
  if (NULL == ob_)
  {
    TBSYS_LOG(WARN, "failed to init ob api");
    err = OB_ERROR;
  }
  else
  {
    ob_api_debug_log(ob_, "info", "log/log_obapi.log");
  }

  if (0 == err)
  {
    err = ob_connect(ob_, rs_ip, rs_port, NULL, NULL);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to connect, rs_ip=%s, rs_port=%d, err=%d", rs_ip, rs_port, err);
    }
  }

  prefix_info_ = &prefix_info;

  return err;
}

int BigqueryWriter::write_data(uint64_t prefix)
{
  int err = 0;

  int64_t row_num = 0;
  int64_t col_arr[16];
  char rule_data[1024];
  ValueRule rule;

  err = prefix_info_->set_read_write_status(prefix, BIGQUERY_UPDATING);
  if (READ_WRITE_CONFLICT != err && 0 != err)
  {
    TBSYS_LOG(WARN, "failed to set update flag to true, prefix=%lu, err=%d", prefix, err);
  }

  if (0 == err)
  {
    err = value_gen_.generate_value_rule(prefix, rule);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to generate value rule, prefix=%lu, err=%d",
          prefix, err);
    }
    else
    {
      err = rule.serialize(rule_data, sizeof(rule_data));
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to serialize rule, rule_data=%p, rule_len=%ld, err=%d",
            rule_data, sizeof(rule_data), err);
      }
    }
  }

  if (0 == err)
  {
    row_num = rule.get_row_num();
    err = prefix_info_->set_rule_and_row_num(prefix, rule_data, row_num);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to set rule or row num, prefix=%lu, rule_data=%s, row_num=%ld, err=%d",
          prefix, rule_data, row_num, err);
    }
  }

  if (0 == err)
  {
    req_ = ob_acquire_set_st(ob_);
    if (NULL == req_)
    {
      TBSYS_LOG(WARN, "failed to acquire set req, ob_=%p", ob_);
      err = OB_ERROR;
    }
  }

  int64_t num = 0;
  static const int64_t NUM_PER_MUTATION = 10 * 1024;

  for (uint64_t i = 0;  i < (uint64_t) row_num && 0 == err; ++i)
  {
    int64_t ret_size = 0;
    rule.get_col_values(i, col_arr, sizeof(col_arr) / sizeof(col_arr[0]), ret_size);
    err = write_data(prefix, i, col_arr, ret_size);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to write data, prefix=%lu, suffix=%lu, err=%d",
          prefix, i, err);
    }
    else
    {
      ++num;
      if (num == NUM_PER_MUTATION)
      {
        num = 0;
        err = commit_set();;
        if (0 != err)
        {
          TBSYS_LOG(WARN, "failed to commit set, num=%ld, i=%ld, err=%d", num, i, err);
        }
        else
        {
          ob_release_set_st(ob_, req_);

          req_ = ob_acquire_set_st(ob_);
          assert(NULL != req_);
        }
      }
    }
  }

  if (0 == err && num > 0)
  {
    err = commit_set();
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to commit set, req_=%p, err=%d", req_, err);
    }
  }

  if (NULL != req_)
  {
    ob_release_set_st(ob_, req_);
    req_ = NULL;
  }

  if (0 == err)
  {
    err = prefix_info_->set_read_write_status(prefix, 0);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to reset flag, prefix=%lu, err=%d", prefix, err);
    }
  }
  else if (READ_WRITE_CONFLICT == err)
  {
    err = OB_SUCCESS;
  }

  return err;
}

int BigqueryWriter::write_data(uint64_t prefix, uint64_t suffix, int64_t* col_ary, int64_t arr_len)
{
  int err = 0;
  assert(col_ary != NULL && arr_len > 0);
  char rowkey[32];
  int64_t rowkey_len = 0;
  translate_uint64(rowkey, prefix);
  translate_uint64(rowkey + 8, suffix);
  rowkey_len = 16;
  char column[32];

  for (int64_t i = 0; 0 == err && i < arr_len; ++i)
  {
    sprintf(column, "%s%ld", COLUMN_PREFIX, i+1);
    err = ob_insert_int(req_, BIGQUERY_TABLE, rowkey, rowkey_len, column, col_ary[i]);
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to insert, i=%ld, err=%d", i, err);
    }
  }

  return err;
}

void BigqueryWriter::translate_uint64(char* rowkey, uint64_t value)
{
  assert(rowkey != NULL);
  for (int i = 7; i >=0; --i)
  {
    *(rowkey + i) = value % 256;
    value /= 256;
  }
}

int BigqueryWriter::commit_set()
{
  int err = 0;
  while (true)
  {
    err = ob_exec_set(ob_, req_);
    if (0 == err)
    {
      break;
    }
    else if (OB_ERR_MEM_NOT_ENOUGH == err)
    {
      TBSYS_LOG(WARN, "failed to exec set, MEM_NOT_ENOUGH");
      sleep(1);
      err = 0;
    }
    else if (OB_ERR_RESPONSE_TIMEOUT == err)
    {
      TBSYS_LOG(WARN, "failed to exec set, TIME_OUT");
      sleep(1);
      err = 0;
    }
    else if (OB_ERR_NOT_MASTER == err)
    {
      TBSYS_LOG(ERROR, "failed to exec set, NOT_MASTER");
      sleep(1);
      err = 0;
    }
    else
    {
      TBSYS_LOG(ERROR, "failed to exec set, err=%d", err);
      break;
    }
  }
  return err;
}


