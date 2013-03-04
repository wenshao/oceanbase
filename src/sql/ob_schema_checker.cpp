/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_checker.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */

#include "ob_schema_checker.h"
#include "common/ob_tsi_factory.h"
#include <config.h>
#include <tblog.h>

using namespace oceanbase::sql;
using namespace oceanbase::common;

const char* SCHEMA_FILE = "./MyTestSchema.ini";

ObSchemaChecker::ObSchemaChecker()
{
  ObSchemaManagerV2 *schema_mgr = GET_TSI_MULT(ObSchemaManagerV2, 1);
  if (schema_mgr)
  {
    tbsys::CConfig cfg;
    schema_mgr->parse_from_file(SCHEMA_FILE, cfg);
    schema_mgr_ = schema_mgr;
  }
}

ObSchemaChecker::ObSchemaChecker(const common::ObSchemaManagerV2& schema_mgr)
{
  schema_mgr_ = &schema_mgr;
}

ObSchemaChecker::~ObSchemaChecker()
{
  schema_mgr_ = NULL;
}

bool ObSchemaChecker::column_exists(const ObString& table_name, const ObString& column_name) const
{
  bool ret = false;
  if (schema_mgr_ && schema_mgr_->get_column_schema(table_name, column_name) != NULL)
    ret = true;
  return ret;
}

const ObColumnSchemaV2* ObSchemaChecker::get_column_schema(
    const ObString& table_name, 
    const ObString& column_name) const
{
  const ObColumnSchemaV2 *col = NULL;
  if (schema_mgr_)
    col = schema_mgr_->get_column_schema(table_name, column_name);
  return col;
}

uint64_t ObSchemaChecker::get_column_id(
    const ObString& table_name, 
    const ObString& column_name) const
{
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *col = get_column_schema(table_name, column_name);
  if (col != NULL)
    column_id = col->get_id();
  return column_id;
}

const ObTableSchema* ObSchemaChecker::get_table_schema(const char* table_name) const
{
  const ObTableSchema *table_schema = NULL;
  if (schema_mgr_)
    table_schema = schema_mgr_->get_table_schema(table_name);
  return table_schema;
}

uint64_t ObSchemaChecker::get_table_id(const ObString& table_name) const
{
  uint64_t table_id = OB_INVALID_ID;
  const ObTableSchema *table_schema = schema_mgr_->get_table_schema(table_name);
  if (table_schema != NULL)
    table_id = table_schema->get_table_id();
  return table_id;
}

const ObColumnSchemaV2* ObSchemaChecker::get_table_columns(
    const uint64_t table_id,
    int32_t& size) const
{
  return schema_mgr_->get_table_schema(table_id, size);
}


