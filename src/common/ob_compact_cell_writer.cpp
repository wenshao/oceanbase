/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_writer.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_compact_cell_writer.h"

using namespace oceanbase;
using namespace common;

ObCompactCellWriter::ObCompactCellWriter()
  :store_type_(SPARSE)
{
}

int ObCompactCellWriter::init(char *buf, int64_t size, enum ObCompactStoreType store_type)
{
  int ret = OB_SUCCESS;
  if(NULL == buf)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "buf is null");
  }
  else if(OB_SUCCESS != (ret = buf_writer_.assign(buf, size)))
  {
    TBSYS_LOG(WARN, "buf write assign fail:ret[%d]", ret);
  }
  else
  {
    store_type_ = store_type;
  }
  return ret;
}

int ObCompactCellWriter::write_decimal(const ObObj &decimal)
{
  int ret = OB_SUCCESS;
  ObDecimalMeta dec_meta;
  dec_meta.dec_precision_ = decimal.meta_.dec_precision_;
  dec_meta.dec_scale_ = decimal.meta_.dec_scale_;
  dec_meta.dec_nwords_ = decimal.meta_.dec_nwords_;

  const uint32_t *words = NULL;
  uint8_t nwords = decimal.meta_.dec_nwords_;
  nwords ++;

  if(OB_SUCCESS != (ret = buf_writer_.write<uint8_t>(decimal.meta_.dec_vscale_)))
  {
    TBSYS_LOG(WARN, "write dec vscale fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = buf_writer_.write<ObDecimalMeta>(dec_meta)))
  {
    TBSYS_LOG(WARN, "write dec_meta fail:ret[%d]", ret);
  }
  else
  {
    if(nwords <= 3)
    {
      words = reinterpret_cast<const uint32_t*>(&(decimal.varchar_len_));
    }
    else
    {
      words = decimal.value_.dec_words_; 
    }

    for(uint16_t i=0;OB_SUCCESS == ret && i<=dec_meta.dec_nwords_;i++)
    {
      if(OB_SUCCESS != (ret = buf_writer_.write<uint32_t>(words[i])))
      {
        TBSYS_LOG(WARN, "write words fail:ret[%d]", ret);
      }
    }
  }

  return ret;
}

int ObCompactCellWriter::write_varchar(const ObObj &value, ObObj *clone_value)
{
  int ret = OB_SUCCESS;
  ObString varchar_value;
  ObString varchar_written;

  value.get_varchar(varchar_value);
  if(varchar_value.length() > UINT16_MAX)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "varchar is too long:[%d]", varchar_value.length());
  }
  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write<uint16_t>((uint16_t)(varchar_value.length()));
  }
  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write_varchar(varchar_value, &varchar_written);
  }
  if(OB_SUCCESS == ret && NULL != clone_value)
  {
    clone_value->set_varchar(varchar_written);
  }

  return ret;
}

int ObCompactCellWriter::append(const ObObj &value, ObObj *clone_value /* = NULL */)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = append(OB_INVALID_ID, value, clone_value)))
  {
    TBSYS_LOG(WARN, "append value fail:ret[%d]", ret);
  }
  return ret;
}

int ObCompactCellWriter::append(uint64_t column_id, const ObObj &value, ObObj *clone_value)
{
  int ret = OB_SUCCESS;
  ObCellMeta cell_meta;
  int64_t int_value = 0;
  ObDateTime datetime_value = 0;
  ObPreciseDateTime precise_datetime_value = 0;
  ObCreateTime createtime_value = 0;
  ObModifyTime modifytime_value = 0;
  ObNumber decimal_value;

  if(NULL != clone_value)
  {
    *clone_value = value;
  }

  if(value.get_add())
  {
    cell_meta.attr_ = ObCellMeta::AR_ADD;
  }
  else
  {
    cell_meta.attr_ = ObCellMeta::AR_NORMAL;
  }

  if(OB_SUCCESS == ret)
  {
    switch(value.get_type())
    {
    case ObNullType:
      cell_meta.type_ = ObCellMeta::TP_NULL;
      break;
    case ObIntType:
      value.get_int(int_value);
      switch(get_int_byte(int_value))
      {
      case 1:
        cell_meta.type_ = ObCellMeta::TP_INT8;
        break;
      case 2:
        cell_meta.type_ = ObCellMeta::TP_INT16;
        break;
      case 4:
        cell_meta.type_ = ObCellMeta::TP_INT32;
        break;
      case 8:
        cell_meta.type_ = ObCellMeta::TP_INT64;
        break;
      }
      break;
    case ObDecimalType:
      cell_meta.type_ = ObCellMeta::TP_DECIMAL;
      break;
    case ObFloatType:
      cell_meta.type_ = ObCellMeta::TP_FLOAT;
      break;
    case ObDoubleType:
      cell_meta.type_ = ObCellMeta::TP_DOUBLE;
      break;
    case ObDateTimeType:
      cell_meta.type_ = ObCellMeta::TP_TIME;
      break;
    case ObPreciseDateTimeType:
      cell_meta.type_ = ObCellMeta::TP_PRECISE_TIME;
      break;
    case ObVarcharType:
      cell_meta.type_ = ObCellMeta::TP_VARCHAR;
      break;
    case ObCreateTimeType:
      cell_meta.type_ = ObCellMeta::TP_CREATE_TIME;
      break;
    case ObModifyTimeType:
      cell_meta.type_ = ObCellMeta::TP_MODIFY_TIME;
      break;
    default:
      TBSYS_LOG(WARN, "unsupported type:type[%d]", value.get_type());
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = buf_writer_.write<ObCellMeta>(cell_meta);
  }

  if(OB_SUCCESS == ret)
  {
    switch(cell_meta.type_)
    {
    case ObCellMeta::TP_NULL:
      break;
    case ObCellMeta::TP_INT8:
      value.get_int(int_value);
      ret = buf_writer_.write<int8_t>((int8_t)int_value);
      break;
    case ObCellMeta::TP_INT16:
      value.get_int(int_value);
      ret = buf_writer_.write<int16_t>((int16_t)int_value);
      break;
    case ObCellMeta::TP_INT32:
      value.get_int(int_value);
      ret = buf_writer_.write<int32_t>((int32_t)int_value);
      break;
    case ObCellMeta::TP_INT64:
      value.get_int(int_value);
      ret = buf_writer_.write<int64_t>(int_value);
      break;
    case ObCellMeta::TP_DECIMAL:
      ret = write_decimal(value);
      break;
    case ObCellMeta::TP_TIME:
      value.get_datetime(datetime_value);
      ret = buf_writer_.write<ObDateTime>(datetime_value);
      break;
    case ObCellMeta::TP_PRECISE_TIME:
      value.get_precise_datetime(precise_datetime_value);
      ret = buf_writer_.write<ObPreciseDateTime>(precise_datetime_value);
      break;
    case ObCellMeta::TP_VARCHAR:
      ret = write_varchar(value, clone_value);
      break;
    case ObCellMeta::TP_CREATE_TIME:
      value.get_createtime(createtime_value);
      ret = buf_writer_.write<ObCreateTime>(createtime_value);
      break;
    case ObCellMeta::TP_MODIFY_TIME:
      value.get_modifytime(modifytime_value);
      ret = buf_writer_.write<ObModifyTime>(modifytime_value);
      break;
    default:
      TBSYS_LOG(WARN, "unsupported type:type[%d]", value.get_type());
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(column_id == OB_INVALID_ID)
    {
      if(SPARSE == store_type_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "cannot append column_id OB_INVALID_ID");
      }
    }
    else if(column_id > UINT16_MAX)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "column_id is too long:%ld", column_id);
    }
    else
    {
      ret = buf_writer_.write<uint16_t>((uint16_t)column_id);
    }
  }
  return ret;
}

int ObCompactCellWriter::append_escape(int64_t escape)
{
  int ret = OB_SUCCESS;
  ObCellMeta cell_meta;

  if(escape < 0 || escape > 0x7)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "escape is invalid:[%ld]", escape);
  }
  else
  {
    cell_meta.type_ = ObCellMeta::TP_ESCAPE;
    cell_meta.attr_ = ((uint8_t)(escape) & 0x7);
    ret = buf_writer_.write<ObCellMeta>(cell_meta);
  }
  return ret;
}

