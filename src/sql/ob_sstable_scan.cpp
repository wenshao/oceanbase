/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_sstable_scan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObSstableScan::ObSstableScan()
{
}

ObSstableScan::~ObSstableScan()
{
}

int ObSstableScan::open()
{
  //根据扫描range_打开文件或者初始化缓冲区;
  //根据元数据初始化row_desc_;
  return OB_SUCCESS;
}

int ObSstableScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int64_t ObSstableScan::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(buf);
  UNUSED(buf_len);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int ObSstableScan::close()
{
  //释放资源，例如关闭文件或者释放缓冲区等;
  return OB_SUCCESS;
}

int ObSstableScan::get_next_row(const common::ObString *&rowkey, const ObRow *&row)
{
  //从sstable读取下一行的若干cell构造curr_row_;
  //curr_row_.set_row_desc(row_desc_);
  UNUSED(rowkey);
  row = &curr_row_;
  return OB_SUCCESS;
}

int ObSstableScan::set_scan_param(const sstable::ObSSTableScanParam& param)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(param);
  return ret;
}

