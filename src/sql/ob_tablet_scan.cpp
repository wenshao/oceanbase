/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_scan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;


ObTabletScan::ObTabletScan()
  :join_batch_count_(0),
  op_root_(NULL),
  op_sstable_scan_(NULL),
  op_ups_scan_(NULL),
  op_ups_multi_get_(NULL),
  op_tablet_fuse_(NULL),
  op_tablet_join_(NULL),
  schema_mgr_(NULL),
  op_factory_(NULL),
  ups_rpc_stub_(NULL),
  has_project_(false),
  has_filter_(false),
  has_limit_(false)
{
}

int ObTabletScan::add_filter(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = filter_.add_filter(expr)))
  {
    TBSYS_LOG(WARN, "fail to add filter:ret[%d]", ret);
  }
  else
  {
    has_filter_ = true;
  }
  return ret;
}

int ObTabletScan::set_limit(const int64_t limit, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ret = limit_.set_limit(limit, offset);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
  }
  else
  {
    has_limit_ = true;
  }
  return ret;
}

int ObTabletScan::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = out_columns_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "add output column fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = project_.add_output_column(expr)))
  {
    TBSYS_LOG(WARN, "fail to add project:ret[%d]", ret);
  }
  else
  {
    has_project_ = true;
  }
  return ret;
}

ObTabletScan::~ObTabletScan()
{
}

int ObTabletScan::set_operator_factory(ObOperatorFactory *op_factory)
{
  int ret = OB_SUCCESS;
  if(NULL == op_factory)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "op_factory is null");
  }
  else
  {
    op_factory_ = op_factory;
  }
  return ret;
}

int ObTabletScan::set_schema_manager(ObSchemaManagerV2 *schema_mgr)
{
  int ret = OB_SUCCESS;
  if(NULL == schema_mgr)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "schema mgr is null");
  }
  else
  {
    schema_mgr_ = schema_mgr;
  }
  return ret;
}

int ObTabletScan::set_ups_rpc_stub(ObUpsRpcStub *ups_rpc_stub)
{
  int ret = OB_SUCCESS;
  if(NULL == ups_rpc_stub)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "ups rpc stub is null");
  }
  else
  {
    ups_rpc_stub_ = ups_rpc_stub;
  }
  return ret;
}

bool ObTabletScan::check_inner_stat()
{
  bool ret = false;
  ret = join_batch_count_ > 0 && NULL != schema_mgr_ && OB_INVALID_ID != scan_range_.table_id_ && NULL != op_factory_ && NULL != ups_rpc_stub_;
  if(!ret)
  {
    TBSYS_LOG(WARN, "join_batch_count_[%ld], schema_mgr_[%p], scan_range_.table_id_[%lu], op_factory_[%p], ups_rpc_stub_[%p]", 
      join_batch_count_, schema_mgr_, scan_range_.table_id_, op_factory_, ups_rpc_stub_);
  }
  return ret;
}

int64_t ObTabletScan::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(buf);
  UNUSED(buf_len);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int ObTabletScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

void ObTabletScan::set_range(const ObRange &scan_range)
{
  scan_range_ = scan_range;
}

int ObTabletScan::create_plan()
{
  int ret = OB_SUCCESS;
  ObSqlExpression expr;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  ObTabletJoin::TableJoinInfo table_join_info;
  ObTabletJoin::JoinInfo join_info_item;
  ObTabletJoin::JoinInfo join_cond_item;
  std::set<uint64_t> column_ids;
  std::set<uint64_t>::iterator iter;
  ObScanParam scan_param;
  bool is_cid = false;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  //找到所有的需要join的普通列
  for(int32_t i=0;OB_SUCCESS == ret && i<out_columns_.count();i++)
  {
    if(OB_SUCCESS != (ret = out_columns_.at(i, expr)))
    {
      TBSYS_LOG(WARN, "get expression from out columns fail:ret[%d] i[%d]", ret, i);
    }
    if(OB_SUCCESS == ret)
    {
      table_id = expr.get_table_id();
      column_id = expr.get_column_id();

      //如果是普通列, 复合列不交给下层的物理操作符
      if( (OB_SUCCESS == (ret = expr.get_decoded_expression().is_column_index_expr(is_cid))) && is_cid)
      {
        column_ids.insert(column_id);
        if(NULL == (column_schema = schema_mgr_->get_column_schema(table_id, column_id)))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "get column schema fail:table_id[%lu] column_id[%lu]", table_id, column_id);
        }
        //构造对应的join_info信息
        if(OB_SUCCESS == ret)
        {
          join_info = column_schema->get_join_info();
          if(NULL != join_info)
          {
            if(OB_INVALID_ID == table_join_info.left_table_id_)
            {
              table_join_info.left_table_id_ = table_id;
            }
            else if(table_join_info.left_table_id_ != table_id)
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "left table id cannot change:t1[%lu], t2[%lu]", table_join_info.left_table_id_, table_id);
            }
            
            if(OB_SUCCESS == ret)
            {
              join_info_item.right_table_id_ = join_info->join_table_;
              join_info_item.left_column_id_ = column_schema->get_id();
              join_info_item.right_column_id_ = join_info->correlated_column_;
              if(OB_SUCCESS != (ret = table_join_info.join_column_.push_back(join_info_item)))
              {
                TBSYS_LOG(WARN, "add join info item fail:ret[%d]", ret);
              }
            }
          }
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    //如果存在需要join的列
    if(table_join_info.join_column_.count() > 0)
    {
      //取得构成join table rowkey的条件
      if(OB_SUCCESS != (ret = get_join_cond(table_join_info)))
      {
        TBSYS_LOG(WARN, "get join condition fail:ret[%d]", ret);
      }

      //添加join需要的列id
      if(OB_SUCCESS == ret)
      {
        for(int32_t i=0;OB_SUCCESS == ret && i<table_join_info.join_condition_.count();i++)
        {
          if(OB_SUCCESS != (ret = table_join_info.join_condition_.at(i, join_cond_item)))
          {
            TBSYS_LOG(WARN, "get join cond fail:ret[%d] i[%d]", ret, i);
          }
          column_ids.insert(join_cond_item.left_column_id_);
        }
      }

      if(OB_SUCCESS == ret)
      {
        op_tablet_join_ = new(std::nothrow) ObTabletJoin();
        if(NULL == op_tablet_join_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "new ObTabletJoin fail");
        }
      }

      if(OB_SUCCESS == ret)
      {
        op_ups_multi_get_ = new(std::nothrow) ObUpsMultiGet();
        if(NULL == op_ups_multi_get_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "new ups multi get fail");
        }
        else if( OB_SUCCESS != (ret = op_ups_multi_get_->set_rpc_stub(ups_rpc_stub_)) )
        {
          TBSYS_LOG(WARN, "ups multi get set rpc stub fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = op_ups_multi_get_->set_network_timeout(1000)))
        {
          TBSYS_LOG(WARN, "set network timeout fail:ret[%d]", ret);
        }
        else
        {
          op_tablet_join_->set_ups_multi_get(op_ups_multi_get_);
        }
      }

      if(OB_SUCCESS == ret)
      {
        op_tablet_join_->set_table_join_info(table_join_info);
        op_tablet_join_->set_batch_count(join_batch_count_);

        for(iter = column_ids.begin();OB_SUCCESS == ret && iter != column_ids.end();iter ++)
        {
          if(OB_SUCCESS != (ret = op_tablet_join_->add_column_id(*iter)))
          {
            TBSYS_LOG(WARN, "tablet join operator add column id fail:ret[%d], column_id[%lu]", ret, *iter);
          }
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    op_ups_scan_ = new(std::nothrow) ObUpsScan();
    if(NULL == op_ups_scan_)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "new ups scan fail");
    }
    else if(OB_SUCCESS != (ret = op_ups_scan_->set_ups_rpc_stub(ups_rpc_stub_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    op_ups_scan_->set_range(scan_range_);

    for(iter = column_ids.begin();OB_SUCCESS == ret && iter != column_ids.end();iter ++)
    {
      if(OB_SUCCESS != (ret = scan_param.add_column(*iter)))
      {
        TBSYS_LOG(WARN, "scan param add column fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = op_ups_scan_->add_column(*iter)))
      {
        TBSYS_LOG(WARN, "op ups scan add column fail:ret[%d]", ret);
      }
    }

    op_sstable_scan_ = op_factory_->new_sstable_scan();
    if(NULL == op_sstable_scan_)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "new sstable scan fail");
    }
 }

 if(OB_SUCCESS == ret)
 {
    scan_param.set_range(scan_range_);
    sstable::ObSSTableScanParam sstable_scan_param;
    sstable_scan_param.assign(scan_param);
    sstable_scan_param.get_range().table_id_ = scan_range_.table_id_;

    if(OB_SUCCESS != (ret = op_sstable_scan_->set_scan_param(sstable_scan_param)))
    {
      TBSYS_LOG(WARN, "set scan param fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(NULL == (op_tablet_fuse_ = new(std::nothrow) ObTabletFuse()))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "new ObTabletFuse fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_tablet_fuse_->set_sstable_scan(op_sstable_scan_)))
    {
      TBSYS_LOG(WARN, "set sstable scan fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_tablet_fuse_->set_incremental_scan(op_ups_scan_)))
    {
      TBSYS_LOG(WARN, "set incremental scan fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(NULL != op_tablet_join_)
    {
      op_tablet_join_->set_child(0, *op_tablet_fuse_);
      op_root_ = op_tablet_join_;
    }
    else
    {
      op_root_ = op_tablet_fuse_;
    }
  }

  return ret;
}

int ObTabletScan::open()
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = create_plan()))
  {
    TBSYS_LOG(WARN, "create plan fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret && has_filter_)
  {
    if (OB_SUCCESS != (ret = filter_.set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set filter child. ret=%d", ret);
    }
    else
    {
      op_root_ = &filter_;
    }
  }
  if (OB_SUCCESS == ret && has_project_)
  {
    if (OB_SUCCESS != (ret = project_.set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
    }
    else
    {
      op_root_ = &project_;
    }
  }
  if (OB_SUCCESS == ret && has_limit_)
  {
    if (OB_SUCCESS != (ret = limit_.set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
    }
    else
    {
      op_root_ = &limit_;
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = op_root_->open();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "open tablets scan child operator fail. ret=%d", ret);
    }
  }
  return ret;
}

int ObTabletScan::close()
{
  op_root_->close();
  //释放内存等;

  if(NULL != op_sstable_scan_)
  {
    delete op_sstable_scan_;
  }
  if(NULL != op_ups_scan_)
  {
    delete op_ups_scan_;
  }
  if(NULL != op_ups_multi_get_)
  {
    delete op_ups_multi_get_;
  }
  if(NULL != op_tablet_fuse_)
  {
    delete op_tablet_fuse_;
  }
  if(NULL != op_tablet_join_)
  {
    delete op_tablet_join_;
  }
  return OB_SUCCESS;
}

int ObTabletScan::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ret = op_root_->get_next_row(row);
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
  }
  return ret;
}

