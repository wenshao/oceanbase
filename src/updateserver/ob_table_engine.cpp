////===================================================================
 //
 // ob_table_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2012-06-20 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "ob_table_engine.h"
#include "ob_memtable.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    std::pair<int64_t, int64_t> ObCellInfoNode::get_size_and_cnt() const
    {
      std::pair<int64_t, int64_t> ret(0, 0);
      ObCellInfoNodeIterable cci;
      uint64_t column_id = OB_INVALID_ID;
      ObObj value;
      cci.set_cell_info_node(this);
      while (OB_SUCCESS == cci.next_cell())
      {
        if (OB_SUCCESS == cci.get_cell(column_id, value))
        {
          ret.first += 1;
          ret.second += MemTable::get_varchar_length_kb_(value);
        }
      }
      return ret;
    }

    ObCellInfoNodeIterable::ObCellInfoNodeIterable() : cell_info_node_(NULL),
                                                       is_iter_end_(false),
                                                       cci_(),
                                                       ctrl_list_(NULL)
    {
      head_node_.column_id = OB_INVALID_ID;
      head_node_.next = NULL;

      rne_node_.column_id = OB_INVALID_ID;
      rne_node_.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
      rne_node_.next = NULL;
      
      mtime_node_.column_id = OB_INVALID_ID;
      mtime_node_.next = NULL;
    }

    ObCellInfoNodeIterable::~ObCellInfoNodeIterable()
    {
    }

    int ObCellInfoNodeIterable::next_cell()
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_
          || (NULL == cell_info_node_ && NULL == ctrl_list_))
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (NULL == ctrl_list_
            || NULL == (ctrl_list_ = ctrl_list_->next))
        {
          if (NULL == cell_info_node_)
          {
            ret = OB_ITER_END;
          }
          else
          {
            ret = cci_.next_cell();
            bool is_row_finished = false;
            if (OB_SUCCESS == ret)
            {
              uint64_t column_id = OB_INVALID_ID;
              ObObj value;
              ret = cci_.get_cell(column_id, value, &is_row_finished);
            }
            if (OB_SUCCESS == ret
                && is_row_finished)
            {
              ret = OB_ITER_END;
            }
          }
        }
      }
      is_iter_end_ = (OB_ITER_END == ret);
      return ret;
    }

    int ObCellInfoNodeIterable::get_cell(uint64_t &column_id, ObObj &value)
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_
          || (NULL == cell_info_node_ && NULL == ctrl_list_)
          || &head_node_ == ctrl_list_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL != ctrl_list_)
      {
        column_id = ctrl_list_->column_id;
        value = ctrl_list_->value;
      }
      else
      {
        bool is_row_finished = false;
        ret = cci_.get_cell(column_id, value, &is_row_finished);
      }
      return ret;
    }

    void ObCellInfoNodeIterable::set_mtime(const uint64_t column_id, const ObModifyTime value)
    {
      mtime_node_.column_id = column_id;
      mtime_node_.value.set_modifytime(value);
      mtime_node_.next = ctrl_list_;
      ctrl_list_ = &mtime_node_;
    }

    void ObCellInfoNodeIterable::set_rne()
    {
      rne_node_.next = ctrl_list_;
      ctrl_list_ = &rne_node_;
    }

    void ObCellInfoNodeIterable::set_head()
    {
      head_node_.next = ctrl_list_;
      ctrl_list_ = &head_node_;
    }

    void ObCellInfoNodeIterable::reset()
    {
      cell_info_node_ = NULL;
      is_iter_end_ = false;
      //cci_.init(NULL);
      ctrl_list_ = NULL;
    }

    void ObCellInfoNodeIterable::set_cell_info_node(const ObCellInfoNode *cell_info_node)
    {
      cell_info_node_ = cell_info_node;
      is_iter_end_ = false;
      //cci_.init(NULL == cell_info_node_ ? NULL : cell_info_node_->buf);
      if (NULL != cell_info_node_)
      {
        cci_.init(cell_info_node_->buf);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObCellInfoNodeIterableWithCTime::ObCellInfoNodeIterableWithCTime() : ctime_column_id_(OB_INVALID_ID),
                                                                         column_id_(OB_INVALID_ID),
                                                                         value_(),
                                                                         is_iter_end_(false),
                                                                         need_nop_(false)
    {
    }

    ObCellInfoNodeIterableWithCTime::~ObCellInfoNodeIterableWithCTime()
    {
    }

    int ObCellInfoNodeIterableWithCTime::next_cell()
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (ObModifyTimeType == value_.get_type())
      {
        column_id_ = ctime_column_id_;
        int64_t vtime = 0;
        value_.get_modifytime(vtime);
        value_.set_createtime(vtime);
      }
      else if (ObCreateTimeType == value_.get_type()
              && need_nop_)
      {
        column_id_ = OB_INVALID_ID;
        value_.set_ext(ObActionFlag::OP_NOP);
      }
      else
      {
        ret = ObCellInfoNodeIterable::next_cell();
        if (OB_SUCCESS != ret
            || OB_SUCCESS != (ret = ObCellInfoNodeIterable::get_cell(column_id_, value_)))
        {
          is_iter_end_ = true;
        }
      }
      return ret;
    }

    int ObCellInfoNodeIterableWithCTime::get_cell(uint64_t &column_id, common::ObObj &value)
    {
      int ret = OB_SUCCESS;
      if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        column_id = column_id_;
        value = value_;
      }
      return ret;
    }

    void ObCellInfoNodeIterableWithCTime::reset()
    {
      ObCellInfoNodeIterable::reset();
      ctime_column_id_ = OB_INVALID_ID;
      column_id_ = OB_INVALID_ID;
      value_.set_null();
      is_iter_end_ = false;
      need_nop_ = false;
    }

    void ObCellInfoNodeIterableWithCTime::set_ctime_column_id(const uint64_t column_id)
    {
      ctime_column_id_ = column_id;
    }

    void ObCellInfoNodeIterableWithCTime::set_need_nop()
    {
      need_nop_ = true;
    }
  }
}

