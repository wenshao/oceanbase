
#include "ob_nb_accessor.h"

using namespace oceanbase;
using namespace common;

ScanConds::ScanConds(const char* column_name, const ObLogicOperator& cond_op, int64_t value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_int(value);

  EasyArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
}

ScanConds::ScanConds()
{
}

ScanConds::ScanConds(const char* column_name, const ObLogicOperator& cond_op, ObString& value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_varchar(value);

  EasyArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
}

ScanConds& ScanConds::operator()(const char* column_name, const ObLogicOperator& cond_op, int64_t value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_int(value);

  EasyArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
  return *this;
}

ScanConds& ScanConds::operator()(const char* column_name, const ObLogicOperator& cond_op, ObString& value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_varchar(value);

  EasyArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
  return *this;
}

ObNbAccessor::ObNbAccessor()
  :client_proxy_(NULL)
{
}

ObNbAccessor::~ObNbAccessor()
{
}

int ObNbAccessor::get(QueryRes*& res, const char* table_name, const ObString& rowkey, const SC& select_columns)
{
  int ret = OB_SUCCESS;
  ObGetParam* param = NULL;
  ObCellInfo cell_info;
  if(NULL == table_name && NULL != res)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name[%p], res[%p]", table_name, res);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != select_columns.get_exec_status())
  {
    ret = select_columns.get_exec_status();
    TBSYS_LOG(WARN, "select columns error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner status fail");
  }

  if(OB_SUCCESS == ret)
  {
    param = GET_TSI_MULT(ObGetParam, TSI_COMMON_GET_PARAM_1);
    if(NULL == param)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific get param fail");
    }
    else
    {
      param->reset();
    }
  }

  if(OB_SUCCESS == ret)
  {
    cell_info.table_name_.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));
    cell_info.row_key_ = rowkey;

    const char* column = NULL;
    for(int32_t i=0;i<select_columns.count() && OB_SUCCESS == ret;i++)
    {
      ret = select_columns.at(i, column);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get select column element fail:ret[%d]", ret);
      }
      
      if(OB_SUCCESS == ret)
      {
        cell_info.column_name_.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(strlen(column)));
        ret = param->add_cell(cell_info);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get param add cell fail:ret[%d]", ret);
        }
      }
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    void* tmp = ob_malloc(sizeof(QueryRes), ObModIds::OB_NB_ACCESSOR);
    if(NULL == tmp)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate QueryRes fail");
    }
    else
    {
      res = new(tmp)QueryRes;
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->get(*param, *(res->get_scanner()));
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"get from ms failed,ret = %d",ret);
    }
    else
    {
      TBSYS_LOG(INFO, "got %ld cells", res->get_scanner()->get_size());
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = res->init(select_columns);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create QueryRes fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS != ret)
  {
    if(NULL != res)
    {
      res->~QueryRes();
      ob_free(res, ObModIds::OB_NB_ACCESSOR);
      res = NULL;
    }
  }
  
  return ret;
}

bool ObNbAccessor::check_inner_stat()
{
  return client_proxy_ != NULL;
}

int ObNbAccessor::scan(QueryRes*& res, const char* table_name, const ObRange& range, const SC& select_columns)
{
  ScanConds scan_conds;
  return scan(res, table_name, range, select_columns, scan_conds);
}

int ObNbAccessor::scan(QueryRes*& res, const char* table_name, const ObRange& range, const SC& select_columns, const ScanConds& scan_conds)
{
  int ret = OB_SUCCESS;
  ObScanParam* param = NULL;
  ObVersionRange version_range;
  ObString ob_table_name;

  if(NULL == table_name || NULL != res)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "table_name[%p], res[%p]", table_name, res);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != select_columns.get_exec_status())
  {
    ret = select_columns.get_exec_status();
    TBSYS_LOG(WARN, "select columns error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && OB_SUCCESS != scan_conds.get_exec_status())
  {
    ret = scan_conds.get_exec_status();
    TBSYS_LOG(WARN, "scan conds error:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner status fail");
  }

  if(OB_SUCCESS == ret)
  {
    ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));
  }

  if(OB_SUCCESS == ret)
  {
    param = GET_TSI_MULT(ObScanParam, TSI_COMMON_SCAN_PARAM_1);
    if(NULL == param)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific scan param fail");
    }
    else
    {
      param->reset();
    }
  }

  if(OB_SUCCESS == ret)
  {
    version_range.border_flag_.set_min_value();
    version_range.border_flag_.set_max_value();
    param->set_version_range(version_range);

    param->set(OB_INVALID_ID, ob_table_name, range);
  }

  const char* select_column = NULL;
  ObString ob_select_column;
  for(int32_t i=0; OB_SUCCESS == ret && i<select_columns.count(); i++)
  {
    ret = select_columns.at(i, select_column);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get sc array element fail:i[%d], ret[%d]", i, ret);
    }

    if(OB_SUCCESS == ret)
    {
      ob_select_column.assign_ptr(const_cast<char*>(select_column), static_cast<int32_t>(strlen(select_column)));
      ret = param->add_column(ob_select_column);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add column to scan param fail:ret[%d]", ret);
      }
    }
  }

  ObSimpleCond scan_cond;
  for(int32_t i=0;i<scan_conds.count() && OB_SUCCESS == ret;i++)
  {
    ret = scan_conds.at(i, scan_cond);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get scan cond array element fail:i[%d], ret[%d]", i, ret);
    }

    if(OB_SUCCESS == ret)
    {
      ret = param->add_where_cond(scan_cond.get_column_name(), scan_cond.get_logic_operator(), scan_cond.get_right_operand()); 
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add cond to scan param fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    void* tmp = ob_malloc(sizeof(QueryRes), ObModIds::OB_NB_ACCESSOR);
    if(NULL == tmp)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate QueryRes fail");
    }
    else
    {
      res = new(tmp)QueryRes;
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->scan(*param, *(res->get_scanner()));
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
    }
    else
    {
      TBSYS_LOG(INFO, "got %ld cells", res->get_scanner()->get_size());
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    ret = res->init(select_columns);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create QueryRes fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS != ret)
  {
    if(NULL != res)
    {
      res->~QueryRes();
      ob_free(res, ObModIds::OB_NB_ACCESSOR);
      res = NULL;
    }
  }
  
  return ret;
}

int ObNbAccessor::delete_row(const char* table_name, const ObString& rowkey)
{
  int ret = OB_SUCCESS;

  ObString ob_table_name;
  ob_table_name.assign_ptr(const_cast<char*>(table_name), static_cast<int32_t>(strlen(table_name)));

  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1); 
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->del_row(ob_table_name, rowkey);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add del row info to mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->apply(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }

  return ret;
}


int ObNbAccessor::update(const char* table_name, const ObString& rowkey, const KV& kv)
{
  return insert(table_name, rowkey, kv);
}

int ObNbAccessor::insert(const char* table_name, const ObString& rowkey, const KV& kv)
{
  int ret = OB_SUCCESS;

  ObMutator* mutator = NULL;
  
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1); 
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  ObMutatorHelper mutator_helper;

  if(OB_SUCCESS == ret)
  {
    ret = mutator_helper.init(mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init mutator helper fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator_helper.insert(table_name, rowkey, kv);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "insert fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->apply(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObNbAccessor::init(ObClientProxy* client_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client_proxy is null");
  }
  else
  {
    this->client_proxy_ = client_proxy;
  }
  return ret;
}


int QueryRes::get_row(TableRow** table_row)
{
  *table_row = &cur_row_;
  return OB_SUCCESS;
}

int QueryRes::next_row()
{
  int ret = OB_SUCCESS;
  int64_t column_index = 0;
  ObCellInfo* cell = NULL;
  bool is_row_changed = false;

  ret = scanner_iter_.get_cell(&cell, &is_row_changed);
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "scanner iter get cell fail:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret)
  {
    if(!is_row_changed)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "not the begin cell");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = cur_row_.set_cell(cell, column_index);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "table row set cell fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "cell %.*s", cell->row_key_.length(), cell->row_key_.ptr());
      TBSYS_LOG(DEBUG, "cell %.*s, %lu", cell->column_name_.length(), cell->column_name_.ptr(), cell->column_id_);
      TBSYS_LOG(DEBUG, "cell %d", cell->value_.get_type());
      int64_t ext = 0;
      cell->value_.get_ext(ext);
      TBSYS_LOG(DEBUG, "cell ext %ld", ext);
      column_index ++;
    }
  }

  while(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS == ret)
    {
      scanner_iter_ ++;
      ret = scanner_iter_.get_cell(&cell, &is_row_changed);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "scanner iter get cell fail:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "cell %.*s, %lu", cell->column_name_.length(), cell->column_name_.ptr(), cell->column_id_);
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(!is_row_changed)
      {
        ret = cur_row_.set_cell(cell, column_index);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "table row set cell fail:ret[%d]", ret);
        }
        else
        {
          column_index ++;
        }
      }
      else
      {
        break;
      }
    }
  }
  return ret;
}

int QueryRes::init(const SC& sc)
{
  int ret = OB_SUCCESS;

  ret = sc.get_exec_status();
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "SC exec fail:ret[%d]", ret);
  }
  
  //构建列名到列序号的映射表
  if(OB_SUCCESS == ret)
  {
    ret = cell_map_.create(COLUMN_NAME_MAP_BUCKET_NUM);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create hash map fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<sc.count() && OB_SUCCESS == ret;i++)
    {
      int err = 0;
      const char* col_name = NULL;
      sc.at(i, col_name);
      if(NULL == col_name)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "col_name is null");
      }

      if(OB_SUCCESS == ret)
      {
        err = cell_map_.set(col_name, i);
        if(hash::HASH_INSERT_SUCC != err)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "hash map insert error:err[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "add [%s] to cell map", col_name);
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    scanner_iter_ = scanner_.begin();
  }

  ObCellInfo* buf = NULL;
  if(OB_SUCCESS == ret)
  {
    buf = reinterpret_cast<ObCellInfo *>(ob_malloc(sizeof(ObCellInfo) * sc.count(), ObModIds::OB_NB_ACCESSOR));
    if(NULL == buf)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "allocate memory fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = cur_row_.init(&cell_map_, buf, sc.count());
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init table row fail:ret[%d]", ret);
    }
  }

  return ret;

}

QueryRes::QueryRes()
{
}

QueryRes::~QueryRes()
{
}


TableRow* QueryRes::get_only_one_row() 
{
  TableRow* ret = NULL;
  int err = OB_SUCCESS;

  err = next_row();
  if(OB_SUCCESS == err)
  {
    err = get_row(&ret);
  }
  return ret;
}

TableRow::TableRow()
  :cell_map_(NULL),
  cells_(NULL),
  cell_count_(0)
{
}

TableRow::~TableRow()
{
  if(NULL != cells_)
  {
    ob_free(cells_, ObModIds::OB_NB_ACCESSOR);
    cells_ = NULL;
    cell_count_ = 0;
  }
}

int64_t TableRow::count() const
{
  return cell_count_;
}

int TableRow::init(hash::ObHashMap<const char*, int64_t>* cell_map, ObCellInfo* cells, int64_t cell_count)
{
  int ret = OB_SUCCESS;
  if(NULL == cell_map || NULL == cells || cell_count <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cell_map[%p], cells[%p], cell_count[%ld]", cell_map, cells, cell_count);
  }
  else
  {
    this->cell_map_ = cell_map;
    this->cells_ = cells;
    this->cell_count_ = cell_count;
  }

  return ret;
}

ObCellInfo* TableRow::get_cell_info(const char* column_name) const
{
  ObCellInfo* ret = NULL;
  int err = OB_SUCCESS;

  if(NULL == column_name)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "column_name is null");
  }

  if(OB_SUCCESS == err && !check_inner_stat())
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }

  int64_t index = 0;
  if(OB_SUCCESS == err)
  {
    int hash_rc = 0;
    hash_rc = cell_map_->get(column_name, index); 
    if(-1 == hash_rc)
    {
      ret = NULL;
      err = OB_ERROR;
      TBSYS_LOG(WARN, "cell_map_ get fail:column_name[%s]", column_name);
    }
    else if(hash::HASH_NOT_EXIST == hash_rc)
    {
      ret = NULL;
      err = OB_ERROR;
      TBSYS_LOG(WARN, "hash not exist:column_name[%s]", column_name);
    }
  }

  if(OB_SUCCESS == err)
  {
    ret = get_cell_info(index);
  }

  return ret;
}

ObCellInfo* TableRow::get_cell_info(int64_t index) const
{
  ObCellInfo* ret = NULL;
  int err = OB_SUCCESS;
  
  if(index < 0 || index >= cell_count_)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "index is out of range:index[%ld], cell_count_[%ld]", index, cell_count_);
  }

  if(OB_SUCCESS == err && !check_inner_stat())
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }

  if(OB_SUCCESS == err)
  {
    ret = &(cells_[index]);
  }

  return ret;
}


bool TableRow::check_inner_stat() const
{
  return cells_ != NULL && cell_map_ != NULL && cell_count_ > 0;
}

int TableRow::set_cell(ObCellInfo* cell, int64_t index)
{
  int ret = OB_SUCCESS;
  if(NULL == cell || index < 0 || index >= cell_count_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cell[%p], index[%ld], cell_count_[%ld]", cell, index, cell_count_);
  }

  if(OB_SUCCESS == ret && !check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "cells_[%p], cell_map_[%p], cell_count_[%ld]", cells_, cell_map_, cell_count_);
  }

  
  if(OB_SUCCESS == ret)
  {
    cells_[index] = *cell;
  }

  return ret;
}

void ObNbAccessor::release_query_res(QueryRes* res)
{
  if(NULL != res)
  {
    res->~QueryRes();
    ob_free(res, ObModIds::OB_NB_ACCESSOR);
  }
}

int ObNbAccessor::batch_begin(BatchHandle& handle)
{
  int ret = OB_SUCCESS;
  
  if(OB_SUCCESS == ret)
  {
    handle.mutator_ = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1); 
    if(NULL == handle.mutator_)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_helper_.init(handle.mutator_);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init mutator helper fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    handle.modified = false;
  }
  return ret;
}

int ObNbAccessor::batch_update(BatchHandle& handle, const char* table_name, const ObString& rowkey, const KV& kv)
{
  return batch_insert(handle, table_name, rowkey, kv);
}

int ObNbAccessor::batch_insert(BatchHandle& handle, const char* table_name, const ObString& rowkey, const KV& kv)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    ret = handle.mutator_helper_.insert(table_name, rowkey, kv);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "insert fail:ret[%d]", ret);
    }
    else
    {
      handle.modified = true;
    }
  }
  return ret;
}

int ObNbAccessor::batch_end(BatchHandle& handle)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret && handle.modified)
  {
    ret = client_proxy_->apply(*(handle.mutator_));
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail:ret[%d]", ret);
    }
  }
  return ret;
}


