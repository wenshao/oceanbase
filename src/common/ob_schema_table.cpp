
#include "ob_schema_table.h"
#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/thread_buffer.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_schema.h"
#include "common/ob_mutator.h"
#include "common/utility.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase
{
namespace common
{
  void get_table_name(const ObTableSchema& table, ObObj& value)
  {
    ObString table_name;
    table_name.assign_ptr(const_cast<char*>(table.get_table_name()), 
      static_cast<int32_t>(strlen(table.get_table_name())));
    value.set_varchar(table_name);
  }

  void get_table_id(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(static_cast<int64_t>(table.get_table_id()));
  }

  void get_table_type(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(static_cast<int64_t>(table.get_table_type()));
  }

  void get_rowkey_len(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(static_cast<int64_t>(table.get_rowkey_max_length()));
  }

  void get_rowkey_is_fixed_len(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(static_cast<int64_t>(table.is_row_key_fixed_len()));
  }

  void get_rowkey_split(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(static_cast<int64_t>(table.get_split_pos()));
  }

  void get_compressor(const ObTableSchema& table, ObObj& value)
  {
    ObString compressor;
    compressor.assign_ptr(const_cast<char*>(table.get_compress_func_name()),
      static_cast<int32_t>(strlen(table.get_compress_func_name())));
    value.set_varchar(compressor);
  }

  void get_max_column_id(const ObTableSchema& table, ObObj& value)
  {
    value.set_int(table.get_max_column_id());
  }
    
    
  void get_table_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    const ObTableSchema* table = schema_mgr.get_table_schema(column.get_table_id());
    if(NULL != table)
    {
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(table->get_table_name()),
        static_cast<int32_t>(strlen(table->get_table_name())));
      value.set_varchar(table_name);
    }
    else
    {
      TBSYS_LOG(WARN, "cannot get table schema with table id[%lu]", column.get_table_id());
    }
  }

  void get_table_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    value.set_int(static_cast<int64_t>(column.get_table_id()));
  }

  void get_column_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    ObString column_name;
    column_name.assign_ptr(const_cast<char*>(column.get_name()), static_cast<int32_t>(strlen(column.get_name())));
    value.set_varchar(column_name);
  }

  void get_column_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    value.set_int(static_cast<int64_t>(column.get_id()));
  }

  void get_data_type(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    value.set_int(static_cast<int64_t>(column.get_type()));
  }

  void get_data_length(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    value.set_int(static_cast<int64_t>(column.get_size()));
  }

  void get_column_group_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    value.set_int(static_cast<int64_t>(column.get_column_group_id()));
  }
  

  void get_start_pos(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      value.set_int(static_cast<int64_t>(joininfo->start_pos_));
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }

  void get_end_pos(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      value.set_int(static_cast<int64_t>(joininfo->end_pos_));
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }

  void get_right_table_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      const ObTableSchema* table = schema_mgr.get_table_schema(joininfo->join_table_);
      if(NULL != table)
      {
        ObString table_name;
        table_name.assign_ptr(const_cast<char*>(table->get_table_name()), static_cast<int32_t>(strlen(table->get_table_name())));
        value.set_varchar(table_name);
      }
      else
      {
        TBSYS_LOG(WARN, "cannot get table schema for table[%lu]", joininfo->join_table_);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }

  void get_right_table_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      value.set_int(static_cast<int64_t>(joininfo->join_table_));
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }

  void get_right_column_name(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      const ObColumnSchemaV2* col = schema_mgr.get_column_schema(joininfo->join_table_, joininfo->correlated_column_);
      if(NULL != col)
      {
        ObString col_name;
        col_name.assign_ptr(const_cast<char*>(col->get_name()), static_cast<int32_t>(strlen(col->get_name())));
        value.set_varchar(col_name);
      }
      else
      {
        TBSYS_LOG(WARN, "column has no join info");
      }
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }
  
  void get_right_column_id(const ObSchemaManagerV2& schema_mgr, const ObColumnSchemaV2& column, ObObj& value)
  {
    UNUSED(schema_mgr);
    const ObColumnSchemaV2::ObJoinInfo* joininfo = column.get_join_info();
    if(NULL != joininfo)
    {
      value.set_int(static_cast<int64_t>(joininfo->correlated_column_));
    }
    else
    {
      TBSYS_LOG(WARN, "column has no join info");
    }
  }

  const TableMap TABLE_OR_MAP[8] =
  {
    TableMap("table_name", get_table_name),
    TableMap("table_id", get_table_id),
    TableMap("table_type", get_table_type),
    TableMap("rowkey_len", get_rowkey_len),
    TableMap("rowkey_is_fixed_len", get_rowkey_is_fixed_len),
    TableMap("rowkey_split", get_rowkey_split),
    TableMap("compressor", get_compressor),
    TableMap("max_column_id", get_max_column_id)
  };

  const ColumnMap COLUMN_OR_MAP[7] = 
  {
    ColumnMap("table_name", get_table_name),
    ColumnMap("table_id", get_table_id),
    ColumnMap("column_name", get_column_name),
    ColumnMap("column_id", get_column_id),
    ColumnMap("data_type", get_data_type),
    ColumnMap("data_length", get_data_length),
    ColumnMap("column_group_id", get_column_group_id)
  };

  const JoininfoMap JOININFO_OR_MAP[10] = 
  {
    JoininfoMap("left_table_name", get_table_name),
    JoininfoMap("left_table_id", get_table_id),
    JoininfoMap("left_column_name", get_column_name),
    JoininfoMap("left_column_id", get_column_id),
    JoininfoMap("start_pos", get_start_pos),
    JoininfoMap("end_pos", get_end_pos),
    JoininfoMap("right_table_name", get_right_table_name),
    JoininfoMap("right_table_id", get_right_table_id),
    JoininfoMap("right_column_name", get_right_column_name),
    JoininfoMap("right_column_id", get_right_column_id)
  };
}
}


int oceanbase::common::dump_joininfo(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  ObString rowkey;
  ObString column;
  ObString table_name;
  ObObj value;
  table_name.assign_ptr(const_cast<char*>(JOININFO_TABLE_NAME), static_cast<int32_t>(strlen(JOININFO_TABLE_NAME)));
  column.assign_ptr(const_cast<char*>(JOININFO_OR_MAP[0].column_name_), static_cast<int32_t>(strlen(JOININFO_OR_MAP[0].column_name_)));

  ret = clean_table(table_name, column, client_helper, mutator);
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "clean table[%s] fail:ret[%d]", JOININFO_TABLE_NAME, ret);
  }

  char buf[sizeof(uint64_t) * 2];
  for(const ObColumnSchemaV2* it=schema_mgr.column_begin();it != schema_mgr.column_end();it++)
  {
    if(NULL == it->get_join_info())
    {
      continue;
    }

    // rowkey = (left_table_id, left_column_id)
    uint64_t table_id = it->get_table_id();
    uint64_t column_id = it->get_id();
    memcpy(buf, &table_id, sizeof(uint64_t));
    memcpy(buf + sizeof(uint64_t), &column_id, sizeof(uint64_t));
    rowkey.assign_ptr(buf, sizeof(uint64_t));

    for(uint32_t i=0;i<ARRAYSIZEOF(JOININFO_OR_MAP) && (OB_SUCCESS == ret);i++)
    {
      column.assign_ptr(const_cast<char*>(JOININFO_OR_MAP[i].column_name_), static_cast<int32_t>(strlen(JOININFO_OR_MAP[i].column_name_)));
      JOININFO_OR_MAP[i].func_(schema_mgr, *it, value);

      TBSYS_LOG(DEBUG, "table_name[%.*s], column[%.*s]", table_name.length(), table_name.ptr(), column.length(), column.ptr());
      hex_dump(rowkey.ptr(), rowkey.length());

      if(ObIntType == value.get_type())
      {
        int64_t tmp = 0;
        value.get_int(tmp);
        TBSYS_LOG(DEBUG, "value [%ld]", tmp);
      }
      else if(ObVarcharType == value.get_type())
      {
        ObString tmp;
        value.get_varchar(tmp);
        TBSYS_LOG(DEBUG, "value [%.*s]", tmp.length(), tmp.ptr());
      }

      ret = mutator->update(table_name, rowkey, column, value);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add update to ob mutator fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int oceanbase::common::dump_columns(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  ObString rowkey;
  ObString column;
  ObString table_name;
  ObObj value;
  table_name.assign_ptr(const_cast<char*>(COLUMN_TABLE_NAME), static_cast<int32_t>(strlen(COLUMN_TABLE_NAME)));
  column.assign_ptr(const_cast<char*>(COLUMN_OR_MAP[0].column_name_), static_cast<int32_t>(strlen(COLUMN_OR_MAP[0].column_name_)));

  ret = clean_table(table_name, column, client_helper, mutator);
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "clean table[%s] fail:ret[%d]", COLUMN_TABLE_NAME, ret);
  }

  char buf[sizeof(uint64_t) * 2];
  for(const ObColumnSchemaV2* it=schema_mgr.column_begin();it != schema_mgr.column_end() && (OB_SUCCESS == ret); it++)
  {
    // rowkey = (table_id, column_id)
    uint64_t table_id = it->get_table_id();
    uint64_t column_id = it->get_id();
    memcpy(buf, &table_id, sizeof(uint64_t));
    memcpy(buf + sizeof(uint64_t), &column_id, sizeof(uint64_t));
    rowkey.assign_ptr(buf, 2 * sizeof(table_id));

    for(uint32_t i=0;i<ARRAYSIZEOF(COLUMN_OR_MAP) && (OB_SUCCESS == ret);i++)
    {
      column.assign_ptr(const_cast<char*>(COLUMN_OR_MAP[i].column_name_), static_cast<int32_t>(strlen(COLUMN_OR_MAP[i].column_name_)));
      COLUMN_OR_MAP[i].func_(schema_mgr, *it, value);

      TBSYS_LOG(DEBUG, "table_name[%.*s], column[%.*s]", table_name.length(), table_name.ptr(), column.length(), column.ptr());
      hex_dump(rowkey.ptr(), rowkey.length());

      if(ObIntType == value.get_type())
      {
        int64_t tmp = 0;
        value.get_int(tmp);
        TBSYS_LOG(DEBUG, "value [%ld]", tmp);
      }
      else if(ObVarcharType == value.get_type())
      {
        ObString tmp;
        value.get_varchar(tmp);
        TBSYS_LOG(DEBUG, "value [%.*s]", tmp.length(), tmp.ptr());
      }

      ret = mutator->update(table_name, rowkey, column, value);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add update to ob mutator fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}


int oceanbase::common::dump_tables(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, ObMutator* mutator)
{
  int ret = OB_SUCCESS;

  ObString rowkey;
  ObString column;
  ObString table_name;
  ObObj value;
  table_name.assign_ptr(const_cast<char*>(FIRST_TABLET_TABLE_NAME), static_cast<int32_t>(strlen(FIRST_TABLET_TABLE_NAME)));
  column.assign_ptr(const_cast<char*>(TABLE_OR_MAP[0].column_name_), static_cast<int32_t>(strlen(TABLE_OR_MAP[0].column_name_)));

  ret = clean_table(table_name, column, client_helper, mutator); 
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "clean table[%s] fail:ret[%d]", FIRST_TABLET_TABLE_NAME, ret);
  }
 
  for(const ObTableSchema* it=schema_mgr.table_begin();(it != schema_mgr.table_end()) && (OB_SUCCESS == ret);it++)
  {
    rowkey.assign_ptr(const_cast<char*>(it->get_table_name()), static_cast<int32_t>(strlen(it->get_table_name())));
    for(uint32_t i = 0;i<ARRAYSIZEOF(TABLE_OR_MAP) && (OB_SUCCESS == ret);i++)
    {
      column.assign_ptr(const_cast<char*>(TABLE_OR_MAP[i].column_name_), static_cast<int32_t>(strlen(TABLE_OR_MAP[i].column_name_)));
      TABLE_OR_MAP[i].func_(*it, value);

      TBSYS_LOG(INFO, "table_name[%.*s], rowkey[%.*s], column[%.*s]", table_name.length(), table_name.ptr(), rowkey.length(), rowkey.ptr(),
        column.length(), column.ptr());

      if(ObIntType == value.get_type())
      {
        int64_t tmp = 0;
        value.get_int(tmp);
        TBSYS_LOG(DEBUG, "value [%ld]", tmp);
      }
      else if(ObVarcharType == value.get_type())
      {
        ObString tmp;
        value.get_varchar(tmp);
        TBSYS_LOG(DEBUG, "value [%.*s]", tmp.length(), tmp.ptr());
      }

      ret = mutator->update(table_name, rowkey, column, value);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add update to ob mutator fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int oceanbase::common::clean_table(const ObString& table_name, const ObString& select_column, common::ObClientHelper* client_helper, ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  ObScanParam* param = NULL;
  ObVersionRange version_range;
  ObRange range;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "argument mutator is null");
    }
  }

  if(OB_SUCCESS == ret && NULL == client_helper)
  {
    ret = OB_INVALID_ARGUMENT;
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "argument client_helper is null");
    }
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

    range.border_flag_.set_min_value();
    range.border_flag_.set_max_value();
    param->set(OB_INVALID_ID, table_name, range);
  }

  if(OB_SUCCESS == ret)
  {
    ret = param->add_column(select_column);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add column to scan param fail:ret[%d]", ret);
    }
  }

  ObScanner* scanner = NULL;
  if(OB_SUCCESS == ret)
  {
    scanner = GET_TSI_MULT(ObScanner, TSI_COMMON_SCANNER_1);
    if(NULL == scanner)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific ob scanner fail");
    }
    else
    {
      scanner->reset();
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_helper->scan(*param, *scanner);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
    }
    else
    {
      TBSYS_LOG(INFO, "got %ld cells", scanner->get_size());
    }
  }

  ObCellInfo* cell = NULL;
  bool is_row_changed = false;

  while(OB_SUCCESS == ret)
  {
    ret = scanner->next_cell();
    if(OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }

    if(OB_SUCCESS == ret)
    {
      cell = NULL;
      ret = scanner->get_cell(&cell, &is_row_changed);
    }

    if(OB_SUCCESS == ret)
    {
      ret = mutator->del_row(table_name, cell->row_key_);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "add del row to ob mutator fail:ret[%d]", ret);
      }
    }
  }
  
  return ret;
}

int oceanbase::common::dump_schema_manager(const ObSchemaManagerV2& schema_mgr, common::ObClientHelper* client_helper, const ObServer& update_server)
{
  int ret = OB_SUCCESS;
  ObMutator* mutator = NULL;

  if(NULL == client_helper)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "arg client_helper is null");
  }

  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "get thread specific ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = dump_tables(schema_mgr, client_helper, mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "dump tables fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = dump_columns(schema_mgr, client_helper, mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "dump columns fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = dump_joininfo(schema_mgr, client_helper, mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "dump joininfo fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_helper->apply(update_server, *mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator schema table fail:ret[%d]", ret);
    }
  }

  return ret;
}

