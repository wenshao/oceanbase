#include "tblog.h"
#include "data_syntax.h"
#include "common/utility.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "sstable_builder.h"

#include <vector>
#include <string>
namespace oceanbase 
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;

    char DELIMETER = '\1';
    int32_t RAW_DATA_FIELD = 0;
  
    static struct row_key_format *ROW_KEY_ENTRY;
    static struct data_format *ENTRY;

    static int32_t ROW_KEY_ENTRIES_NUM = 0;
    static int32_t DATA_ENTRIES_NUM = 0;
    static int64_t SSTABLE_BLOCK_SIZE = 64 * 1024;
    static int64_t DOUBLE_MULTIPLE_VALUE = 100;
    static int64_t SSTABLE_FORMAT_TYPE = OB_SSTABLE_STORE_DENSE; // dense 1, sparse 2

    static const char *PUBLIC_SECTION = "public";
    static const char *TABLE_ID = "table_id";
    static const char *DELIM = "delim";
    static const char *RAW_DATA_FIELD_CNT = "raw_data_field_cnt";
    static const char *BLOCK_SIZE = "sstable_block_size";
    static const char *MULTIPLE_VALUE = "double_multiple_value";
    static const char *SSTABLE_FORMAT = "sstable_format";

    static const char *ROW_KEY_COLUMN_INFO="row_key_column_info";
    static const char *COLUMN_INFO="column_info";

    static struct data_format DATA_ENTRY[MAX_COLUMS];
    static struct row_key_format ROW_KEY_DATA_ENTRY[MAX_COLUMS];

    static const int DEFAULT_MMAP_THRESHOLD = 64 * 1024 + 128;
    static SSTableBuilder *sstable_builder = NULL;
    static ObSchemaManagerV2 *schema = NULL;
    
    int drop_esc_char(char *buf,int32_t& len)
    {
      int ret = OB_SUCCESS;
      int32_t orig_len = len;
      char *dest = NULL;
      char *ptr = NULL;
      int32_t final_len = len;
      if (NULL == buf)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        dest = buf;
        ptr = buf;
        for(int32_t i=0;i<orig_len-1;++i)
        {
          if ('\\' == *ptr)
          {
            switch(*(ptr+1))
            {
              case '\"':
                *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\'':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              case '\\':
              *dest++ = *(ptr + 1);
              ptr += 2;
              --final_len;
              break;
              default:
              {
                if (dest != ptr)
                  *dest = *ptr;
                ++dest;
                ++ptr;
              }
              break;
            }
          }
          else
          {
            if (dest != ptr)
              *dest = *ptr;
            ++dest;
            ++ptr;
          }
        }
        len = final_len;
      }
      return ret;
    }
    
    int fill_sstable_schema(const ObSchemaManagerV2* schema, uint64_t table_id,
      ObSSTableSchema* sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;
      int cols = 0;
      int32_t size = 0;
      const ObColumnSchemaV2 *col = schema->get_table_schema(table_id, size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR, "cann't find this table:%lu", table_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int col_index = 0; col_index < size; ++col_index)
        {
          memset(&column_def, 0, sizeof(column_def));
          column_def.table_id_ = static_cast<uint32_t>(table_id);
          column_def.column_group_id_ = static_cast<uint16_t>((col + col_index)->get_column_group_id());
          column_def.column_name_id_ = static_cast<uint32_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
          ret = sstable_schema->add_column_def(column_def);
          ++cols;
        }
      }

      if (0 == cols) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }

      return ret;
    }

    int parse_data_syntax(const char *syntax_file, uint64_t& table_id,
      const ObSchemaManagerV2* schema)
    {
      int ret = OB_SUCCESS;
      tbsys::CConfig c1;
      char table_section[20];

      if (NULL == syntax_file || '\0' == syntax_file || NULL == schema)
      {
        TBSYS_LOG(ERROR,"syntax_file is null");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && c1.load(syntax_file) != 0)
      {
        TBSYS_LOG(ERROR,"load syntax file [%s],falied",syntax_file);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        table_id = c1.getInt(PUBLIC_SECTION, TABLE_ID, -1);
        if (table_id == 0 || table_id == OB_INVALID_ID)
        {
          TBSYS_LOG(ERROR, "table_id (%lu) cannot == 0 or OB_INVALID_ID.", table_id);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        DELIMETER = (char)c1.getInt(PUBLIC_SECTION, DELIM, 1);
      }

      if (OB_SUCCESS == ret)
      {
        RAW_DATA_FIELD = c1.getInt(PUBLIC_SECTION, RAW_DATA_FIELD_CNT, 0);
        if (RAW_DATA_FIELD <= 0)
        {
          TBSYS_LOG(ERROR, "RAW_DATA_FIELD (%d) cannot <= 0", RAW_DATA_FIELD);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        SSTABLE_BLOCK_SIZE = c1.getInt(PUBLIC_SECTION, BLOCK_SIZE, 64 * 1024);
        if (SSTABLE_BLOCK_SIZE <= 0)
        {
          TBSYS_LOG(ERROR, "SSTABLE_BLOCK_SIZE (%ld) cannot <= 0", SSTABLE_BLOCK_SIZE);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        DOUBLE_MULTIPLE_VALUE = c1.getInt(PUBLIC_SECTION, MULTIPLE_VALUE, 100);
        if (DOUBLE_MULTIPLE_VALUE <= 0)
        {
          TBSYS_LOG(ERROR, "DOUBLE_MULTIPLE_VALUE (%ld) cannot <= 0", DOUBLE_MULTIPLE_VALUE);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        SSTABLE_FORMAT_TYPE = c1.getInt(PUBLIC_SECTION, SSTABLE_FORMAT, OB_SSTABLE_STORE_DENSE);
        if (SSTABLE_FORMAT_TYPE <= 0 || SSTABLE_FORMAT_TYPE > 2)
        {
          TBSYS_LOG(ERROR, "SSTABLE_FORMAT_TYPE (%ld) cannot <= 0 or > 2", 
            SSTABLE_FORMAT_TYPE);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        snprintf(table_section, sizeof(table_section), "%lu", table_id);
        vector<const char *> row_key_info = 
          c1.getStringList(table_section,ROW_KEY_COLUMN_INFO);
        if (row_key_info.empty())
        {
          TBSYS_LOG(ERROR,"load row key info failed");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          int i = 0;
          int d[4];
          int l = 4;
          for(vector<const char *>::iterator it = row_key_info.begin(); 
              it != row_key_info.end();++it)
          {
            l = 4;
            if ((ret = parse_string_to_int_array(*it,',',d,l)) != OB_SUCCESS || l < 3)
            {
              TBSYS_LOG(ERROR,"deal row key info failed [%s]",*it);
              break;
            }
            ROW_KEY_DATA_ENTRY[i].index = d[0];
            ROW_KEY_DATA_ENTRY[i].type = static_cast<RowKeyType>(d[1]);
            ROW_KEY_DATA_ENTRY[i].len = d[2];

            if (4 == l)
            {
              ROW_KEY_DATA_ENTRY[i].flag = d[3];
            }
            else
            {
              ROW_KEY_DATA_ENTRY[i].flag = 0;
            }
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"row key entry [%d], type:%2d,index:%2d,len:%2d",i,d[1],d[0],d[2]);
#endif
            ++i;
          }

          if (OB_SUCCESS == ret)
          {
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"row key entry num: [%d]",i);
#endif
            ROW_KEY_ENTRIES_NUM = i;
            ROW_KEY_ENTRY = ROW_KEY_DATA_ENTRY;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        vector<const char *> column_info = c1.getStringList(table_section,COLUMN_INFO);
        if (column_info.empty())
        {
          TBSYS_LOG(ERROR,"load column info failed");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          int i = 0;
          int d[2];
          int l = 2;
          //const ObColumnSchemaV2 *column_schema = NULL;
          int32_t column_index[OB_MAX_COLUMN_GROUP_NUMBER];
          int32_t size = OB_MAX_COLUMN_GROUP_NUMBER;
          for(vector<const char *>::iterator it = column_info.begin(); 
              it != column_info.end();++it)
          {
            if ((ret = parse_string_to_int_array(*it,',',d,l)) != OB_SUCCESS || l != 2)
            {
              TBSYS_LOG(ERROR,"deal column info failed [%s]",*it);
              break;
            }
            DATA_ENTRY[i].column_id= d[0];
            DATA_ENTRY[i].index = d[1];
            if (0 != DATA_ENTRY[i].column_id)
            {
              //column_schema = table_schema->find_column_info(DATA_ENTRY[i].column_id);
              ret = schema->get_column_index(table_id,
                static_cast<uint64_t>(DATA_ENTRY[i].column_id),column_index,size);
              if (ret != OB_SUCCESS || size <= 0)
              {
                TBSYS_LOG(ERROR,"find column info from schema failed : %d,ret:%d,size,%d",d[0],ret,size);
                ret = OB_ERROR;
                break;
              }
              DATA_ENTRY[i].type = schema->get_column_schema(column_index[0])->get_type();
              DATA_ENTRY[i].len = static_cast<int32_t>(schema->get_column_schema(column_index[0])->get_size());
            }
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"data entry [%d], id:%2d,index:%2d,type:%2d,len:%2d",i,DATA_ENTRY[i].column_id,
                DATA_ENTRY[i].index,DATA_ENTRY[i].type, DATA_ENTRY[i].len);
#endif
            ++i;
          }

          if (OB_SUCCESS == ret)
          {
#ifdef BUILDER_DEBUG
            TBSYS_LOG(INFO,"data entry num: [%d]",i);
#endif
            DATA_ENTRIES_NUM = i;
            ENTRY = DATA_ENTRY;
          }
        }
      }
      return ret;
    }

    SSTableBuilder::SSTableBuilder()
    : table_id_(0), 
      total_rows_(0),
      table_schema_(NULL),
      sstable_schema_(NULL)
    {
    }
        
    SSTableBuilder::~SSTableBuilder()
    {
      if (NULL != sstable_schema_)
      {
        delete sstable_schema_;
        sstable_schema_ = NULL;
      }
    }

    int SSTableBuilder::init(const uint64_t table_id, const ObSchemaManagerV2* schema)
    {
      int ret = OB_SUCCESS;
     
      if (0 == table_id || OB_INVALID_ID == table_id || NULL == schema)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, schema=%p", table_id, schema);
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = table_id;
        schema_ = schema;
        table_schema_ = schema_->get_table_schema(table_id_);
  
        sstable_schema_ = new ObSSTableSchema();
        if (NULL == sstable_schema_)
        {
          TBSYS_LOG(ERROR,"alloc sstable schema failed");
          ret = OB_ERROR;
        }
        else
        {
          fill_sstable_schema(schema_, table_id_, sstable_schema_);
          const char* compressor_name = table_schema_->get_compress_func_name();
          compressor_string_.assign((char*)compressor_name, static_cast<int32_t>(strlen(compressor_name)));
          memset(row_key_buf_, 0, sizeof(row_key_buf_));
        }
      }

      return ret;
    }

    int SSTableBuilder::start_builder()
    {
      int ret = OB_SUCCESS;
      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if ((ret = schema_->get_column_groups(table_id_, column_group_ids, 
        column_group_num)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
      }
      else 
      {
        if ( 1 == column_group_num)
        {
          TBSYS_LOG(DEBUG,"just have one column group");
        }
        else if ( column_group_num > 1)
        {
          TBSYS_LOG(ERROR, "Not support more than one column groups, "
                           "column_group_num=%d", 
            column_group_num);
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(ERROR,"schema error");
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int SSTableBuilder::append(const char* input, const int64_t input_size, 
      bool is_first, bool is_last, const char** output, int64_t* output_size)
    {
      int ret = OB_SUCCESS;
      int fields = 0;
      int64_t pos = 0;
      ObTrailerParam trailer_param;
      *output = NULL;
      *output_size = 0;

      if (is_first)
      {
        trailer_param.compressor_name_ = compressor_string_;
        trailer_param.table_version_ = 1;
        trailer_param.store_type_ = static_cast<int32_t>(SSTABLE_FORMAT_TYPE);
        trailer_param.block_size_ = SSTABLE_BLOCK_SIZE;
        trailer_param.frozen_time_ = tbsys::CTimeUtil::getTime();
        if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,
          trailer_param)))
        {
          TBSYS_LOG(ERROR,"create sstable failed ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret && NULL != input && input_size > 0)
      {
        while(OB_SUCCESS == ret && read_line(input, input_size, pos, fields) != NULL)
        {
          ret = process_line(fields);
          if (OB_SUCCESS == ret)
          {
            if ((ret = writer_.append_row(sstable_row_, current_sstable_size_)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "append_row failed, current_sstable_size_=%ld", 
                current_sstable_size_);
            }
            else
            {
              ++total_rows_;
            }
          }
        }
      }

      if (OB_SUCCESS == ret && is_last)
      {
        ret = close_sstable();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "close sstable failed");
        }
      }

      if (OB_SUCCESS == ret)
      {
        *output = writer_.get_write_buf(*output_size);
        writer_.reset_data_size();
      }

      return ret;
    }

    const char *SSTableBuilder::read_line(const char* input, 
      const int64_t input_size, int64_t& pos, int &fields)
    {
      const char *line = NULL;
      fields = 0;

      if (NULL == input || input_size <= 0 || pos >= input_size || pos < 0)
      {
        line = NULL;
      }
      else
      {
        while (pos < input_size)
        {
          line = input + pos;
          char *phead = (char*)line;
          char *ptail = phead;
          const char *pend = input + input_size;
          int i = 0;
          while (ptail < pend && *ptail != '\n')
          {
            while(ptail < pend && (*ptail != DELIMETER) && (*ptail != '\n'))
              ++ptail;
            if (ptail >= pend)
            {
              TBSYS_LOG(ERROR, "input buffer size over follow, ptail=%p, pend=%p",
                ptail, pend);
              line = NULL;
              break;
            }
            colums_[i].column_ = phead;
            colums_[i++].len_ = static_cast<int32_t>(ptail - phead);
            if ('\n' == *ptail)
            {
              *ptail= '\0';
              pos += ptail - line + 1;
              break;
            }
            else
            {
              *ptail++ = '\0';
            }
            phead = ptail;
          }

          if (ptail >= pend)
          {
            TBSYS_LOG(ERROR, "input buffer size over follow, ptail=%p, pend=%p",
              ptail, pend);
            line = NULL;
            break;
          }
  
          if ('\n' == *ptail)
          {
            pos += ptail - line + 1;
            if ('\0' == *(ptail - 1))
            {
              colums_[i++].len_ = 0;
            }
          }
          fields = i;
  
          //check 
          if (RAW_DATA_FIELD != 0 && fields < RAW_DATA_FIELD)
          {
            TBSYS_LOG(ERROR,"raw data expect %d fields,but %d",
              RAW_DATA_FIELD, fields);
            line = NULL;
            continue;
          }
          else
          {
            break;
          }
        }
      }

#ifdef BUILDER_DEBUG
      if (line != NULL)
      {
        TBSYS_LOG(DEBUG,"fields : %d",fields);
        for(int i=0;i<fields;++i)
        {
          TBSYS_LOG(DEBUG,"%d : type:%d,val: %s",i,ENTRY[i].type,colums_[i].column_);
        }
      }
#endif

      return line;
    }
    
    int SSTableBuilder::process_line(int fields)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      int j = 0;
      int64_t pos = 0;
      int64_t val = 0;

      if (fields <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = create_rowkey(row_key_buf_, ROW_KEY_BUF_LEN, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to create rowkey, fields=%d", fields);
        }

        for(; i < DATA_ENTRIES_NUM && OB_SUCCESS == ret; ++i)
        {
          if (0 == ENTRY[i].column_id) //row key
          {
            continue;
          }

          if (-1 == ENTRY[i].index) //new data ,add a null obj
          {
            if (SSTABLE_FORMAT_TYPE == OB_SSTABLE_STORE_SPARSE)
            {
              column_id_[j] = ENTRY[i].column_id;
              row_value_[j++].set_ext(ObActionFlag::OP_NOP);
            }
            else
            {
              row_value_[j++].set_null();
            }
          }
          else if ( ENTRY[i].index >= fields)
          {
            TBSYS_LOG(ERROR,"data format error?");
          }
          else
          {
            if (SSTABLE_FORMAT_TYPE == OB_SSTABLE_STORE_SPARSE)
            {
              column_id_[j] = ENTRY[i].column_id;
            }
            switch(ENTRY[i].type)
            {
              case ObIntType:
                {
                  char *p = colums_[ENTRY[i].index].column_;
                  int64_t v = 0;
                  if (p != NULL)
                  {
                    if (strchr(p,'.') != NULL) //float/double to int
                    {
                      double a = atof(p);
                      a *= (double)DOUBLE_MULTIPLE_VALUE;
                      v = static_cast<int64_t>(a);
                    }
                    else
                    {
                      v = atol(colums_[ENTRY[i].index].column_);
                    }
                  }
                  row_value_[j++].set_int(v);
                }
                break;
              case ObFloatType:
                row_value_[j++].set_float(strtof(colums_[ENTRY[i].index].column_,NULL));
                break;
              case ObDoubleType:
                row_value_[j++].set_double(atof(colums_[ENTRY[i].index].column_));
                break;
            case ObDateTimeType:
                ret = transform_date_to_time(colums_[ENTRY[i].index].column_, val);
                if (OB_SUCCESS == ret)
                {
                  row_value_[j++].set_datetime(val);
                }
                break;
            case ObModifyTimeType:
                ret = transform_date_to_time(colums_[ENTRY[i].index].column_, val);
                if (OB_SUCCESS == ret)
                {
                  row_value_[j++].set_modifytime(val * 1000 * 1000L); //seconds -> ms
                }
                break;
            case ObCreateTimeType:
                ret = transform_date_to_time(colums_[ENTRY[i].index].column_, val);
                if (OB_SUCCESS == ret)
                {
                  row_value_[j++].set_createtime(val * 1000 * 1000L);
                }
                break;
              case ObVarcharType:
                {
                  ObString bstring;
                  if ( colums_[ENTRY[i].index].len_ > 0)
                  {
                    int32_t len = colums_[ENTRY[i].index].len_;
                    char *obuf = colums_[ENTRY[i].index].column_;
                    drop_esc_char(obuf,len);
                    bstring.assign(obuf,len);
                  }
                  row_value_[j++].set_varchar(bstring);
                }
                break;
            case ObPreciseDateTimeType:
                ret = transform_date_to_time(colums_[ENTRY[i].index].column_, val);
                if (OB_SUCCESS == ret)
                {
                  row_value_[j++].set_precise_datetime(val * 1000 * 1000L); //seconds -> ms
                }
                break;
              default:
                TBSYS_LOG(DEBUG,"unexpect type index: %d,type:%d",i,ENTRY[i].type);
                continue;
                break;
            }
          }
        }
      }
#ifdef BUILDER_DEBUG
      common::hex_dump(row_key_.ptr(),row_key_.length(),true);
      for(int k=0;k<j;++k)
      {
        TBSYS_LOG(DEBUG,"%d,type:%d",k,row_value_[k].get_type());
        row_value_[k].dump(TBSYS_LOG_LEVEL_INFO);
      }
#endif

      if (OB_SUCCESS == ret)
      {
        sstable_row_.clear();
        if ((ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS )
        {
          TBSYS_LOG(WARN,"set_row_key failed:%hd [%d]\n",row_key_.length(),ret);
        }
        else
        {
          sstable_row_.set_table_id(table_id_);
          sstable_row_.set_column_group_id(0);
        }
  
        for(int k=0;k<j && OB_SUCCESS == ret;++k)
        {
          if (SSTABLE_FORMAT_TYPE == OB_SSTABLE_STORE_SPARSE)
          {
            if ( (ret = sstable_row_.add_obj(row_value_[k], column_id_[k])) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"add_obj failed:%d\n",ret);
            }
          }
          else {
            if ( (ret = sstable_row_.add_obj(row_value_[k])) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"add_obj failed:%d\n",ret);
            }
          }
        }
      }
      return ret;
    }

    int SSTableBuilder::close_sstable()
    {
      int64_t t = 0;
      int64_t sst_size = 0;
      int ret = OB_SUCCESS;

      if ((ret = writer_.close_sstable(t, sst_size)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"close_sstable failed [%d]", ret);
      }
      
      return ret;
    }

    int SSTableBuilder::transform_date_to_time(const char *str, ObDateTime& val)
    {
      int err = OB_SUCCESS;
      struct tm time;
      time_t tmp_time = 0;
      val = -1;
      if (NULL != str && *str != '\0')
      {
        if (strchr(str, '-') != NULL)
        {
          if (strchr(str, ':') != NULL)
          {
            if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else
          {
            if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        else
        {
          if (strchr(str, ':') != NULL)
          {
            if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else if (strlen(str) > 8)
          {
            if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                    &time.tm_mon,&time.tm_mday,&time.tm_hour,
                    &time.tm_min,&time.tm_sec)) != 6)
            {
              err = OB_ERROR;
            }
          }
          else
          {
            if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                    &time.tm_mday)) != 3)
            {
              err = OB_ERROR;
            }
            time.tm_hour = 0;
            time.tm_min = 0;
            time.tm_sec = 0;
          }
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"sscanf failed : [%s] ",str);
        }
        else
        {
          time.tm_year -= 1900;
          time.tm_mon -= 1;
          time.tm_isdst = -1;

          if ((tmp_time = mktime(&time)) != -1)
          {
            val = tmp_time;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to mktime");
            err = OB_ERROR;
          }
        }
      }
      return err;    
    }

    int SSTableBuilder::create_rowkey(char *buf,int64_t buf_len,int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf || buf_len < 0 || pos < 0)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for(int32_t i=0; i < ROW_KEY_ENTRIES_NUM; ++i)
        {
          switch(ROW_KEY_ENTRY[i].type)
          {
            case INT8:
              {
                int val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i8(buf,buf_len,pos,static_cast<int8_t>(val));
                serialize_size_ += 1;
              }
              break;
            case INT16:
              {
                int val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i16(buf,buf_len,pos,static_cast<int16_t>(val));
                serialize_size_ += 2;
              }
              break;
            case INT32:
              {
                int32_t val = atoi(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i32(buf,buf_len,pos,val);
                serialize_size_ += 4;
              }
              break;
            case INT64:
              {
                int64_t val = atol(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i64(buf,buf_len,pos,val);
                serialize_size_ += 8;
              }
              break;
            case VARCHAR:
              {
                if (colums_[ROW_KEY_ENTRY[i].index].len_ + pos <= ROW_KEY_BUF_LEN)
                {
                  memcpy(buf + pos,colums_[ROW_KEY_ENTRY[i].index].column_,colums_[ROW_KEY_ENTRY[i].index].len_);
                  pos += colums_[ROW_KEY_ENTRY[i].index].len_;
                  if (ROW_KEY_ENTRY[i].flag != 0)
                  {
                    *(buf + pos) = '\0';
                    ++pos;
                  }
                  serialize_size_ += colums_[ROW_KEY_ENTRY[i].index].len_;
                }
                else
                {
                  TBSYS_LOG(WARN, "rowkey buf over flow, pos=%ld, buf_size=%d, entry+len=%d",
                    pos, ROW_KEY_BUF_LEN, colums_[ROW_KEY_ENTRY[i].index].len_);
                  ret = OB_ERROR;
                }
              }
              break;
            case DATETIME:
              {
                int64_t val = 0;
                ret = transform_date_to_time(colums_[ROW_KEY_ENTRY[i].index].column_, val);
                if (OB_SUCCESS == ret)
                {
                  ret = serialization::encode_i64(buf,buf_len,pos,val * 1000L * 1000L);
                  serialize_size_ += 8;
                }
              }
              break;
            default:
              break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_key_.assign(buf,static_cast<int32_t>(pos));
      }
      return ret;
    }
  }
}

using namespace oceanbase;
using namespace oceanbase::chunkserver;

int init(const char* schema_file, const char* syntax_file)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;

  ::mallopt(M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD); 
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");

  tbsys::CConfig c1;
  schema = new (std::nothrow)ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  if (!schema->parse_from_file(schema_file, c1))
  {
    TBSYS_LOG(ERROR, "parse schema file failed");
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    if ((ret = parse_data_syntax(syntax_file, table_id, schema)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR,"parse_data_syntax failed : [%d]",ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    const ObTableSchema *table_schema = schema->get_table_schema(table_id);
    if (NULL == table_schema)
    {
      TBSYS_LOG(ERROR, "table schema is null");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    sstable_builder = new SSTableBuilder();
    if (NULL == sstable_builder)
    {
      TBSYS_LOG(ERROR, "new sstable_builder failed");
      ret = OB_ERROR;
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    if (sstable_builder->init(table_id, schema) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "sstable_builder init failed");
    }
    else
    {
      ret = sstable_builder->start_builder();
    }
  }

  return ret;
}

int append(const char* input, const int64_t input_size, 
  bool is_first, bool is_last, const char** output, int64_t* output_size)
{
  return sstable_builder->append(input, input_size, is_first, 
    is_last, output, output_size);
}

void clolse()
{
  if (NULL != sstable_builder)
  {
    delete sstable_builder;
    sstable_builder = NULL;
  }

  if (NULL != schema)
  {
    delete schema;
    schema = NULL;
  }
}

/*
 * Class:     com_taobao_mrsstable_SSTableBuilder
 * Method:    init
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_com_taobao_mrsstable_SSTableBuilder_init
  (JNIEnv *env, jobject arg, jstring schema, jstring syntax)
{
  jint ret = 0;
  const char *schema_file = env->GetStringUTFChars(schema, JNI_FALSE); 
  const char *syntax_file = env->GetStringUTFChars(syntax, JNI_FALSE);
  (void)arg; 

  ret = init(schema_file, syntax_file);
  env->ReleaseStringUTFChars(schema, (const char*)schema_file);
  env->ReleaseStringUTFChars(syntax, (const char*)syntax_file);

  return ret;
}

/*
 * Class:     com_taobao_mrsstable_SSTableBuilder
 * Method:    append
 * Signature: (Ljava/nio/ByteBuffer;ZZ)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_com_taobao_mrsstable_SSTableBuilder_append
  (JNIEnv *env, jobject arg, jobject input, jboolean is_first, jboolean is_last)
{
  jint ret = 0;
  void* output = NULL;
  jlong output_size = 0;
  void* input_buf = env->GetDirectBufferAddress(input);
  jlong input_size = env->GetDirectBufferCapacity(input);
  (void)arg;

  ret = append((const char*)input_buf, input_size, is_first, 
    is_last, (const char**)&output, &output_size);
  if (0 != ret)
  {
    fprintf(stderr,"append data failed, input=%p, input_size=%ld", 
      input, input_size);
    return env->NewDirectByteBuffer((void*)input, 0);
  }

  return env->NewDirectByteBuffer((void*)output, output_size);
}

/*
 * Class:     com_taobao_mrsstable_SSTableBuilder
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_taobao_mrsstable_SSTableBuilder_close
  (JNIEnv *env, jobject arg)
{
  (void)env;
  (void)arg;
  clolse();
}
