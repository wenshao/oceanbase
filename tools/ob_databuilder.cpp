#include <iostream>
#include <stdio.h>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include "tblog.h"
#include "data_syntax.h"
#include "common/utility.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "common/ob_range.h"
#include "ob_tablet_meta.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "ob_databuilder.h"

#include <vector>
#include <string>

const char *g_sstable_directory = "./";

namespace oceanbase 
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;

    char DELIMETER = '\0';
    int32_t RAW_DATA_FILED = 0;
  
    static struct row_key_format *ROW_KEY_ENTRY;
    static struct data_format *ENTRY;

    static int32_t ROW_KEY_ENTRIES_NUM = 0;
    static int32_t DATA_ENTRIES_NUM = 0;

    static const char *ROW_KEY_COLUMN_INFO="row_key_column_info";
    static const char *COLUMN_INFO="column_info";

    static struct data_format DATA_ENTRY[MAX_COLUMS];
    static struct row_key_format ROW_KEY_DATA_ENTRY[MAX_COLUMS];

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
    
    int fill_sstable_schema(const ObSchemaManagerV2* schema,uint64_t table_id,ObSSTableSchema* sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;
     // memset(&sstable_schema,0,sizeof(sstable_schema));
      
      int cols = 0;
      int32_t size = 0;

      const ObColumnSchemaV2 *col = schema->get_table_schema(table_id,size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this table:%lu",table_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int col_index = 0; col_index < size; ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));
          column_def.table_id_ = static_cast<uint32_t>(table_id);
          column_def.column_group_id_ = static_cast<uint16_t>((col + col_index)->get_column_group_id());
          column_def.column_name_id_ = static_cast<uint32_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
          ret = sstable_schema->add_column_def(column_def);
          ++cols;
        }
      }

      if ( 0 == cols ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      return ret;
    }

    int parse_data_syntax(const char *syntax_file,uint64_t table_id,const ObSchemaManagerV2* schema)
    {
      int ret = OB_SUCCESS;
      tbsys::CConfig c1;
      char table_section[20];

      snprintf(table_section,sizeof(table_section),"%ld",table_id);

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
        vector<const char *> row_key_info = c1.getStringList(table_section,ROW_KEY_COLUMN_INFO);
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
          for(vector<const char *>::iterator it = row_key_info.begin(); it != row_key_info.end();++it)
          {
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
          for(vector<const char *>::iterator it = column_info.begin(); it != column_info.end();++it)
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
              ret = schema->get_column_index(table_id,static_cast<uint64_t>(DATA_ENTRY[i].column_id),column_index,size);
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

    ObDataBuilder::ObDataBuilder():fp_(NULL),
                                   file_no_(-1),
                                   first_key_(true),
                                   reserve_ids_(-1),
                                   current_sstable_id_(SSTABLE_ID_BASE),
                                   current_sstable_size_(0),
                                   row_key_buf_index_(0),
                                   wrote_keys_(0),
                                   read_lines_(0),
                                   table_id_(0),
                                   total_rows_(0),
                                   table_schema_(NULL),
                                   sstable_schema_(NULL)
    {
    }
        
    ObDataBuilder::~ObDataBuilder()
    {
      if (NULL != sstable_schema_)
      {
        delete sstable_schema_;
        sstable_schema_ = NULL;
      }
    }

    int ObDataBuilder::init(ObDataBuilderArgs& args)
    {
      int ret = OB_SUCCESS;
      fp_ = NULL;
      file_no_ = args.file_no_;
      reserve_ids_ = args.reserve_ids_;
      max_sstable_size_ = args.max_sstable_size_;

      current_sstable_id_ = SSTABLE_ID_BASE;
      current_sstable_size_ = 0;
      row_key_buf_index_ = 0;
      wrote_keys_ = 0;
      read_lines_ = 0;
      
      table_id_ = args.table_id_;
      schema_ = args.schema_;
      table_schema_ = args.schema_->get_table_schema(table_id_);

      sstable_schema_ = new ObSSTableSchema();

      if (NULL == sstable_schema_)
      {
        TBSYS_LOG(ERROR,"alloc sstable schema failed");
        ret = OB_ERROR;
      }
      else
      {
        fill_sstable_schema(schema_,table_id_,sstable_schema_);

        split_pos_ = table_schema_->get_split_pos();

        disk_no_ = args.disk_no_;

        strncpy(filename_,args.filename_,strlen(args.filename_));
        filename_[strlen(args.filename_)] = '\0';

        strncpy(dest_dir_,args.dest_dir_,strlen(args.dest_dir_));
        dest_dir_[strlen(args.dest_dir_)] = '\0';

        snprintf(index_file_path_,sizeof(index_file_path_),"%s/%d.idx_%d",dest_dir_,file_no_,disk_no_);

        //strncpy(compressor_name_,"lzo_1.0",7); //TODO
        //compressor_name_[strlen(compressor_name_)] = '\0';
        compressor_string_.assign((char*)args.compressor_string_,static_cast<int32_t>(strlen(args.compressor_string_)));

        memset(row_key_buf_[0],0,sizeof(row_key_buf_[0]));
        memset(row_key_buf_[1],0,sizeof(row_key_buf_[1]));
        memset(last_end_key_buf_,0,sizeof(last_end_key_buf_));

        if ((ret = parse_data_syntax(args.syntax_file_,table_id_,schema_)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"parse_data_syntax failed : [%d]",ret);
        }
        last_end_key_.assign_ptr(last_end_key_buf_,table_schema_->get_rowkey_max_length());
        image_.set_data_version(1);
      }

      return ret;
    }

    int ObDataBuilder::start_builder()
    {
      int ret = OB_SUCCESS;

      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if ( (ret = schema_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
      }
      else 
      {
        if ( 1 == column_group_num)
        {
          TBSYS_LOG(DEBUG,"just have one column group");
          ret = build_with_no_cache();
        }
        else if ( column_group_num > 1)
        {
          ret = build_with_cache();
        }
        else
        {
          TBSYS_LOG(ERROR,"schema error");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObDataBuilder::build_with_no_cache()
    {
      int fields = 0;
      int ret = 0;
      if ((NULL == fp_) && ( NULL == (fp_ = fopen(filename_,"r"))))
      {
        TBSYS_LOG(ERROR,"open file error:%s\n",strerror(errno));
        ret = OB_ERROR;
      }
      else 
      {
        prepare_new_sstable();
        if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,dest_file_string_,compressor_string_,1L)))
        {
          TBSYS_LOG(ERROR,"create sstable failed [%d]\n",ret);
        }
        else
        {
          while(OB_SUCCESS == ret && read_line(fields) != NULL)
          {
            process_line(fields);

            if ( (current_sstable_size_ > 0) && (current_sstable_size_ >= max_sstable_size_))
            {
              //TBSYS_LOG(DEBUG,"split_pos:%ld",split_pos_);
              //common::hex_dump(row_key_buf_[0],split_pos_,false);
              //common::hex_dump(row_key_buf_[1],split_pos_,false);
              if (0 == split_pos_  || memcmp(row_key_buf_[0],row_key_buf_[1],split_pos_) != 0)
              {
                //chang file
                TBSYS_LOG(INFO,"close_sstable,change file\n");
                close_sstable();
                prepare_new_sstable();
                if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,dest_file_string_,compressor_string_,table_id_)))
                {
                  TBSYS_LOG(ERROR,"create sstable failed [%d]",ret);
                }
                current_sstable_size_ = 0;
                wrote_keys_ = 0;
              }
            }
            //common::hex_dump(sstable_row_.get_row_key().ptr(),sstable_row_.get_row_key().length(),false);

            if (writer_.append_row(sstable_row_,current_sstable_size_) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"append_row failed:%ld\n",read_lines_);
            }
            else
            {
              ++wrote_keys_;
              ++total_rows_;
              last_key_ = row_key_;
            }
          }
        }

        fclose(fp_);
        if (wrote_keys_ > 0)
        {
          close_sstable();
        }
        else
        {
          //don't write index
          int64_t t = 0;
          writer_.close_sstable(t);
        }
        //ok,now we write meta info to the index file
        ret = write_idx_info();
      }
      TBSYS_LOG(INFO,"total_rows:%ld\n",total_rows_);
      return ret;
    }
    
    int ObDataBuilder::build_with_cache()
    {
      int fields = 0;
      int ret = OB_SUCCESS;
      int64_t app_size = 0;

      bool have_last_key = false;
      int64_t total_line = 0;

      if ((NULL == fp_) && ( NULL == (fp_ = fopen(filename_,"r"))))
      {
        TBSYS_LOG(ERROR,"open file error:%s\n",strerror(errno));
        ret = OB_ERROR;
      }

      prepare_new_sstable();

      if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,dest_file_string_,compressor_string_,1L)))
      {
        TBSYS_LOG(ERROR,"create sstable failed [%d]\n",ret);
      }

      while(OB_SUCCESS == ret)
      {
        serialize_size_ = 0;

        while( NULL != read_line(fields) )
        {
          if ((OB_SUCCESS == (ret = process_line_with_cache(fields,serialize_size_))))
          {
            ++read_lines_;
            if (serialize_size_ >= max_sstable_size_)
            {
              if ( 0 == split_pos_  ||
                   memcmp(key_array_[read_lines_ - 1].ptr(),key_array_[read_lines_ - 2].ptr(),split_pos_) != 0)
              {
                have_last_key = true;
                break;
              }
            }
          }
        }
        
        if (have_last_key)
        {
          total_line = read_lines_ - 1;
        }
        else
        {
          total_line = read_lines_;
          TBSYS_LOG(INFO,"end of this file,line:%ld",total_line);
        }
      
        TBSYS_LOG(INFO,"table_schema_:%p",table_schema_);
        
        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);


        if ( (ret = schema_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }

        for(int32_t i = 0; i < column_group_num && OB_SUCCESS == ret; ++i)
        {
          for(int32_t line = 0; line < total_line; ++line)
          {
            sstable_row_.clear();
            sstable_row_.set_table_id(table_id_);
            sstable_row_.set_column_group_id(column_group_ids[i]);
            sstable_row_.set_row_key(key_array_[line]);

            int32_t column_num = 0;
            const ObColumnSchemaV2 *id = schema_->get_group_schema(table_id_,column_group_ids[i], column_num);

            if (NULL == id || column_num <= 0)
            {
              TBSYS_LOG(ERROR,"get column group schema failed");
              ret = OB_ERROR;
            }

            for(int32_t column_index = 0; column_index < column_num && OB_SUCCESS == ret; ++column_index)
            {
              if ( (ret = sstable_row_.add_obj( values_[line][ (id + column_index)->get_id() - 2])) != OB_SUCCESS )
              {
                TBSYS_LOG(ERROR,"add obj to row failed : [%d]",ret);
              }
            }
            if (OB_SUCCESS == ret)
            {
              if ( (ret = writer_.append_row(sstable_row_,app_size)) != OB_SUCCESS )
              {
                TBSYS_LOG(ERROR,"append row failed");
              }
            }
          }
        }

        last_key_ = key_array_[total_line - 1];

        close_sstable();

        if (!have_last_key)
        {
          TBSYS_LOG(INFO,"end of file");
          break; //eof
        }

        prepare_new_sstable();
        
        if (OB_SUCCESS != (ret = writer_.create_sstable(*sstable_schema_,dest_file_string_,compressor_string_,table_id_)))
        {
          TBSYS_LOG(ERROR,"create sstable failed [%d]\n",ret);
        }

        if (OB_SUCCESS == ret)
        {
          for(int32_t i = 0; i < column_group_num && OB_SUCCESS == ret; ++i)
          {
            for(int32_t line = 0; line < total_line; ++line)
            {
              sstable_row_.clear();
              sstable_row_.set_table_id(table_id_);
              sstable_row_.set_column_group_id(column_group_ids[i]);
              sstable_row_.set_row_key(key_array_[line]);

              int32_t column_num = 0;
              const ObColumnSchemaV2 *id = schema_->get_group_schema(table_id_,column_group_ids[i], column_num);
              for(int32_t column_index = 0; i < column_num; ++column_index)
              {
                sstable_row_.add_obj( values_[line][ id->get_id() - 2] );
              }
              writer_.append_row(sstable_row_,app_size);
            }
          }

          have_last_key = false;
          read_lines_ = 0;
        }
        arena_.free();
      }
      ret = write_idx_info();
      return ret;
    }

    char *ObDataBuilder::read_line(int &fields)
    {
      char *line = NULL;
      static int buf_idx = 0;
      while(true)
      {
        if ( (line = fgets(buf_[buf_idx%2],sizeof(buf_[buf_idx%2]),fp_)) != NULL)
        {
          //++read_lines_;
          char *phead = buf_[buf_idx%2];
          char *ptail = phead;
          int i = 0;
          while (*ptail != '\n')
          {
            while( (*ptail != DELIMETER) && (*ptail != '\n'))
              ++ptail;
            colums_[i].column_ = phead;
            colums_[i++].len_ = static_cast<int32_t>(ptail - phead);
            if ('\n' == *ptail)
            {
              *ptail= '\0';
              break;
            }
            else
            {
              *ptail++ = '\0';
              //++ptail;
            }
            phead = ptail;
          }

          if ( '\n' == *ptail && '\0' == *(ptail - 1))
          {
            colums_[i++].len_ = 0;
          }
          
          //colums_[i] = NULL;
          fields = i;

          //check 
          if (RAW_DATA_FILED != 0 && fields != RAW_DATA_FILED)
          {
            TBSYS_LOG(ERROR,"%ld : raw data expect %d fields,but %d",read_lines_,RAW_DATA_FILED,fields);
            continue;
          }

          ++buf_idx;

          int64_t row_key_len = 0;

          for(int j=0;j<i;++j)
          {
            if ( 0 == ENTRY[j].column_id)
            {
              row_key_len += strlen(colums_[j].column_) + 1;
            }
            else
            {
              break; //row_key always at the begin
            }
          }

          if (0 == memcmp(buf_[0],buf_[1],row_key_len)) 
          {
            TBSYS_LOG(DEBUG,"dont deal the same line,%ld,%d,%ld\n",read_lines_,buf_idx,row_key_len);
            continue;
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
        break;
      }
      return line;
    }
    
    int ObDataBuilder::process_line(int fields)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      int j = 0;
      int64_t pos = 0;

      if (fields <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        create_rowkey(row_key_buf_[row_key_buf_index_++ % 2],ROW_KEY_BUF_LEN,pos);

        for(;i < DATA_ENTRIES_NUM; ++i)
        {
          if (0 == ENTRY[i].column_id) //row key
          {
            continue;
          }

          if (-1 == ENTRY[i].index) //new data ,add a null obj
          {
            row_value_[j++].set_null();
          }
          else if ( ENTRY[i].index >= fields)
          {
            TBSYS_LOG(ERROR,"data format error?");
          }
          else
          {
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
                      a *= 100.0;
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
                row_value_[j++].set_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_));
                break;
              case ObModifyTimeType:
                row_value_[j++].set_modifytime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
                break;
              case ObCreateTimeType:
                row_value_[j++].set_createtime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L);
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
                row_value_[j++].set_precise_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
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

      for(int k=0;k<j;++k)
      {
        if ( (ret = sstable_row_.add_obj(row_value_[k])) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"add_obj failed:%d\n",ret);
        }
      }
      return ret;
    }

    int ObDataBuilder::process_line_with_cache(int fields,int64_t& serialize_size_)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      int j = 0;
      int64_t pos = 0;
      ObObj *v = values_[read_lines_];
      
      if (fields <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        create_rowkey(row_key_buf_[row_key_buf_index_++ % 2],ROW_KEY_BUF_LEN,pos);

        for(;i < DATA_ENTRIES_NUM; ++i)
        {
          if (0 == ENTRY[i].column_id) //row key
          {
            continue;
          }

          if (-1 == ENTRY[i].index) //new data ,add a null obj
          {
            v[j].set_null();
            serialize_size_ += v[j].get_serialize_size();
            ++j;
          }
          else if ( ENTRY[i].index >= fields)
          {
            TBSYS_LOG(ERROR,"data format error?");
          }
          else
          {
            switch(ENTRY[i].type)
            {
              case ObIntType:
                {
                  char *p = colums_[ENTRY[i].index].column_;
                  int64_t v1 = 0;
                  if (p != NULL)
                  {
                    if (strchr(p,'.') != NULL) //float/double to int
                    {
                      double a = atof(p);
                      a *= 100.0;
                      v1 = static_cast<int64_t>(a);
                    }
                    else
                    {
                      v1 = atol(colums_[ENTRY[i].index].column_);
                    }
                  }
                  v[j].set_int(v1);
                  serialize_size_ += v[j].get_serialize_size();
                  ++j;

                }
                break;
              case ObFloatType:
                v[j].set_float(strtof(colums_[ENTRY[i].index].column_,NULL));
                serialize_size_ += v[j].get_serialize_size();
                ++j;
                break;
              case ObDoubleType:
                v[j].set_double(atof(colums_[ENTRY[i].index].column_));
                serialize_size_ += v[j].get_serialize_size();
                ++j;
                break;
              case ObDateTimeType:
                v[j].set_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_));
                serialize_size_ += v[j].get_serialize_size();
                ++j;
                break;
              case ObModifyTimeType:
                v[j].set_modifytime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
                serialize_size_ += v[j].get_serialize_size();
                ++j;
                break;
              case ObCreateTimeType:
                v[j].set_createtime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L);
                serialize_size_ += v[j].get_serialize_size();
                ++j;
                break;
              case ObVarcharType:
                {
                  ObString bstring;
                  if ( colums_[ENTRY[i].index].len_ > 0)
                  {
                    int32_t len = colums_[ENTRY[i].index].len_;
                    char *obuf = colums_[ENTRY[i].index].column_;
                    drop_esc_char(obuf,len);
                    char *nbuf = arena_.alloc(len);
                    memcpy(nbuf,obuf,len);
                    bstring.assign(nbuf,len);
                  }
                  v[j].set_varchar(bstring);
                  serialize_size_ += v[j].get_serialize_size();
                  ++j;
                }
                break;
              case ObPreciseDateTimeType:
                v[j].set_precise_datetime(transform_date_to_time(colums_[ENTRY[i].index].column_) * 1000 * 1000L); //seconds -> ms
                serialize_size_ += v[j].get_serialize_size();
                ++j;
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
        TBSYS_LOG(DEBUG,"%d,type:%d",k,v[k].get_type());
        v[k].dump(TBSYS_LOG_LEVEL_INFO);
      }
#endif

      if (OB_SUCCESS == ret)
      {
        char *p = arena_.alloc(row_key_.length());
        memcpy(p,row_key_.ptr(),row_key_.length());
        key_array_[read_lines_].assign(p,row_key_.length());
      }
      return ret;
    }

    int ObDataBuilder::prepare_new_sstable()
    {
      if (SSTABLE_ID_BASE == current_sstable_id_)
      {
        current_sstable_id_ += (file_no_ * reserve_ids_) + 1;
      }
      else 
      {
        ++current_sstable_id_;
      }

      uint64_t real_id = (current_sstable_id_ << 8)|((disk_no_ & 0xff));
      id_.sstable_file_id_ = real_id;
      id_.sstable_file_offset_ = 0;
      
      snprintf(dest_file_,sizeof(dest_file_),"%s/%ld",dest_dir_,real_id);
      dest_file_string_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));

      return OB_SUCCESS;
    }

    int ObDataBuilder::close_sstable()
    {
      int64_t t = 0;
      int ret = OB_SUCCESS;
      if ( (ret = writer_.close_sstable(t)) != OB_SUCCESS )
      {
        TBSYS_LOG(WARN,"close_sstable failed [%d]",ret);
      }
      else
      {

        ObTablet *tablet = NULL;
        ObRange range;

        range.border_flag_.set_inclusive_end();
        range.start_key_ = last_end_key_;
        range.end_key_ = last_key_;
        range.table_id_ = table_id_;


        if (split_pos_ > 0)
        {
          memset(range.end_key_.ptr() + split_pos_,0xff, 
               range.end_key_.length() - split_pos_);
        }

        hex_dump(range.start_key_.ptr(),range.start_key_.length(),true);
        hex_dump(range.end_key_.ptr(),range.end_key_.length(),true);

        if (((ret = image_.alloc_tablet_object(range,tablet)) != OB_SUCCESS) || NULL == tablet)
        {
          TBSYS_LOG(ERROR,"alloc_tablet_object failed [%d]",ret);
        }

        if (OB_SUCCESS == ret && tablet != NULL)
        {
          tablet->set_data_version(1);
          tablet->add_sstable_by_id(id_);
          tablet->set_disk_no(disk_no_);
        }

        if (OB_SUCCESS == ret && ((ret = image_.add_tablet(tablet)) != OB_SUCCESS))
        {
          TBSYS_LOG(ERROR,"add tablet failed [%d]",ret);
        }

        memcpy(last_end_key_buf_,range.end_key_.ptr(),range.end_key_.length());
        last_end_key_.assign_ptr(last_end_key_buf_,range.end_key_.length());
      }
      return ret;
    }

    int ObDataBuilder::write_idx_info()
    {
      int ret = OB_SUCCESS;

      if ((ret = image_.write(index_file_path_,0)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"write meta info failed");
      }
      return ret;
    }

    ObDateTime ObDataBuilder::transform_date_to_time(const char *str)
    {
      int err = OB_SUCCESS;
      struct tm time;
      ObDateTime t = 0;
      time_t tmp_time = 0;
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
          TBSYS_LOG(ERROR,"sscanf failed : [%s] ",str);
          TBSYS_LOG(ERROR,"line,%ld\n",read_lines_);
          t = atol(str);
        }
        else
        {
          time.tm_year -= 1900;
          time.tm_mon -= 1;
          time.tm_isdst = -1;

          if ((tmp_time = mktime(&time)) != -1)
          {
            t = tmp_time;
          }
        }
      }
      return t;
    }

    int ObDataBuilder::create_rowkey(char *buf,int64_t buf_len,int64_t& pos)
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
                memcpy(buf + pos,colums_[ROW_KEY_ENTRY[i].index].column_,colums_[ROW_KEY_ENTRY[i].index].len_);
                pos += colums_[ROW_KEY_ENTRY[i].index].len_;
                if (ROW_KEY_ENTRY[i].flag != 0)
                {
                  *(buf + pos) = '\0';
                  ++pos;
                }
                serialize_size_ += colums_[ROW_KEY_ENTRY[i].index].len_;
              }
              break;
            case DATETIME:
              {
                int64_t val = 1000L * 1000L * transform_date_to_time(colums_[ROW_KEY_ENTRY[i].index].column_);
                ret = serialization::encode_i64(buf,buf_len,pos,val);
                serialize_size_ += 8;
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
        if (first_key_)
        {
          memcpy(last_end_key_buf_,buf,pos);
          if (split_pos_ > 0 && pos > split_pos_)
            memset(last_end_key_buf_ + split_pos_,0xff,pos - split_pos_);
          last_end_key_.assign_ptr(last_end_key_buf_,static_cast<int32_t>(pos));
          first_key_ = false;
        }
      }
      return ret;
    }

    int ObDataBuilder::create_and_append_rowkey(const char *fields,int index,char *buf,int64_t buf_len,int64_t& pos)
    {
      int ret = 0;
      switch(ROW_KEY_ENTRY[index].type)
      {
        case INT8:
          {
            int val = atoi(fields);
            ret = serialization::encode_i8(buf,buf_len,pos,static_cast<int8_t>(val));
          }
          break;
        case INT16:
          {
            int val = atoi(fields);
            ret = serialization::encode_i16(buf,buf_len,pos,static_cast<int16_t>(val));
          }
          break;
        case INT32:
          {
            int32_t val = atoi(fields);
            ret = serialization::encode_i32(buf,buf_len,pos,val);
          }
          break;
        case INT64:
          {
            int64_t val = atol(fields);
            ret = serialization::encode_i64(buf,buf_len,pos,val);
          }
          break;
        case VARCHAR:
          //TODO
          break;
        default:
          break;
      }
      return ret;
    }
  }
}


using namespace oceanbase;
using namespace oceanbase::chunkserver;

void usage(const char *program_name)
{
  printf("Usage: %s  -s schema_file\n"
                    "\t\t-t table_id\n"
                    "\t\t-f data_file\n"
                    "\t\t-d dest_path\n"
                    "\t\t-i disk_no (default is 1)\n"
                    "\t\t-b sstable id will start from this id(default is 1)\n"
                    "\t\t-l delimeter(ascii value)\n"
                    "\t\t-x syntax file\n"
                    "\t\t-r number of fields in raw data\n"
                    "\t\t-z name of compressor library\n"
                    "\t\t-S maximum sstable size(in Bytes, default is %ld)\n"
                    "\t\t-n maximum sstable number for one data file(default is %ld)\n"
                    ,program_name,
                    ObDataBuilder::DEFAULT_MAX_SSTABLE_SIZE,
                    ObDataBuilder::DEFAULT_MAX_SSTABLE_NUMBER);
  exit(1);
}

int main(int argc, char *argv[])
{
  const char *schema_file = NULL;
  int ret = OB_SUCCESS;
  ObDataBuilder::ObDataBuilderArgs args;

  while((ret = getopt(argc,argv,"s:t:f:d:i:b:x:l:r:z:S:n:")) != -1)
  {
    switch(ret)
    {
      case 's':
        schema_file = optarg;
        break;
      case 't':
        args.table_id_ = atoi(optarg);
        break;
      case 'f':
        args.filename_ = optarg;
        break;
      case 'd':
        args.dest_dir_ = optarg;
        break;
      case 'i':
        args.disk_no_ = atoi(optarg);
        break;
      case 'b':
        args.file_no_ = atoi(optarg);
        break;
      case 'x':
        args.syntax_file_ = optarg;
        break;
      case 'l':
        DELIMETER  = static_cast<char>(atoi(optarg));
        break;
      case 'r':
        RAW_DATA_FILED = atoi(optarg);
        break;
      case 'z':
        args.compressor_string_ = optarg;
        break;
      case 'S':
        args.max_sstable_size_ = atol(optarg);
        break;
      case 'n':
        args.reserve_ids_ = static_cast<int32_t>(atol(optarg));
        break;
      default:
        fprintf(stderr,"%s is not identified",optarg);
        break;
    }
  }

  if (NULL == schema_file
      || args.table_id_ <= 1000 
      || NULL == args.filename_ 
      || NULL == args.dest_dir_ 
      || args.disk_no_ < 0 
      || args.file_no_ < 0
      || NULL == args.syntax_file_
      || NULL == args.compressor_string_)
  {
    usage(argv[0]);
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  tbsys::CConfig c1;
  ObSchemaManagerV2  *mm = new (std::nothrow)ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  if ( !mm->parse_from_file(schema_file, c1) )
  {
    TBSYS_LOG(ERROR,"parse schema failed.\n");
    exit(1);
  }

  const ObTableSchema *table_schema = mm->get_table_schema(args.table_id_);

  if (NULL == table_schema)
  {
    TBSYS_LOG(ERROR,"table schema is null");
    exit(1);
  }
  args.schema_ = mm;

  ObDataBuilder *data_builder = new ObDataBuilder();

  fprintf(stderr,"sizeof databuilder : %lu\n",sizeof(ObDataBuilder));
 
  if (NULL == data_builder)
  {
    TBSYS_LOG(ERROR,"new databulder failed");
    exit(1);
  }
  
  if (data_builder->init(args) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR,"init failed");
  }
  else
  {
    int ret = data_builder->start_builder();
    if (OB_SUCCESS != ret)
    {
      exit(ret);
    }
  }

  if (mm != NULL)
  {
    delete mm;
    mm = NULL;
  }

  if (data_builder != NULL)
  {
    delete data_builder;
    data_builder = NULL;
  }
  return 0;
}
