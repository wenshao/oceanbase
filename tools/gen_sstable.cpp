/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 * gen_sstable.cpp is for what ...
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *   huating <huating.zmq@taobao.com>
 *
 */
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
#include <tbsys.h>
#include <map>
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_schema.h"
#include "common/ob_crc64.h"
#include "gen_sstable.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_disk_path.h"
#include "common/utility.h"
#include <getopt.h>
#include <vector>

const char* g_sstable_directory = NULL;
namespace oceanbase 
{
  namespace chunkserver
  {
    using namespace std;
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;
    const int64_t MICRO_PER_SEC = 1000000;
    const int64_t FIR_MULTI = 256;
    const int64_t SEC_MULTI = 100;
    const int32_t ARR_SIZE = 62;
    const int32_t JOIN_DIFF = 9;
    const int64_t crtime=1289915563;

    static char item_type_[62] = {'0','1','2','3','4','5','6','7','8','9',
                                  'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
                                  'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

  
    int fill_sstable_schema(const ObSchemaManagerV2& schema,uint64_t table_id,ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      ObSSTableSchemaColumnDef column_def;
      //memset(&sstable_schema,0,sizeof(sstable_schema));
      
      int cols = 0;
      int32_t size = 0;

      const ObColumnSchemaV2 *col = schema.get_table_schema(table_id,size);

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
          column_def.column_group_id_ = static_cast<int16_t>((col + col_index)->get_column_group_id());
          column_def.column_name_id_ = static_cast<int16_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
          ret = sstable_schema.add_column_def(column_def);
          ++cols;
        }
      }

      if ( 0 == cols ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      return ret;
    }

    ObGenSSTable::ObGenSSTable():
      file_no_(-1),
      reserve_ids_(30),
      disk_no_(1),
      dest_dir_(NULL),
      gen_join_table_(false),
      block_size_(0),
      row_key_prefix_(0),
      row_key_suffix_(0),
      step_length_(1),
      max_rows_(0),
      max_sstable_num_(1),
      schema_mgr_(NULL),
      table_schema_(NULL),
      current_sstable_id_(SSTABLE_ID_BASE),
      curr_uid_(0),
      curr_tid_(0),
      curr_itype_(0),
      config_file_(NULL),
      comp_name_(NULL)
    {
      memset(&table_id_list_,0,sizeof(table_id_list_));
    }

    ObGenSSTable::~ObGenSSTable()
    {
    }

    void ObGenSSTable::init(ObGenSSTableArgs& args)
    {
      config_file_ = args.config_file_;
      file_no_ = args.file_no_;
      reserve_ids_ = args.reserve_ids_;
      disk_no_ = args.disk_no_;

      memcpy(table_id_list_,args.table_id_list_,sizeof(args.table_id_list_));
      row_key_prefix_ = args.seed_ - 1;
      row_key_suffix_ = args.suffix_ - 1;
      step_length_ = args.step_length_;
      max_rows_ = args.max_rows_;
      max_sstable_num_ = args.max_sstable_num_;
      set_min_ = args.set_min_;
      set_max_ = args.set_max_;
      gen_join_table_ = args.gen_join_table_;
      data_type_ = args.data_type_;
      block_size_ = args.block_size_;
      curr_uid_ = args.c_uid_;
      current_sstable_id_ = SSTABLE_ID_BASE;

      schema_mgr_ = args.schema_mgr_;
      dest_dir_ = args.dest_dir_;

      comp_name_ = args.comp_name_;
      if (NULL == comp_name_)
      {
        comp_name_ = "lzo_1.0";
      }
      strncpy(compressor_name_,comp_name_,strlen(comp_name_)); 
      compressor_name_[strlen(comp_name_)] = '\0';
      compressor_string_.assign(compressor_name_,static_cast<int32_t>(strlen(compressor_name_)));

    }

    int ObGenSSTable::start_builder()
    {
      int ret = OB_SUCCESS;
      bool is_join_table = false;

      tablet_image_.set_data_version(1);

      for(int table=0; table_id_list_[table] != 0; ++table)
      {
        TBSYS_LOG(INFO,"start build data of table[%d]",table_id_list_[table]);

        TableGen gen(*this);
        if (gen_join_table_ && 1 == table)
        {
          is_join_table = true;
        }

        if ((ret = gen.init(table_id_list_[table], is_join_table)) != OB_SUCCESS )
        {
          TBSYS_LOG(ERROR,"init tablegen failed:[%d]",ret);
          continue; //ignore this table
        }

        if ( (ret = gen.generate()) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"generate data failed:[%d]",ret);
          continue; 
        }
      }

      snprintf(dest_file_,sizeof(dest_file_),"%s/idx_1_%d",dest_dir_,disk_no_);
      snprintf(dest_path_, sizeof(dest_path_), "%s", dest_dir_);
      g_sstable_directory = dest_path_;

      if (OB_SUCCESS == ret && (ret = tablet_image_.write(dest_file_, disk_no_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"write meta file failed: [%d]",ret);
      }
      return ret;
    }

    int64_t ObGenSSTable::get_sstable_id()
    {
      if (SSTABLE_ID_BASE == current_sstable_id_)
      {
        current_sstable_id_ += (file_no_ * reserve_ids_) + 1;
      }
      else 
      {
        ++current_sstable_id_;
      }
      return current_sstable_id_;
    }
        
    const common::ObString& ObGenSSTable::get_compressor() const
    {
      return compressor_string_;
    }

    /*-----------------------------------------------------------------------------
     *  ObGenSSTable::TableGen
     *-----------------------------------------------------------------------------*/
    
    ObGenSSTable::TableGen::TableGen(ObGenSSTable& gen_sstable) : 
                                                    data_type_(1),
                                                    row_key_prefix_(-1),
                                                    row_key_suffix_(-1),
                                                    step_length_(0),
                                                    max_rows_(1),
                                                    max_sstable_num_(1),
                                                    set_min_(false),
                                                    set_max_(false),
                                                    is_join_table_(false),
                                                    gen_sstable_(gen_sstable),
                                                    table_schema_(NULL),
                                                    first_key_(true),
                                                    inited_(false),
                                                    table_id_(0),
                                                    total_rows_(0),
                                                    disk_no_(1),
                                                    row_key_cmp_size_(0),
                                                    current_sstable_size_(0),
                                                    tablet_image_(gen_sstable.tablet_image_),
                                                    curr_uid_(0),
                                                    curr_tid_(0),
                                                    curr_itype_(0),
                                                    config_file_(NULL),
                                                    comp_name_(NULL)
    {}

    int ObGenSSTable::TableGen::init(uint64_t table_id, const bool is_join_table)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        table_id_ = table_id;
        config_file_    = gen_sstable_.config_file_;
        dest_dir_       = gen_sstable_.dest_dir_;
        disk_no_        = gen_sstable_.disk_no_; 
        comp_name_      = gen_sstable_.comp_name_;

        if (NULL == config_file_)
        {
          row_key_suffix_ = gen_sstable_.row_key_suffix_;
          step_length_    = gen_sstable_.step_length_;
          set_min_        = gen_sstable_.set_min_;
          set_max_        = gen_sstable_.set_max_;
          is_join_table_  = is_join_table;
          disk_no_        = gen_sstable_.disk_no_;
          data_type_      = gen_sstable_.data_type_;
          max_sstable_num_ = gen_sstable_.max_sstable_num_;
          if (is_join_table_)
          {
            //join table only has 1/10 rows count of wide table
            max_rows_     = gen_sstable_.max_rows_ / ITEMS_PER_USER;
            row_key_prefix_ = (disk_no_ - 1) * 
              (gen_sstable_.max_rows_ * max_sstable_num_ / ITEMS_PER_USER) - 1;
          }
          else
          {
            max_rows_     = gen_sstable_.max_rows_;
            row_key_prefix_ = (disk_no_ - 1) * 
              (gen_sstable_.max_rows_ * max_sstable_num_ / ITEMS_PER_USER) - 1;
          }
        }
        else
        {
          char table_name[100];
          snprintf(table_name,100,"%ld",table_id);
          //TBSYS_LOG(INFO,"%s",table_name);
          //TBSYS_LOG(INFO,"%s",TBSYS_CONFIG.getString(table_name,"data_type",0));

          row_key_prefix_   = TBSYS_CONFIG.getInt(table_name,"rowkey_prefix",0) - 1;
          row_key_suffix_   = TBSYS_CONFIG.getInt(table_name,"rowkey_suffix",0) - 1;
          step_length_      = TBSYS_CONFIG.getInt(table_name,"step",1); 

          set_min_       = false;
          set_max_       = false;
          int smax       = TBSYS_CONFIG.getInt(table_name,"set_max",1);
          int smin       = TBSYS_CONFIG.getInt(table_name,"set_min",1);
          set_max_       = 1 == smax ? true : false;
          set_min_       = 1 == smin ? true : false;

          data_type_      = TBSYS_CONFIG.getInt(table_name,"data_type",1);
          curr_uid_       = TBSYS_CONFIG.getInt(table_name,"start_uid",0);
          curr_tid_       = TBSYS_CONFIG.getInt(table_name,"start_tid",0);
          curr_itype_     = TBSYS_CONFIG.getInt(table_name,"start_type",0);
          max_rows_       = TBSYS_CONFIG.getInt(table_name,"max_rows",1);
          max_sstable_num_ = TBSYS_CONFIG.getInt(table_name,"max_sstable",10);

        }

        schema_mgr_ = gen_sstable_.schema_mgr_;
        table_schema_ = gen_sstable_.schema_mgr_->get_table_schema(table_id);
        if (NULL == table_schema_)
        {
          TBSYS_LOG(ERROR,"can't find the schema of table [%lu]", table_id);
          ret = OB_ERROR;
        }
        else
        { 
          row_key_cmp_size_ = table_schema_->get_split_pos();
        }
        
        if ( OB_SUCCESS == ret && fill_sstable_schema(*gen_sstable_.schema_mgr_,table_schema_->get_table_id(),schema_) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"convert schema failed");
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          inited_  = true;
        }
        
        func_table_[0] = &ObGenSSTable::TableGen::fill_row_common;
        func_table_[1] = &ObGenSSTable::TableGen::fill_row_chunk;
        func_table_[2] = &ObGenSSTable::TableGen::fill_row_with_aux_column;
        //func_table_[5] = &ObGenSSTable::TableGen::fill_row_item;
        func_table_[6] = &ObGenSSTable::TableGen::fill_row_common_item;
        //func_table_[7] = &ObGenSSTable::TableGen::fill_row_consistency_test;
        //func_table_[8] = &ObGenSSTable::TableGen::fill_row_consistency_test_item;
      }
      return ret;
    }

    int ObGenSSTable::TableGen::deal_first_range()
    {
      int ret = OB_SUCCESS;
      if (first_key_)
      {
        int64_t pos = 0;
        serialization::encode_i64(start_key_,sizeof(start_key_),pos,
                                  row_key_prefix_-1 < 0 ? 0 : row_key_prefix_ - 1);
        if (!is_join_table_)
        {
          serialization::encode_i64(start_key_,sizeof(start_key_),pos,
                                    row_key_suffix_-1 < 0 ? 0 : row_key_suffix_ - 1);
        }
        if (0 == row_key_prefix_)
          memset(start_key_ + row_key_cmp_size_,0, pos - row_key_cmp_size_);
        else
          memset(start_key_ + row_key_cmp_size_,0xFF,pos - row_key_cmp_size_);

        last_end_key_.assign_ptr(start_key_,static_cast<int32_t>(pos));
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }
      return ret;
    }


    int ObGenSSTable::TableGen::generate()
    {
      int ret = OB_SUCCESS;
      int64_t prev_row_key_prefix = 0;
      int64_t prev_row_key_suffix = 0;

      if ((ret = create_new_sstable(table_id_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create sstable failed.");
      }

      uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
      int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

      if ( (ret = schema_mgr_->get_column_groups(table_id_,column_group_ids,column_group_num)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
      }

      for(int32_t sstable_num = 0; sstable_num < max_sstable_num_; ++sstable_num)
      {
        if (2 == data_type_)
        {
          prev_row_key_prefix = row_key_prefix_;
          prev_row_key_suffix = row_key_suffix_;
        }
        for(int32_t group_index=0; group_index < column_group_num; ++group_index)
        {
          if (2 == data_type_)
          {
            srandom(static_cast<int32_t>(prev_row_key_prefix));
            row_key_prefix_ = prev_row_key_prefix;
            row_key_suffix_ = prev_row_key_suffix;
          }
          TBSYS_LOG(DEBUG,"add column group %lu",column_group_ids[group_index]);
          for(int i=0;(i < max_rows_) && (OB_SUCCESS == ret); ++i)
          {
            if ( (ret = (this->*(func_table_[data_type_]))( column_group_ids[group_index] ) ) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"fill row error: [%d]",ret);
            }

            if (OB_SUCCESS == ret && 
                (ret = writer_.append_row(sstable_row_,current_sstable_size_)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"append row failed: [%d]",ret);
            }
          }
        }

        bool set_max = sstable_num == max_sstable_num_ - 1 ? set_max_ : false;
        
        if ((ret = finish_sstable(set_max)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"finish_sstable failed: [%d]",ret);
        }
        else if (sstable_num < max_sstable_num_ - 1)
        {
          ret = create_new_sstable(table_id_);
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::create_new_sstable(int64_t table_id)
    {
      int ret = OB_SUCCESS;
      sstable::ObSSTableId sst_id;

      uint64_t real_id = ( gen_sstable_.get_sstable_id() << 8)|((disk_no_ & 0xff));

      sst_id.sstable_file_id_ = real_id;
      sst_id.sstable_file_offset_ = 0;

      //ret = get_sstable_path(sst_id, dest_file_, sizeof(dest_file_));
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR,"get sstable file path failed");
      }
      else
      {
        snprintf(dest_file_,sizeof(dest_file_),"%s/%ld",dest_dir_,real_id);
        dest_file_string_.assign(dest_file_,static_cast<int32_t>(strlen(dest_file_)));
  
        sstable_id_.sstable_file_id_ = real_id;
        sstable_id_.sstable_file_offset_ = 0;
  
        range_.table_id_ = table_id;
        range_.start_key_ = last_end_key_;
  
        if ((ret = writer_.create_sstable(schema_,dest_file_string_,gen_sstable_.get_compressor(),
                0, OB_SSTABLE_STORE_DENSE, gen_sstable_.block_size_)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"create sstable failed");
        }
      }

      return ret;
    }

    int ObGenSSTable::TableGen::finish_sstable(bool set_max /*=false*/)
    {
      int64_t t = 0;
      int64_t size = 0;
      int ret = OB_SUCCESS;
      //close this sstable 
      if ((ret = writer_.close_sstable(t,size)) != OB_SUCCESS || size < 0 )
      {
        TBSYS_LOG(ERROR,"close_sstable failed,offset %ld,ret: [%d]\n",t,ret);
      }

      if (size > 0)
      {
        //update end key
        range_.border_flag_.unset_inclusive_start();
        range_.border_flag_.set_inclusive_end();
        range_.end_key_ = row_key_;

        if (table_schema_->get_split_pos() != 0)
        {
          if (range_.end_key_.length() - row_key_cmp_size_ > 0)
            memset(range_.end_key_.ptr() + row_key_cmp_size_,0xff,range_.end_key_.length() - row_key_cmp_size_);
        }
        
        if (set_min_)
        {
          TBSYS_LOG(INFO,"set_min");
          range_.border_flag_.set_min_value();
          set_min_ = false;
        }
        else
        {
          range_.border_flag_.unset_min_value();
        }

        if (set_max)
        {
          TBSYS_LOG(INFO,"set_max");
          range_.border_flag_.set_max_value();
        }
        else
        {
          range_.border_flag_.unset_max_value();
        }

        //load the tablet
        ObTablet *tablet = NULL;
#ifdef GEN_SSTABLE_DEBUG
        range_.dump();
        range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
#endif
        if ((ret = tablet_image_.alloc_tablet_object(range_,tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "alloc_tablet_object failed.\n");
        }
        else
        {
          tablet->add_sstable_by_id(sstable_id_);
          tablet->set_data_version(1);
          tablet->set_disk_no(disk_no_);
        }

        if (OB_SUCCESS == ret && (ret = tablet_image_.add_tablet(tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"add table failed,ret:[%d]",ret);
        }
        
        //prepare the start key for the next sstable
        memcpy(start_key_,range_.end_key_.ptr(),range_.end_key_.length());
        last_end_key_.assign_ptr(start_key_,range_.end_key_.length());

      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_common(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      //const common::ObColumnSchemaV2* end = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();

      if ((ret = create_rowkey_common()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }
      else
      {
        ret = deal_first_range();
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(group_id);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),group_id,size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = row_key_prefix_ * column->get_id() * column->get_type();
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld],",column->get_id(),column_value);
#endif
              break;
            case ObFloatType:
              float_val = static_cast<float>(column_value);
              obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [float],value [%f],",column->get_id(),float_val);
#endif
              break;
            case ObDoubleType:
              double_val = static_cast<double>(column_value);
              obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [double],value [%f],",column->get_id(),double_val);
#endif

              break;
            case ObDateTimeType:
              obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [datetime],value [%ld],",column->get_id(),column_value);
#endif

              break;
            case ObPreciseDateTimeType:
              obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [pdatetime],value [%ld],",column->get_id(),column_value);
#endif
              break;
            case ObModifyTimeType:
              obj.set_modifytime(labs(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [cdatetime],value [%ld],",column->get_id(),labs(column_value));
#endif
              break;
            case ObCreateTimeType:
              obj.set_createtime(labs(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [mdatetime],value [%ld],",column->get_id(),labs(column_value));
#endif
              break;
            case ObVarcharType:
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
              str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
              obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [varchar:%d],",column->get_id(),obj.get_type());
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_common_item(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();

      if ((ret = create_rowkey_common_item()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }
      else
      {
        ret = deal_first_range();
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),group_id,size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = row_key_prefix_ * column->get_id() * column->get_type();
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld],",column->get_id(),column_value);
#endif
              break;
            case ObFloatType:
              float_val = static_cast<float>(column_value);
              obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [float],value [%f],",column->get_id(),float_val);
#endif
              break;
            case ObDoubleType:
              double_val = static_cast<double>(column_value);
              obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [double],value [%f],",column->get_id(),double_val);
#endif

              break;
            case ObDateTimeType:
              obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [datetime],value [%ld],",column->get_id(),column_value);
#endif

              break;
            case ObPreciseDateTimeType:
              obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [pdatetime],value [%ld],",column->get_id(),column_value);
#endif
              break;
            case ObModifyTimeType:
              obj.set_modifytime(labs(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [cdatetime],value [%ld],",column->get_id(),labs(column_value));
#endif
              break;
            case ObCreateTimeType:
              obj.set_createtime(labs(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [mdatetime],value [%ld],",column->get_id(),labs(column_value));
#endif
              break;
            case ObVarcharType:
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
              str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
              obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [varchar:%d],",column->get_id(),obj.get_type());
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }
    
    int ObGenSSTable::TableGen::fill_row_with_aux_column(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      const common::ObColumnSchemaV2* column_end = NULL; 
      int64_t column_value = 0;
      int64_t hash_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      bool aux_column = false;
      MurmurHash2 mur_hash;
      ObObj obj;
      ObString str;

      std::map<uint64_t,int64_t> aux_value;
      std::map<uint64_t,float> aux_float_value;
      std::map<uint64_t,double> aux_double_value;
      sstable_row_.clear();

      static bool special_key = false;
      static int id = 1;

      if ((ret = create_rowkey_aux()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }
      else
      {
        ret = deal_first_range();
      }

      if (!is_join_table_ && 0 == row_key_prefix_ && 0 == row_key_suffix_)
      {
        special_key = true;
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = sstable_row_.set_table_id(table_id_);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR,"set row table id failed: [%d]",ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = sstable_row_.set_column_group_id(group_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR,"set row column group id failed: [%d]",ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),
                                                            group_id, size);
        column_end = column + size;
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          if ( 0 == strncmp(column->get_name(),"aux_",4) )
          {
            aux_column = true;
          }
          else
          {
            aux_column = false;
          }

          obj.reset();
          if (special_key && id <= 2)
          {
            column_value = max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM; //assume there are 10 disks
          }
          else if (!aux_column)
          {
            column_value = row_key_prefix_ * column->get_id() * column->get_type() * 991;
          }

          if (!aux_column || (special_key && id <= 2))
          {
            if (special_key && id <= 2)
            {
              ++id;
              if (3 == id)
              {
                special_key = false;
              }
            }

            switch(column->get_type())
            {
              case ObIntType:
                obj.set_int(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld]",column->get_id(),column_value);
#endif
                break;
              case ObFloatType:
                float_val = static_cast<float>(column_value);
                obj.set_float(float_val);
                aux_float_value.insert(make_pair(column->get_id(), float_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [float],value [%f]",column->get_id(),float_val);
#endif
                break;
              case ObDoubleType:
                double_val = static_cast<double>(column_value);
                obj.set_double(double_val);
                aux_double_value.insert(make_pair(column->get_id(),double_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [double],value [%f]",column->get_id(),double_val);
#endif
                break;
              case ObDateTimeType:
                obj.set_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [datetime],value [%ld]",column->get_id(),column_value);
#endif
                break;
              case ObPreciseDateTimeType:
                obj.set_precise_datetime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [pdatetime],value [%ld]",column->get_id(),column_value);
#endif
                break;
              case ObModifyTimeType:
                obj.set_modifytime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [cdatetime],value [%ld]",column->get_id(),column_value);
#endif
                break;
              case ObCreateTimeType:
                obj.set_createtime(column_value);
                aux_value.insert(make_pair(column->get_id(),0 - column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [mdatetime],value [%ld]",column->get_id(),column_value);
#endif
                break;
              case ObVarcharType:
                snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
                hash_value = mur_hash(varchar_buf_);
                str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
                obj.set_varchar(str);
                aux_value.insert(make_pair(column->get_id(),0 - hash_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [varchar:%d],value [%s]",column->get_id(),obj.get_type(),varchar_buf_);
#endif
                break;
              default:
                TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
                ret = OB_ERROR;
                break;
            }
          }
          else
          {
            switch(column->get_type())
            {
              case ObIntType:
                {
                  map<uint64_t,int64_t>::iterator it = aux_value.find(column->get_id() - 1);
                  if (it != aux_value.end())
                  {
                    obj.set_int(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld]",column->get_id(),it->second);
#endif
                }
                break;
              case ObFloatType:
                {
                  map<uint64_t,float>::iterator it = aux_float_value.find(column->get_id() - 1);
                  if (it != aux_float_value.end())
                  {
                    obj.set_float(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                TBSYS_LOG(DEBUG,"[%d]type [float],value [%f]",column->get_id(),it->second);
#endif
                }
                break;
              case ObDoubleType:
                {
                  map<uint64_t,double>::iterator it = aux_double_value.find(column->get_id() - 1);
                  if (it != aux_double_value.end())
                  {
                    obj.set_double(it->second);
                  }
#ifdef GEN_SSTABLE_DEBUG
                TBSYS_LOG(DEBUG,"[%d]type [double],value [%f]",column->get_id(),it->second);
#endif
                }
                break;
              default:
                TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
                ret = OB_ERROR;
                break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }
    
    int ObGenSSTable::TableGen::fill_row_chunk(uint64_t group_id)
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      int64_t hash_value = 0;
      int64_t rowkey_hash_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      MurmurHash2 mur_hash;
      ObObj obj;
      ObString str;
      sstable_row_.clear();

      if ((ret = create_rowkey_chunk()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }
      else
      {
        ret = deal_first_range();
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_group_schema(table_schema_->get_table_id(),group_id,size);
        
        for(int32_t i=0; i < size - 1 && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = row_key_prefix_ * column->get_id() * column->get_type() * 991;
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObFloatType:
              float_val = static_cast<float>(column_value);
              obj.set_float(float_val);
              hash_value += mur_hash((void *)&float_val,sizeof(float_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [float],value [%f],hash [%ld]",column->get_id(),float_val,hash_value);
#endif
              break;
            case ObDoubleType:
              double_val = static_cast<double>(column_value);
              obj.set_double(double_val);
              hash_value += mur_hash((void *)&double_val,sizeof(double_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [double],value [%f],hash [%ld]",column->get_id(),double_val,hash_value);
#endif

              break;
            case ObDateTimeType:
              obj.set_datetime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [datetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif

              break;
            case ObPreciseDateTimeType:
              obj.set_precise_datetime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [pdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObModifyTimeType:
              obj.set_modifytime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [cdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObCreateTimeType:
              obj.set_createtime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [mdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObVarcharType:
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
              hash_value += mur_hash(varchar_buf_);
              str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
              obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [varchar:%d],hash [%ld]",column->get_id(),obj.get_type(),hash_value);
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }

        if ( OB_SUCCESS == ret)
        {
          //the last column
          if (column->get_type() != ObIntType)
          {
            TBSYS_LOG(ERROR,"this schema is illigal");
            ret = OB_ERROR;
          }
          else
          {
            rowkey_hash_value = mur_hash(row_key_.ptr(),row_key_.length());
            obj.reset();
            obj.set_int(rowkey_hash_value - hash_value);
#ifdef GEN_SSTABLE_DEBUG
            TBSYS_LOG(DEBUG,"last col type [int],value [%ld],rowkey_hash_value:[%ld]",rowkey_hash_value - hash_value, rowkey_hash_value);
#endif
            sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_null()
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      int64_t hash_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      MurmurHash2 mur_hash;
      ObObj obj;
      ObString str;
      sstable_row_.clear();

      if ((ret = create_rowkey_null()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_table_schema(table_schema_->get_table_id(),size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = row_key_prefix_ * column->get_id() * column->get_type() * 991;
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [int],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObFloatType:
              float_val = static_cast<float>(column_value);
              obj.set_float(float_val);
              hash_value += mur_hash((void *)&float_val,sizeof(float_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [float],value [%f],hash [%ld]",column->get_id(),float_val,hash_value);
#endif
              break;
            case ObDoubleType:
              double_val = static_cast<double>(column_value);
              obj.set_double(double_val);
              hash_value += mur_hash((void *)&double_val,sizeof(double_val));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [double],value [%f],hash [%ld]",column->get_id(),double_val,hash_value);
#endif

              break;
            case ObDateTimeType:
              obj.set_datetime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [datetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif

              break;
            case ObPreciseDateTimeType:
              obj.set_precise_datetime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [pdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObModifyTimeType:
              obj.set_modifytime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [cdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObCreateTimeType:
              obj.set_createtime(column_value);
              hash_value += mur_hash((void *)&column_value,sizeof(column_value));
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [mdatetime],value [%ld],hash [%ld]",column->get_id(),column_value,hash_value);
#endif
              break;
            case ObVarcharType:
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%ld",column_value);
              hash_value += mur_hash(varchar_buf_);
              str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
              obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"[%d]type [varchar:%d],hash [%ld]",column->get_id(),obj.get_type(),hash_value);
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_item()
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      float float_val = 0.0;
      double double_val = 0.0;
      ObObj obj;
      ObString str;
      sstable_row_.clear();
      if ((ret = create_rowkey_item()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_table_schema(table_schema_->get_table_id(),size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = (curr_uid_ * FIR_MULTI + curr_tid_) * SEC_MULTI + column->get_id();
          switch(column->get_type())
          {
            case ObIntType:
              column_value = curr_tid_ * SEC_MULTI + column->get_id();
              obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            case ObFloatType:
              float_val = static_cast<float>(curr_tid_ * SEC_MULTI + column->get_id());
              obj.set_float(float_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%f | ",float_val);
#endif
              break;
            case ObDoubleType:
              double_val = static_cast<double>(curr_tid_ * SEC_MULTI + column->get_id());
              obj.set_double(double_val);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%f | ",double_val);
#endif

              break;
            case ObDateTimeType:
              column_value = crtime - curr_tid_ - column->get_id();
              obj.set_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            case ObPreciseDateTimeType:
              column_value = (crtime - curr_tid_ - column->get_id())*MICRO_PER_SEC;
              obj.set_precise_datetime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            case ObModifyTimeType:
              column_value = (crtime - curr_tid_ - column->get_id())*MICRO_PER_SEC;
              obj.set_modifytime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            case ObCreateTimeType:
              column_value = (crtime - curr_tid_ - column->get_id())*MICRO_PER_SEC;
              obj.set_createtime(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            case ObVarcharType:
              snprintf(varchar_buf_,sizeof(varchar_buf_),"%08ld-%ld",curr_tid_,column->get_id());
              str.assign_ptr(varchar_buf_,static_cast<int32_t>(strlen(varchar_buf_))); //todo len
              obj.set_varchar(str);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%s | ",varchar_buf_);
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
        //change tid uid itype
        curr_tid_++;
        curr_itype_++;
        if ( ARR_SIZE*SEC_MULTI == curr_tid_ )
        {
          curr_tid_ = 0;
          curr_itype_ = 0;
          curr_uid_++;
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_consistency_test()
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      ObObj obj;
      sstable_row_.clear();
      if ((ret = create_rowkey_consistency_test()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_table_schema(table_schema_->get_table_id(),size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = (row_key_suffix_ << 24 | column->get_id() << 16 | 1);
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::fill_row_consistency_test_item()
    {
      int ret = OB_SUCCESS;
      const common::ObColumnSchemaV2* column = NULL;
      int64_t column_value = 0;
      ObObj obj;
      sstable_row_.clear();
      if ((ret = create_rowkey_consistency_test_item()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create rowkey failed: [%d]",ret);
      }

      if ((OB_SUCCESS == ret) && (ret = sstable_row_.set_row_key(row_key_)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"set row key failed: [%d]",ret);
      }
      else
      {
        sstable_row_.set_table_id(table_id_);
        sstable_row_.set_column_group_id(0);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t size = 0;
        column = gen_sstable_.schema_mgr_->get_table_schema(table_schema_->get_table_id(),size);
        
        for(int32_t i=0; i < size && (OB_SUCCESS == ret);++i,++column)
        {
          obj.reset();
          column_value = (row_key_suffix_ << 24 | column->get_id() << 16 | 1);
          switch(column->get_type())
          {
            case ObIntType:
              obj.set_int(column_value);
#ifdef GEN_SSTABLE_DEBUG
              TBSYS_LOG(DEBUG,"%ld | ",column_value);
#endif
              break;
            default:
              TBSYS_LOG(ERROR,"unexpect type : %d",column->get_type());
              ret = OB_ERROR;
              break;
          }

          if (OB_SUCCESS == ret)
          {
            ret = sstable_row_.add_obj(obj);
          }
        }
      }
      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_item()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      sprintf(row_key_buf_,"%c%08ld",item_type_[(curr_itype_/SEC_MULTI)%ARR_SIZE],curr_tid_);
      row_key_.assign(row_key_buf_,static_cast<int32_t>(strlen(row_key_buf_)));
      pos = strlen(row_key_buf_);
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"tid is %ld  itype is %d\n",curr_tid_,curr_itype_);
      TBSYS_LOG(DEBUG,"row_key_buf_ is %s\n",row_key_buf_);
      common::hex_dump(row_key_buf_,pos,false);
#endif
      if (first_key_)
      {
        sprintf(start_key_,"%c%08d",'0',0);
        last_end_key_.assign_ptr(start_key_,static_cast<int32_t>(pos));
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }
      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_common_item()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
     // ++row_key_prefix_;
      ++row_key_suffix_;

      serialization::encode_i8(row_key_buf_,sizeof(row_key_buf_),pos,0);
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_suffix_);

      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,suffix:%ld",row_key_prefix_,row_key_suffix_);
      common::hex_dump(row_key_buf_,pos,false);
#endif
      if (first_key_)
      {
        int64_t pos = 0;
        serialization::encode_i8(start_key_,sizeof(start_key_),pos,0);
        serialization::encode_i64(start_key_,sizeof(start_key_),pos,
                                  row_key_suffix_-1 < 0 ? 0 : row_key_suffix_- 1);

        last_end_key_.assign_ptr(start_key_,table_schema_->get_rowkey_max_length());
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }
      return ret;
    }


    int ObGenSSTable::TableGen::create_rowkey_common()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ++row_key_prefix_;
      ++row_key_suffix_;

      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_prefix_);
      serialization::encode_i8(row_key_buf_,sizeof(row_key_buf_),pos,0);
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_suffix_);

      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,suffix:%ld",row_key_prefix_,row_key_suffix_);
      common::hex_dump(row_key_buf_,pos,false);
#endif

      if (first_key_)
      {
        int64_t pos = 0;
        serialization::encode_i64(start_key_,sizeof(start_key_),pos,
                                  row_key_prefix_-1 < 0 ? 0 : row_key_prefix_ - 1);
        if (0 == row_key_prefix_)
          memset(start_key_ + row_key_cmp_size_,0, table_schema_->get_rowkey_max_length() - row_key_cmp_size_);
        else
          memset(start_key_ + row_key_cmp_size_,0xFF,table_schema_->get_rowkey_max_length() - row_key_cmp_size_);

        last_end_key_.assign_ptr(start_key_,table_schema_->get_rowkey_max_length());
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }

      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_consistency_test()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      static int64_t  member = -1;
      //++row_key_prefix_;
      ++row_key_suffix_;

      if (-1 == member)
      {
        ++row_key_prefix_;
        ++row_key_suffix_;
      }

      if (++member >= 10000)
      {
        ++row_key_prefix_;
        row_key_suffix_ = 0;
        member = 0;
      }
      int64_t suffix = row_key_suffix_;

      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_prefix_);
      serialization::encode_i8(row_key_buf_,sizeof(row_key_buf_),pos,0);
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,suffix);

      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,suffix:%ld",row_key_prefix_,suffix);
      common::hex_dump(row_key_buf_,pos,false);
#endif

      if (first_key_)
      {
        int64_t pos = 0;
        serialization::encode_i64(start_key_,sizeof(start_key_),pos,
            row_key_prefix_-1 < 0 ? 0 : row_key_prefix_ - 1);
        if (0 == row_key_prefix_)
          memset(start_key_ + row_key_cmp_size_,0, table_schema_->get_rowkey_max_length() - row_key_cmp_size_);
        else
          memset(start_key_ + row_key_cmp_size_,0xFF,table_schema_->get_rowkey_max_length() - row_key_cmp_size_);

        last_end_key_.assign_ptr(start_key_,table_schema_->get_rowkey_max_length());
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }

      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_consistency_test_item()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
     // ++row_key_prefix_;
      ++row_key_suffix_;

      //serialization::encode_i8(row_key_buf_,sizeof(row_key_buf_),pos,0);
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_suffix_);

      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,suffix:%ld",row_key_prefix_,row_key_suffix_);
      common::hex_dump(row_key_buf_,pos,false);
#endif
      if (first_key_)
      {
        int64_t pos = 0;
        //serialization::encode_i8(start_key_,sizeof(start_key_),pos,0);
        serialization::encode_i64(start_key_,sizeof(start_key_),pos,
                                  row_key_suffix_-1 < 0 ? 0 : row_key_suffix_- 1);

        last_end_key_.assign_ptr(start_key_,table_schema_->get_rowkey_max_length());
        range_.start_key_ = last_end_key_;
        first_key_ = false;
      }
      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_chunk()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ++row_key_prefix_;
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_prefix_);
      int64_t postfix = (row_key_prefix_ * 12911) % 15485863; 
      char postfix_buf[20];
      snprintf(postfix_buf,sizeof(postfix_buf),"%ld",postfix);
      memcpy(row_key_buf_+pos, postfix_buf,strlen(postfix_buf));
      pos += strlen(postfix_buf);
      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,postfix:%ld",row_key_prefix_,postfix);
      common::hex_dump(row_key_buf_,pos,false);
#endif
      return ret;
    }
   
    int ObGenSSTable::TableGen::create_rowkey_aux()
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t suffix = 0;
      static int64_t row_count = 0;

      if (is_join_table_ || (!is_join_table_ && row_count % ITEMS_PER_USER == 0))
      {
        ++row_key_prefix_;
      }
      serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,row_key_prefix_);
      if (!is_join_table_)
      {
        ++row_key_suffix_;

        if (row_key_prefix_ == 0 && row_key_suffix_ == 0)
        {
          suffix = 0;
          row_count++;
        }
        else
        {
          /**
           * each perfix combine with  10 suffix
           */
          suffix = random() % ((max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM) / ITEMS_PER_USER) 
                   + (row_count++ % ITEMS_PER_USER) * ((max_rows_ * max_sstable_num_ / ITEMS_PER_USER * DISK_NUM) / ITEMS_PER_USER);
        }
        serialization::encode_i64(row_key_buf_,sizeof(row_key_buf_),pos,suffix);
      }
      row_key_.assign(row_key_buf_,static_cast<int32_t>(pos));
#ifdef GEN_SSTABLE_DEBUG
      TBSYS_LOG(DEBUG,"row key, prefix :%ld,suffix:%ld",row_key_prefix_,row_key_suffix_);
      common::hex_dump(row_key_buf_,pos,false);
#endif
      return ret;
    }

    int ObGenSSTable::TableGen::create_rowkey_null()
    {
      int ret = OB_SUCCESS;
      int64_t rowkey_len = table_schema_->get_rowkey_max_length();
      memset(start_key_,0,rowkey_len);
      ++row_key_prefix_;
      ++row_key_suffix_;
      last_end_key_.assign_ptr(start_key_,static_cast<int32_t>(rowkey_len));
      range_.start_key_ = last_end_key_;
      memset(row_key_buf_,0xff,rowkey_len);
      row_key_.assign(row_key_buf_,static_cast<int32_t>(rowkey_len));
      return ret;
    }
  }
}

void usage(const char *program_name)
{
  printf("Usage: %s\t-s schema_file\n" 
                    "\t\t-t table_id\n"
                    "\t\t-l seed (default is 1)\n"
                    "\t\t-e step length (default is 1)\n"
                    "\t\t-m max rows(default is 1)\n"
                    "\t\t-N max sstable number(default is 1)\n"
                    "\t\t-d dest_path\n"
                    "\t\t-i disk_no (default is 1)\n"
                    "\t\t-b sstable id will start from this id(default is 1)\n"
                    "\t\t-n generate null tablet [meta from min to max]\n"
                    "\t\t-a set min range\n"
                    "\t\t-z set max range\n"
                    "\t\t-j whether create join table data\n"
                    "\t\t-u huating's data format\n"
                    "\t\t-x suffix\n"
                    "\t\t-f config file\n"
                    "\t\t-c compress lib name\n"
                    "\t\t-V version\n",
         program_name);
  exit(0);
}

using namespace std;
using namespace oceanbase;
using namespace oceanbase::chunkserver;

int main(int argc, char *argv[])
{
  ObGenSSTable::ObGenSSTableArgs args;
  const char *schema_file = NULL;
  int ret = 0;
  int32_t table_num = 2;
  int quiet = 0;

  while((ret = getopt(argc,argv,"qs:t:l:e:d:i:b:m:f:nazjhVx:uB:c:N:")) != -1)
  {
    switch(ret)
    {
      case 'q':
        quiet = 1;
        break;
      case 's':
        schema_file = optarg;
        break;
      case 't':
        TBSYS_LOG(INFO,"table array:%s",optarg);
        parse_string_to_int_array(optarg,',',args.table_id_list_,table_num);
        break;
      case 'l':
        args.seed_ = strtoll(optarg, NULL, 10);
        break;
      case 'e':
        args.step_length_ = atoi(optarg);
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
      case 'm':
        args.max_rows_ = atoi(optarg);
        break;
      case 'N':
        args.max_sstable_num_ = atoi(optarg);
        break;
      case 'n':
        args.set_min_ = true;
        args.set_max_ = true;
        args.data_type_ = 3;
        break;
      case 'a':
        args.set_min_ = true;
        break;
      case 'z':
        args.set_max_ = true;
        break;
      case 'j':
        args.gen_join_table_ = true;
        break;
      case 'x':
        args.suffix_ = strtoll(optarg,NULL,10);
        break;
      case 'u':
        args.data_type_ = 2;
        break;
      case 'B':
        args.block_size_ = strtoll(optarg, NULL, 10);
        break;
      case 'f':
        args.config_file_ = optarg;
        break;
      case 'c':
        args.comp_name_ = optarg;
        break;
      case 'h':
        usage(argv[0]);
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        exit(1);
      default:
        fprintf(stderr,"%s is not identified",optarg);
        exit(1);
    };
  }

  if (quiet) TBSYS_LOGGER.setLogLevel("ERROR");

  if (NULL != args.config_file_)
  {
    if (TBSYS_CONFIG.load(args.config_file_))
    {
      fprintf(stderr, "load file %s error\n", args.config_file_);
      return EXIT_FAILURE;
    }

    schema_file = TBSYS_CONFIG.getString("config", "schema", "schema.ini");
    TBSYS_LOG(INFO,"schema is %s",schema_file);

    vector<int> tableids = TBSYS_CONFIG.getIntList("config","table");
    int i = 0;
    for (vector<int>::iterator it = tableids.begin(); it != tableids.end(); ++it)
    {
      args.table_id_list_[i++] = *it;
    }

    if (NULL == schema_file || args.table_id_list_[0] < 1000)
    {
      usage(argv[0]);
    }
    
    args.dest_dir_    = TBSYS_CONFIG.getString("config","dest_dir","data");
    args.file_no_     = TBSYS_CONFIG.getInt("config","sstable_id_base",1);
    args.disk_no_     = TBSYS_CONFIG.getInt("config","disk_no",1);
    args.block_size_ = TBSYS_CONFIG.getInt("config","block_size", ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE);
    args.comp_name_ = TBSYS_CONFIG.getString("config", "comp_name", "lzo_1.0");
  }
  else
  {
    if (argc < 6)
    {
      usage(argv[0]);
    }

    if (NULL == schema_file || table_num <= 0 || args.table_id_list_[0] < 1000
        || NULL == args.dest_dir_ || args.disk_no_ < 0
        || args.file_no_ < 0 || args.seed_ < 0 
        || args.step_length_ < 0 || args.max_rows_ < 0 || args.max_sstable_num_ <= 0)
    {
      usage(argv[0]);
    }
    srandom(static_cast<int32_t>(args.seed_));
  }

  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  ob_init_memory_pool();
  tbsys::CConfig c1;
  ObSchemaManagerV2* mm = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
  if ( !mm->parse_from_file(schema_file, c1) )
  {
    TBSYS_LOG(ERROR,"parse schema failed");
    exit(0);
  }
 
  args.schema_mgr_  = mm;

  ObGenSSTable data_builder;
  data_builder.init(args);
  data_builder.start_builder();
  return 0;
}
