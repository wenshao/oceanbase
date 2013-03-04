/*
 * =====================================================================================
 *
 *       Filename:  OceanbaseDb.cpp
 *
 *        Version:  1.0
 *        Created:  04/13/2011 09:55:53 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */

#include "oceanbase_db.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_define.h"
#include "common/serialization.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_malloc.h"
#include "db_record_set.h"
#include "db_table_info.h"
#include "db_record.h"
#include "ob_api_util.h"

#include <string>

const int64_t kMaxLogSize = 256 * 1024 * 1024;  /* log file size 256M */
const int64_t kMaxApplyRetries = 5;
const int kMaxColumns = 50;

using namespace oceanbase::common;

namespace oceanbase {
  using namespace common;
  namespace api {
    RowMutator::RowMutator(const std::string &table_name, const ObString &rowkey, 
                           DbTranscation *tnx) :
      table_name_(table_name), rowkey_(rowkey.ptr(), rowkey.length()), tnx_(tnx)
    {
      op_ = ObActionFlag::OP_UPDATE;
    }

    int RowMutator::add(const char *column_name, const ObObj &val)
    {
      ObMutatorCellInfo mutation;

      mutation.cell_info.table_name_.assign_ptr(const_cast<char *>(table_name_.c_str()), static_cast<int32_t>(table_name_.length()));
      mutation.cell_info.row_key_.assign_ptr(const_cast<char *>(rowkey_.c_str()), static_cast<int32_t>(rowkey_.length()));
      mutation.cell_info.column_name_.assign_ptr(const_cast<char *>(column_name), static_cast<int32_t>(strlen(column_name)));

      mutation.op_type.set_ext(op_);
      mutation.cell_info.value_ = val;
      int ret = tnx_->mutator_.add_cell(mutation);

      return ret;
    }

    int RowMutator::add(const std::string &column_name, const ObObj &val)
    {
      return add(column_name.c_str(), val);
    }

    DbTranscation::DbTranscation(OceanbaseDb *db) : db_(db)
    {
      assert(db_ != NULL);
    }

    int DbTranscation::insert_mutator(const char* table_name, const ObString &rowkey, RowMutator *&mutator)
    {
      int ret = OB_SUCCESS;
      std::string table = table_name;

      mutator = new(std::nothrow) RowMutator(table, rowkey, this);
      if (mutator == NULL)
      {
        TBSYS_LOG(ERROR, "can't allocate memory for RowMutator");
        ret = OB_ERROR;
      }
      else
      {
        mutator->set_op(ObActionFlag::OP_INSERT);
      }

      return ret;
    }

    int DbTranscation::update_mutator(const char* table_name, const ObString &rowkey, RowMutator *&mutator)
    {
      int ret = OB_SUCCESS;
      std::string table = table_name;

      mutator = new(std::nothrow) RowMutator(table, rowkey, this);
      if (mutator == NULL)
      {
        TBSYS_LOG(ERROR, "can't allocate memory for RowMutator");
        ret = OB_ERROR;
      }

      return ret;
    }

    int DbTranscation::free_row_mutator(RowMutator *&mutator)
    {
      delete mutator;
      mutator = NULL;

      return OB_SUCCESS;
    }

    int DbTranscation::commit()
    {
      int ret = OB_SUCCESS;
      ObMemBuf *mbuff = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);

      if (mbuff == NULL || mbuff->ensure_space(OB_MAX_PACKET_LENGTH) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "can't allocate from TSI");
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ObDataBuffer buffer(mbuff->get_buffer(), mbuff->get_buffer_size());

        ObDataBuffer tmp_buffer = buffer;
        ret = mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't deserialize mutator, due to %d", ret);
        }
        else
        {
          int64_t retries = 0;

          while (true) 
          {
            retries++;
            int64_t pos = 0;
            ret = db_->do_server_cmd(db_->update_server_, OB_WRITE, buffer, pos);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "Write to OB error, ret=%d retry_time=%ld",
                  ret, retries);
              sleep(30);

              buffer = tmp_buffer;
              ret = mutator_.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR, "can't deserialize mutator, due to %d", ret);
              }
            }
            else
            {
              break;
            }

            TBSYS_LOG(WARN, "retring write to database, buffer size = %ld", pos);
          }
        }

        if (ret != OB_SUCCESS)
        {
          char server_buf[128];
          db_->update_server_.to_string(server_buf, 128);
          TBSYS_LOG(ERROR, "can't write to update server, due to [%d], server:%s", ret, server_buf);
        }
      }

      return ret;
    }

    int DbTranscation::abort()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int DbTranscation::add_cell(ObMutatorCellInfo &cell)
    {
      return mutator_.add_cell(cell);
    }


    OceanbaseDb::OceanbaseDb(const char *ip, unsigned short port, 
                             int64_t timeout, uint64_t tablet_timeout)
    {            
      inited_ = false;
      root_server_.set_ipv4_addr(ip, port);
      timeout_ = timeout;
      tablet_timeout_ = tablet_timeout;
      TBSYS_LOG(INFO, "%s:%d, timeout is %ld, tablet_timeout is %ld", ip, port, timeout_, tablet_timeout_);
      db_ref_ = 1;
      consistency_ = true;
    }

    OceanbaseDb::~OceanbaseDb()
    {
      transport_.stop();
      transport_.wait();
      free_tablet_cache();

      assert(db_ref_ == 1);
    }

    int OceanbaseDb::get(const std::vector<DbMutiGetRow> &rows, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;

      if (rows.empty()) {
        return ret;
      }

      DbRowKey rowkey = rows[0].rowkey;
      const std::string &table = rows[0].table;

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }
        char server_str[128];
        server.to_string(server_str, 128);

        ret = do_muti_get(server, rows, rs.get_scanner(), rs.get_buffer());
        if (ret == OB_SUCCESS) {
          TBSYS_LOG(DEBUG, "using merger server %s", server_str);
          break;
        } else {
          TBSYS_LOG(WARN, "failed when get data from %s", server_str);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;

    }

    int OceanbaseDb::get(std::string &table,std::vector<std::string> &columns, 
                         const std::vector<DbRowKey> &rowkeys, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;
      DbRowKey rowkey;

      if (rowkeys.empty()) {
        return ret;
      }

      rowkey = rowkeys[0];

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      std::vector<DbMutiGetRow> rows;
      for(size_t i = 0;i < rowkeys.size(); i++) {
        DbMutiGetRow row;
        row.table = table;
        row.rowkey = rowkeys[i];
        row.columns = &columns;

        rows.push_back(row);
      }

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }
        char server_str[128];
        server.to_string(server_str, 128);

        ret = do_muti_get(server, rows, rs.get_scanner(), rs.get_buffer());
        if (ret == OB_SUCCESS) {
          TBSYS_LOG(INFO, "using mergeserver %s", server_str);
          break;
        } else {
          TBSYS_LOG(WARN, "failed when get data from %s", server_str);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;

    }

    int OceanbaseDb::get(std::string &table,std::vector<std::string> &columns, 
                         const DbRowKey &rowkey, DbRecordSet &rs)
    {
      int ret = OB_SUCCESS;
      common::ObServer server;

      if (!rs.inited()) {
        TBSYS_LOG(INFO, "DbRecordSet Not init ,please init it first");
        ret = common::OB_ERROR;
      }

      int retries = kTabletDupNr;

      while (retries--) {
        ret = get_tablet_location(table, rowkey, server);
        if (ret != common::OB_SUCCESS) {
          TBSYS_LOG(ERROR, "No Mergeserver available");
          break;
        }

        ret = do_server_get(server, rowkey, rs.get_scanner(), rs.get_buffer(), table, columns);
        if (ret == OB_SUCCESS) {
          break;
        } else {
          char err_msg[128];

          server.to_string(err_msg, 128);
          TBSYS_LOG(WARN, "failed when get data from %s", err_msg);
          if (ret == OB_ERROR_OUT_OF_RANGE) {
            TBSYS_LOG(ERROR, "rowkey out of range");
            break;
          }

          mark_ms_failure(server, table, rowkey);
        } 
      }

      if (ret == OB_SUCCESS) {
        __sync_add_and_fetch(&db_stats_.total_succ_gets, 1);
      } else {
        __sync_add_and_fetch(&db_stats_.total_fail_gets, 1);
      }

      return ret;
    }

    int OceanbaseDb::init()
    {
      streamer_.setPacketFactory(&packet_factory_);
      transport_.start();
      client_.initialize(&transport_, &streamer_);

      inited_ = true;

      ObServer server;
      int ret = get_update_server(server);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't get update server");
      }

      return ret;
    }

    int OceanbaseDb::global_init(const char *log_dir, const char * level)
    {
      UNUSED(log_dir);
      ob_init_memory_pool();
      //      TBSYS_LOGGER.setFileName(log_dir);
      TBSYS_LOGGER.setMaxFileSize(kMaxLogSize);
      TBSYS_LOGGER.setLogLevel(level); 
      ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
      return OB_SUCCESS;
    }

    int OceanbaseDb::search_tablet_cache(const std::string &table, const DbRowKey &rowkey, TabletInfo &loc)
    {
      tbsys::CThreadGuard guard(&cache_lock_);
      CacheSet::iterator itr = cache_set_.find(table);
      if (itr == cache_set_.end())
        return OB_ERROR;

      CacheRow::iterator row_itr = itr->second.lower_bound(rowkey);
      if (row_itr == itr->second.end())
        return OB_ERROR;

      //remove timeout tablet
      if (row_itr->second.expired(tablet_timeout_)) {
        itr->second.erase(row_itr);
        return OB_ERROR; 
      }

      loc = (row_itr->second);
      return OB_SUCCESS;
    }

    void OceanbaseDb::try_mark_server_fail(TabletInfo &tablet_info, ObServer &server, bool &do_erase_tablet)
    {
      int i;
      for(i = 0; i < kTabletDupNr; i++) {
        if (tablet_info.slice_[i].ip_v4 == server.get_ipv4() && 
            tablet_info.slice_[i].ms_port == server.get_port()) {
          tablet_info.slice_[i].server_avail = false;
          break;
        }
      }

      //check wether tablet is available, if not delete it from set
      for(i = 0; i < kTabletDupNr ; i++) {
        if (tablet_info.slice_[i].server_avail == true) {
          break; 
        }
      }

      if (i == kTabletDupNr) {
        do_erase_tablet = true;
      } else {
        do_erase_tablet = false;
      }
    }

    void OceanbaseDb::mark_ms_failure(ObServer &server, const std::string &table, const ObString &rowkey)
    {
      int ret = OB_SUCCESS;
      CacheRow::iterator row_itr;

      tbsys::CThreadGuard guard(&cache_lock_);

      CacheSet::iterator itr = cache_set_.find(table);
      if (itr == cache_set_.end())
        ret = OB_ERROR;

      if (ret == OB_SUCCESS) {
        row_itr = itr->second.lower_bound(rowkey);
        if (row_itr == itr->second.end())
          ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS) {
        TabletInfo &tablet_info = row_itr->second;
        bool do_erase_tablet = false;

        try_mark_server_fail(tablet_info, server, do_erase_tablet);
        if (do_erase_tablet)
          itr->second.erase(row_itr);
      } else {
        TBSYS_LOG(WARN, "tablet updated, no such rowkey");
      }
    }

    int OceanbaseDb::get_tablet_location(const std::string &table, const DbRowKey &rowkey, 
                                         common::ObServer &server)
    {
      int ret = OB_SUCCESS;
      TabletInfo tablet_info;
      TabletSliceLocation loc;

      ret = search_tablet_cache(table, rowkey, tablet_info);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(DEBUG, "Table %s not find in tablet cache, do root server get", table.c_str());

        ret = get_ms_location(rowkey, table);
        if (ret == OB_SUCCESS)
          ret = search_tablet_cache(table, rowkey, tablet_info);
        else {
          TBSYS_LOG(WARN, "get_ms_location faild , retcode:[%d]", ret);
        }
      }

      if (ret == common::OB_SUCCESS) {
        ret = tablet_info.get_one_avail_slice(loc, rowkey);
        if (ret == common::OB_SUCCESS) {
          server.set_ipv4_addr(loc.ip_v4, loc.ms_port);
        } else {
          TBSYS_LOG(ERROR, "No available Merger Server");
        }
      }

      return ret;
    }

    int OceanbaseDb::get_ms_location(const DbRowKey &row_key, const std::string &table_name)
    {
      int ret = OB_SUCCESS;
      ObScanner scanner;
      std::vector<std::string> columns;
      columns.push_back("*");
      DbRecordSet ds;

      if (!inited_) {
        TBSYS_LOG(ERROR, "OceanbaseDb is not Inited, plesase Initialize it first");
        ret = OB_ERROR;
      } else if ((ret = ds.init()) != common::OB_SUCCESS) {
        TBSYS_LOG(INFO, "DbRecordSet Init error");
      }

      if (ret == OB_SUCCESS) {
        int retires = kTabletDupNr;

        while(retires--) {
          ret = do_server_get(root_server_, row_key, ds.get_scanner(), 
                              ds.get_buffer(), table_name, columns);
          if (ret == OB_SUCCESS)
            break;
        }

        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "do server get failed, ret=[%d]", ret);
        }
      }

      if (ret == OB_SUCCESS) {
//        dump_scanner(ds.get_scanner());
        DbRecordSet::Iterator itr = ds.begin();
        while (itr != ds.end()) {
          DbRecord *recp;
          TabletInfo tablet_info;

          itr.get_record(&recp);
          if (recp == NULL) {
            TBSYS_LOG(WARN, "NULL record skip line");
            itr++;
            continue;
          }

          ret = tablet_info.parse_from_record(recp);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "pase from record failed");
            break;
          }

          insert_tablet_cache(table_name, tablet_info.get_end_key(), tablet_info);
          itr++;
        }
      }

      return ret;
    }


    void OceanbaseDb::insert_tablet_cache (const std::string &table, const DbRowKey &rowkey, TabletInfo &tablet)
    {
      tbsys::CThreadGuard guard(&cache_lock_);

      CacheSet::iterator set_itr = cache_set_.find(table);
      if (set_itr == cache_set_.end()) {
        CacheRow row;

        row.insert(CacheRow::value_type(rowkey, tablet));
        cache_set_.insert(CacheSet::value_type(table, row));
      } else {
        CacheRow::iterator row_itr = set_itr->second.find(rowkey);
        if (row_itr != set_itr->second.end()) {
          set_itr->second.erase(row_itr);
          TBSYS_LOG(DEBUG, "deleting cache table is %s", table.c_str());
        } 

        TBSYS_LOG(DEBUG, "insert cache table is %s", table.c_str());
        set_itr->second.insert(std::make_pair(rowkey, tablet));
      }
    }		/* -----  end of method OceanbaseDb::insert_tablet_cache  ----- */

    void OceanbaseDb::free_tablet_cache( )
    {
      CacheSet::iterator set_itr = cache_set_.begin();
      while (set_itr != cache_set_.end()) {
        CacheRow::iterator row_itr = set_itr->second.begin();
        while (row_itr != set_itr->second.end()) {
          row_itr++;
        }
        set_itr++;
      }
    }		/* -----  end of method OceanbaseDb::free  ----- */

    int OceanbaseDb::do_muti_get(common::ObServer &server, const std::vector<DbMutiGetRow>& rows, 
                                 ObScanner &scanner, ObDataBuffer& data_buff)
    {
      ObGetParam *get_param = NULL;
      int ret = init_get_param(get_param, rows);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(WARN, "can't init get_param");
      }

      if (ret == OB_SUCCESS) {
        data_buff.get_position() = 0;
        ret = get_param->serialize(data_buff.get_data(), data_buff.get_capacity(),
                                   data_buff.get_position());
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(WARN, "can't deserialize get param");
        }
      }

#if 0
      for(int64_t i = 0;i < get_param->get_cell_size(); i++) {
        char buf[128];

        int len = hex_to_str((*get_param)[i]->row_key_.ptr(), (*get_param)[i]->row_key_.length(), buf ,128);
        buf[2 * len] = 0;

        std::string table_name = 
          std::string((*get_param)[i]->table_name_.ptr(), (*get_param)[i]->table_name_.length());
        std::string column_name =
          std::string((*get_param)[i]->column_name_.ptr(), (*get_param)[i]->column_name_.length());

        TBSYS_LOG(INFO, "tablet=%s,Column=%s, Rowkey=%s", table_name.c_str(), column_name.c_str(), buf);
        (*get_param)[i]->value_.dump(TBSYS_LOG_LEVEL_INFO);
      }
#endif
#if 0
      {
        char buf[128];
        server.to_string(buf, 128);
        TBSYS_LOG(INFO, "Assessing Merge server, %s", buf);
      }
#endif

      if (ret == OB_SUCCESS) {
        int64_t pos = 0;

        ret = do_server_cmd(server, (int32_t)OB_GET_REQUEST, data_buff, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(WARN, "can't do server cmd, ret=%d", ret);
        } else {
          scanner.clear();

          ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(WARN, "ob scanner can't deserialize buffer, pos=%ld, ret=%d",pos, ret);
          }
        }
      }

      return ret;
    }

    int OceanbaseDb::init_get_param(ObGetParam *&param, const std::vector<DbMutiGetRow> &rows)
    {
      ObCellInfo cell;

      ObGetParam *get_param = GET_TSI_MULT(ObGetParam, TSI_GET_ID);
      if (get_param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return OB_ERROR;
      }

      get_param->reset();

      ObBorderFlag border;
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_max_value();
      border.set_min_value();

      ObVersionRange version_range;

      version_range.start_version_ = 0;
      version_range.end_version_ = 0;

      version_range.border_flag_ = border;
      get_param->set_version_range(version_range);
      param = get_param;
      param->set_is_read_consistency(consistency_);

      for(size_t i = 0;i < rows.size(); i++) {
        std::vector<std::string>::iterator itr = rows[i].columns->begin();
        cell.row_key_ = rows[i].rowkey;
        cell.table_name_.assign_ptr(const_cast<char *>(rows[i].table.c_str()), static_cast<int32_t>(rows[i].table.length()));

        while(itr != rows[i].columns->end()) {
          cell.column_name_.assign_ptr(const_cast<char *>(itr->c_str()), static_cast<int32_t>(itr->length()));

          int ret = get_param->add_cell(cell);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
            return ret;
          }

          itr++;
        }
      }

      return OB_SUCCESS;
    }

    int OceanbaseDb::do_server_get(common::ObServer &server, const DbRowKey& row_key, 
                                   ObScanner &scanner, 
                                   ObDataBuffer& data_buff, const std::string &table_name, 
                                   std::vector<std::string> &columns)
    {
      int ret;
      ObCellInfo cell;
      ObGetParam *get_param = GET_TSI_MULT(ObGetParam, TSI_GET_ID);
      if (get_param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return OB_ERROR;
      }
      get_param->reset();

      ObBorderFlag border;
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_max_value();
      border.set_min_value();

      ObVersionRange version_range;

      version_range.start_version_ = 0;
      version_range.end_version_ = 0;

      version_range.border_flag_ = border;
      get_param->set_version_range(version_range);
      get_param->set_is_read_consistency(consistency_);

      cell.row_key_ = row_key;
      cell.table_name_.assign_ptr(const_cast<char *>(table_name.c_str()), static_cast<int32_t>(table_name.length()));

      std::vector<std::string>::iterator itr = columns.begin();
      while(itr != columns.end()) {
        cell.column_name_.assign_ptr(const_cast<char *>(itr->c_str()), static_cast<int32_t>(itr->length()));
        int ret = get_param->add_cell(cell);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
          return ret;
        }
        itr++;
      }

      data_buff.get_position() = 0;
      ret = get_param->serialize(data_buff.get_data(), data_buff.get_capacity(),
                                 data_buff.get_position());
      if (OB_SUCCESS == ret) {
        /* update send bytes */
        __sync_add_and_fetch(&db_stats_.total_send_bytes, data_buff.get_position());

        ret = client_.send_request(server, OB_GET_REQUEST, 1,
                                   timeout_, data_buff);

        /* update recv bytes */
        if (ret == OB_SUCCESS) {
          __sync_add_and_fetch(&db_stats_.total_recv_bytes, data_buff.get_position());
        }
      } else {
        TBSYS_LOG(WARN, "serialzie get param failed, ret=%d", ret);
      }

      int64_t pos = 0;

      char ip_buf[25];
      server.ip_to_string(ip_buf, 25);
      TBSYS_LOG(DEBUG, "Merger server ip is %s, port is %d", ip_buf, server.get_port());

      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

      if (OB_SUCCESS != ret)
        TBSYS_LOG(ERROR, "do_server_get deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      else
        ret = result_code.result_code_;

      if (OB_SUCCESS == ret) {
        scanner.clear();
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
          TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
      }

      if (ret != OB_SUCCESS)
        TBSYS_LOG(ERROR, "do server get failed:%d, server=%s", ret, ip_buf);

      return ret;
    }

    int OceanbaseDb::scan(const std::string &table, const std::vector<std::string> &columns, 
             const DbRowKey &start_key, const DbRowKey &end_key, DbRecordSet &rs, int64_t version, 
             bool inclusive_start, bool inclusive_end)
    {
      int ret = OB_SUCCESS;
      TabletInfo tablet_info;

      if (end_key.ptr() == NULL || end_key.length() == 0) {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "scan with null end key");
      }

      if (ret == OB_SUCCESS) {
        ret = search_tablet_cache(table, end_key, tablet_info);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(DEBUG, "Table %s not find in tablet cache, do root server get", table.c_str());

          ret = get_ms_location(end_key, table);
          if (ret == OB_SUCCESS)
            ret = search_tablet_cache(table, end_key, tablet_info);
          else {
            TBSYS_LOG(WARN, "get_ms_location faild , retcode:[%d]", ret);
          }
        }
      }

      if (ret == OB_SUCCESS) {
        ret = scan(tablet_info, table, columns, start_key, end_key, rs, version, inclusive_start, inclusive_end);
      }

      return ret;
    }


    int OceanbaseDb::scan(const TabletInfo &tablets, const std::string &table, const std::vector<std::string> &columns, 
                          const DbRowKey &start_key, const DbRowKey &end_key, DbRecordSet &rs, int64_t version, 
                          bool inclusive_start, bool inclusive_end)
    {
      int ret = OB_SUCCESS;
      ObScanParam *param = get_scan_param(table, columns, start_key, 
                                          end_key, inclusive_start, inclusive_end, version);
      if (NULL == param) {
        ret = OB_ERROR;
      } else {
        ObServer server;

        for(size_t i = 0;i < (size_t)kTabletDupNr; i++) {
          if (tablets.slice_[i].server_avail == true) {
            char server_str[128];

            server.set_ipv4_addr(tablets.slice_[i].ip_v4, tablets.slice_[i].ms_port);
            server.to_string(server_str, 128);

            ObDataBuffer data_buff = rs.get_buffer();

            ret = param->serialize(data_buff.get_data(), data_buff.get_capacity(),
                                   data_buff.get_position());
            if (ret != OB_SUCCESS) {
              TBSYS_LOG(WARN, "can't serialize scan param to databuf, ret=%d", ret);
            } else {
              int64_t pos = 0;

              ret = do_server_cmd(server, (int64_t)OB_SCAN_REQUEST, data_buff, pos);
              if (ret == OB_SUCCESS) {
                ObScanner &scanner = rs.get_scanner();
                scanner.clear();

                ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
                if (ret != OB_SUCCESS) {
                  TBSYS_LOG(WARN, "can't deserialize obscanner, ret=%d", ret);
                }
              } else {
                TBSYS_LOG(WARN, "error when calling do_server_cmd, ret=[%d], server:%s", ret, server_str);
              }
            }

            if (ret == OB_SUCCESS) {
              TBSYS_LOG(DEBUG, "scan requesting server, %s", server_str);
              break;
            }

            if (i < (kTabletDupNr - 1))
              usleep(5000);
          }
        }
      }

      return ret;
    }

    common::ObScanParam *OceanbaseDb::get_scan_param(const std::string &table, const std::vector<std::string>& columns,
                                                     const DbRowKey &start_key, const DbRowKey &end_key,
                                                     bool inclusive_start, bool inclusive_end,
                                                     int64_t data_version)
    {
      ObScanParam *param = GET_TSI_MULT(ObScanParam, TSI_SCAN_ID);
      if (param == NULL) {
        TBSYS_LOG(ERROR, "can't allocate memory from TSI");
        return NULL;
      }

      TBSYS_LOG(DEBUG, "scan param version is %ld", data_version);
      param->reset();

      ObBorderFlag border;

      /* setup version */
      border.set_inclusive_start();
      border.set_inclusive_end();
      border.set_min_value();

      if (data_version == 0) {
        border.set_max_value();
      }

      ObVersionRange version_range;
      version_range.end_version_ = data_version;
      version_range.border_flag_ = border;
      param->set_version_range(version_range);

      /* set consistency */
      param->set_is_read_consistency(consistency_);

      /* do not cache the result */
      param->set_is_result_cached(false);

      /* setup scan range */
      ObBorderFlag scan_border;
      if (inclusive_start)
        scan_border.set_inclusive_start();
      else
        scan_border.unset_inclusive_start();

      if (inclusive_end)
        scan_border.set_inclusive_end();
      else
        scan_border.unset_inclusive_end();

      if (start_key.ptr() == NULL || start_key.length() == 0)
        scan_border.set_min_value();

      ObRange range;
      range.start_key_ = start_key;
      range.end_key_ = end_key;
      range.border_flag_ = scan_border;

      ObString table_name;
      table_name.assign(const_cast<char *>(table.c_str()), static_cast<int32_t>(table.length()));

      param->set(OB_INVALID_ID, table_name, range);

      ObString column;
      for(size_t i = 0;i < columns.size(); i++) {
        column.assign_ptr(const_cast<char *>(columns[i].c_str()), static_cast<int32_t>(columns[i].length()));

        if (param->add_column(column) != OB_SUCCESS) {
          param = NULL;
          TBSYS_LOG(ERROR, "can't add column to scan param");
          break;
        }
      }

      return param;
    }

    int OceanbaseDb::fetch_schema(common::ObSchemaManagerV2& schema_manager)
    { 
      int ret;
      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL) {
        TBSYS_LOG(ERROR, "Fetch schema faild, due to memory lack");
        return common::OB_MEM_OVERFLOW;
      }

      ObDataBuffer data_buff;
      data_buff.set_data(buff, k2M);
      data_buff.get_position() = 0;

      serialization::encode_vi64(data_buff.get_data(), 
                                 data_buff.get_capacity(), data_buff.get_position(), 1);

      ret = client_.send_request(root_server_, OB_FETCH_SCHEMA, 1,
                                 10000000, data_buff);

      int64_t pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);

      if (OB_SUCCESS != ret)
        TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      else
        ret = result_code.result_code_;

      if (OB_SUCCESS == ret) {
        ret = schema_manager.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize schema from buff failed:"
                    "version[%d], pos[%ld], ret[%d]", 1, pos, ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch schema succ:version[%ld]", schema_manager.get_version());
        }
      }

      delete [] buff;

      return ret;
    }

    int OceanbaseDb::do_server_cmd(const ObServer &server, const int32_t opcode, 
                                   ObDataBuffer &inout_buffer, int64_t &pos)
    {
      int ret = OB_SUCCESS;

      ret = client_.send_request(server, opcode, 1, timeout_, inout_buffer);

      pos = 0;
      ObResultCode result_code;
      if (OB_SUCCESS == ret) 
      {
        ret = result_code.deserialize(inout_buffer.get_data(), inout_buffer.get_position(), pos);
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }

      return ret;
    }

    int OceanbaseDb::get_memtable_version(int64_t &version)
    {
      int ret = OB_SUCCESS;

      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocate memory for get_memtable_version");
      }

      if (ret == OB_SUCCESS) {
        ObDataBuffer buffer(buff, OB_MAX_PACKET_LENGTH);
        int64_t pos = 0;

        ret = do_server_cmd(update_server_, (int32_t)OB_UPS_GET_LAST_FROZEN_VERSION, buffer, pos);
        if (ret != OB_SUCCESS) {
          TBSYS_LOG(ERROR, "can't get memtable version errorcode=%d", ret);
        } else {
          ret = serialization::decode_vi64(buffer.get_data(), 
                                           buffer.get_position(), pos, &version);
          if (ret != OB_SUCCESS) {
            TBSYS_LOG(ERROR, "can't decode version, errorcode=%d", ret);
          }
        }
      }

      return ret;
    }

    int OceanbaseDb::get_update_server(ObServer &server)
    {
      int ret = OB_SUCCESS;
      char *buff = new(std::nothrow) char[k2M];
      if (buff == NULL)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocate memory from heap");
      }

      if (ret == OB_SUCCESS)
      {
        ObDataBuffer buffer(buff, OB_MAX_PACKET_LENGTH);
        int64_t pos = 0;

        ret = do_server_cmd(root_server_, (int32_t)OB_GET_UPDATE_SERVER_INFO, buffer, pos);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "can't get updateserver errorcode=%d", ret);
        }
        else
        {
          ret = update_server_.deserialize(buffer.get_data(), buffer.get_position(), pos);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "can't get updateserver");
          }
          else
          {
            server = update_server_;
          }
        }
      }

      if (buff != NULL)
      {
        delete [] buff;
      }

      return ret;
    }

    int OceanbaseDb::start_tnx(DbTranscation *&tnx)
    {
      int ret = OB_SUCCESS;
      tnx = new(std::nothrow) DbTranscation(this);
      if (tnx == NULL) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't allocat transcation class");
      } else {
        ref();
      }

      return ret;
    }

    void OceanbaseDb::end_tnx(DbTranscation *&tnx)
    {
      if (tnx != NULL) {
        delete tnx;
        tnx = NULL;
        unref();
      }
    }

  }
}
