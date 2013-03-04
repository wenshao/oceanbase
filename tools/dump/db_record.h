/*
 * =====================================================================================
 *
 *       Filename:  DbRecord.h
 *
 *        Version:  1.0
 *        Created:  04/12/2011 08:14:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OP_API_DBRECORD_H
#define  OP_API_DBRECORD_H

#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include <string>
#include <map>

namespace oceanbase {
    namespace api {
        class DbRecord;

        class DbRecord {
          public:
              typedef std::map< std::string, common::ObCellInfo> RowData;
              typedef RowData::iterator Iterator;
              ~DbRecord();

              int get(std::string &column, common::ObCellInfo **cell);
              int get(const char *column_str, common::ObCellInfo **cell);

              int get_table_id(int64_t &table_id);
              int get_rowkey(common::ObString &rowkey);

              void append_column(common::ObCellInfo &cell);

              void reset();

              Iterator begin() { return row_.begin(); }
              Iterator end() { return row_.end(); }

              int serialize(common::ObDataBuffer &data_buff);

              bool empty() { return row_.empty(); }

              void dump();

          private:
              RowData row_;
        };
    }
}

#endif
