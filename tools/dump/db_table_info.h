/*
 * =====================================================================================
 *
 *       Filename:  DbTableInfo.h
 *
 *        Version:  1.0
 *        Created:  04/13/2011 09:57:25 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#ifndef OB_API_DBTABLEINFO_H
#define  OB_API_DBTABLEINFO_H

#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "db_record.h"
#include <string>

namespace oceanbase {
    namespace api {
        const int k2M = 1024 * 1024 * 2;
        const int kTabletDupNr = 3;

        typedef common::ObString DbRowKey;
        using namespace oceanbase::common;


        class DbRecord;

        struct TabletSliceLocation {
            TabletSliceLocation()
              :ip_v4(0), cs_port(0), ms_port(0), tablet_version(0), server_avail(false) { }

            int32_t ip_v4;
            unsigned short cs_port;
            unsigned short ms_port;
            int64_t tablet_version;
            bool server_avail;
        };

        struct TabletStats {
          TabletStats() : hit_times(0) { }

          int64_t hit_times;
        };

        class TabletInfo {
          public:
              TabletInfo(void);
              TabletInfo(const TabletInfo &src);

              ~TabletInfo();

              TabletInfo &operator=(const TabletInfo &src);

              void dump_slice(void);

              int get_tablet_location(int idx, TabletSliceLocation &loc); 

              int parse_from_record(DbRecord *recp); 

              int get_one_avail_slice(TabletSliceLocation &loc, const DbRowKey &rowkey);

              TabletSliceLocation slice_[kTabletDupNr];

              DbRowKey get_end_key();

              int assign_end_key(const DbRowKey &key);

              TabletStats stats() const { return stats_; }

              bool expired(uint64_t timeout) const;
              int64_t ocuppy_size_;
              int64_t record_count_;

          private:
              int parse_one_cell(const common::ObCellInfo *cell);
              int check_validity();

              uint64_t timestamp_;
              TabletStats stats_;
              std::string rowkey_buffer_;
        };
    }
}
#endif
