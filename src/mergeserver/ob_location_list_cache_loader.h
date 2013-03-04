#ifndef OCEANBASE_COMMON_LOCATION_LIST_CACHE_LOADER_H_
#define OCEANBASE_COMMON_LOCATION_LIST_CACHE_LOADER_H_
#include <stdint.h>
#include "common/ob_scanner.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "ob_ms_tablet_location_item.h"
#include "ob_ms_tablet_location.h"
using namespace oceanbase::common;
//using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver 
  {
    class ObLocationListCacheLoader
    {
      public:
        static const int32_t OB_MAX_IP_SIZE = 64;
        ObLocationListCacheLoader();
 
      public:
        int load(const char *config_file_name, const char *section_name);
        int get_decoded_location_list_cache(ObMergerTabletLocationCache & cache);

        void dump_config();

      private:
        int load_from_config(const char *section_name);
        int load_string(char* dest, const int32_t size,
            const char* section, const char* name, bool require=true);
        void dump_param(ObScanner &param);
      private:
        ObStringBuf strbuf;
        static const int64_t max_count = 10240;
        int table_id_[max_count];
        ObString range_start_[max_count];
        ObString range_end_[max_count];
        ObString server_list_[max_count];
        // if new config item added, don't forget add it into dump_config
        int64_t range_num_;
        bool config_loaded_;
        tbsys::CConfig config_;
        char   fake_max_row_key_buf_[256];
        ObString fake_max_row_key_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */
