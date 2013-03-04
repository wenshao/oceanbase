#ifndef OCEANBASE_SQL_SCAN_PARAM_H_
#define OCEANBASE_SQL_SCAN_PARAM_H_

#include "common/ob_define.h"
#include "common/ob_array_helper.h"
#include "common/ob_object.h"
#include "common/ob_range.h"
#include "common/ob_simple_filter.h"
#include "common/ob_common_param.h"
#include "common/ob_composite_column.h"

#include "ob_project.h"
#include "ob_limit.h"
#include "ob_filter.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObSqlScanParamTest_serialize_test_Test;

namespace oceanbase
{
  namespace sql 
  {
    class ObSqlScanParam : public ObReadParam
    {
    public:
      ObSqlScanParam();
      virtual ~ObSqlScanParam();

      int set(const uint64_t& table_id, const ObRange& range, bool deep_copy_args = false);

      int set_range(const ObRange& range);

      inline uint64_t get_table_id() const
      {
        return table_id_;
      }
      inline const ObRange* const get_range() const
      {
        return &range_;
      }
      void reset(void);

      int set_project(const ObProject &project);
      int set_filter(const ObFilter &filter);
      int set_limit(const ObLimit &limit);

      inline bool has_project() const;
      inline bool has_filter() const;
      inline bool has_limit() const;

      NEED_SERIALIZE_AND_DESERIALIZE;

      // dump scan param info, basic version
      void dump(void) const;
        
    private:
      // BASIC_PARAM_FIELD
      int serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_basic_param_serialize_size(void) const;

      // END_PARAM_FIELD
      int serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_end_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_end_param_serialize_size(void) const;


    private:
      friend class ::ObSqlScanParamTest_serialize_test_Test;

      ObStringBuf buffer_pool_;
      bool deep_copy_args_;
      uint64_t table_id_;
      ObRange range_;
      ObProject project_;
      ObLimit limit_;
      ObFilter filter_;
      bool has_project_;
      bool has_limit_;
      bool has_filter_;
    };

    inline bool ObSqlScanParam::has_project() const
    {
      return has_project_;
    }
    
    inline bool ObSqlScanParam::has_filter() const
    { 
      return has_filter_; 
    }
    
    inline bool ObSqlScanParam::has_limit() const
    {
      return has_limit_; 
    }

  } /* sql */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_SQL_SCAN_PARAM_H_ */

