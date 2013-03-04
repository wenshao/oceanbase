/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-12-06
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_COMMON_OB_STATISTICS_H_
#define OCEANBASE_COMMON_OB_STATISTICS_H_
#include "ob_define.h"
#include "ob_array_helper.h"
namespace oceanbase
{
  namespace common
  {
    struct ObStat
    {
      public:
        enum 
        {
          MAX_STATICS_PER_TABLE = 30,
        };
        ObStat();
        uint64_t get_table_id() const;
        int64_t get_value(const int32_t index) const;
        
        int set_value(const int32_t index, int64_t value);
        int inc(const int32_t index, const int64_t inc_value = 1);

        void set_table_id(const uint64_t table_id);
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        uint64_t table_id_;
        volatile int64_t values_[MAX_STATICS_PER_TABLE];
    };
    class ObStatManager
    {
      public:
        typedef int64_t (*OpFunc)(const int64_t lv, const int64_t rv);
        static int64_t addop(const int64_t lv, const int64_t rv);
        static int64_t subop(const int64_t lv, const int64_t rv);
      public:
        enum
        {
          SERVER_TYPE_INVALID = 0,
          SERVER_TYPE_ROOT = 1,
          SERVER_TYPE_CHUNK = 2,
          SERVER_TYPE_MERGE = 3,
          SERVER_TYPE_UPDATE = 4,
        };
        typedef const ObStat* const_iterator;

        explicit ObStatManager(uint64_t server_type);
        ObStatManager();
        virtual ~ObStatManager();
        ObStatManager & operator=(const ObStatManager &rhs);
        uint64_t get_server_type() const;
        void set_server_type(const uint64_t server_type);

        int set_value(const uint64_t table_id, const int32_t index, const int64_t value);
        int inc(const uint64_t table_id, const int32_t index, const int64_t inc_value = 1);
        int reset();
        int get_stat(const uint64_t table_id, ObStat* &stat) const;

        ObStatManager & subtract(const ObStatManager &minuend);
        ObStatManager & add(const ObStatManager &augend);
        ObStatManager & operate(const ObStatManager &operand, OpFunc op);

        const_iterator begin() const;
        const_iterator end() const;

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        uint64_t server_type_;
        ObStat data_holder_[OB_MAX_TABLE_NUMBER];
        ObArrayHelper<ObStat> table_stats_;

    };
  }
}
#endif
