#ifndef OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "sstable/ob_sstable_row.h"
#include "common/ob_schema.h"
#include "sstable_writer.h"
#include "com_taobao_mrsstable_SSTableBuilder.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    class SSTableBuilder
    {
      public:
        static const int MAX_COLUMS = 128;
        static const int ROW_KEY_BUF_LEN = 1024;

        struct ObColumn
        {
          char *column_;
          int32_t len_;
        };
        
        SSTableBuilder();
        ~SSTableBuilder();

        int init(const uint64_t table_id, const common::ObSchemaManagerV2* schema);
        int start_builder();
        int append(const char* input, const int64_t input_size, 
          bool is_first, bool is_last, const char** output, int64_t* output_size);

      private:
        int close_sstable();
        const char *read_line(const char* input, const int64_t input_size, 
          int64_t& pos, int &fields);
        int process_line(int fields);
        
        int transform_date_to_time(const char *str, common::ObDateTime& val);
        int create_rowkey(char *buf, int64_t buf_len, int64_t& pos);

      private:
        int64_t current_sstable_size_;
        int64_t serialize_size_;
        int64_t table_id_;
        int64_t total_rows_;

        ObColumn colums_[MAX_COLUMS];
        char row_key_buf_[ROW_KEY_BUF_LEN];
        common::ObString row_key_;
        common::ObObj row_value_[MAX_COLUMS];
        uint64_t column_id_[MAX_COLUMS];
        
        common::ObString compressor_string_;
        sstable::ObSSTableRow sstable_row_;
        
        const common::ObTableSchema *table_schema_;
        const common::ObSchemaManagerV2 *schema_;
        sstable::ObSSTableSchema *sstable_schema_;
        SSTableWriter writer_;
    };
  } /* chunkserver */
} /* oceanbase */

#ifdef __cplusplus
extern "C" {
#endif

int init(const char* schema_file, const char* syntax_file);
int append(const char* input, const int64_t input_size, 
  bool is_first, bool is_last, const char** output, int64_t* output_size);
void clolse();

#ifdef __cplusplus
}
#endif

#endif /*OCEANBASE_CHUNKSERVER_SSTABLE_BUILDER_H_ */
