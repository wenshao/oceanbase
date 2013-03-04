#ifndef __OB_IMPORT_H__
#define  __OB_IMPORT_H__

#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "tokenizer.h"
#include "file_reader.h"
#include "oceanbase_db.h"
#include <vector>

using namespace oceanbase::common;
using namespace oceanbase::api;

struct ColumnDesc {
  std::string name;
  int offset;
  int len;
};

struct RowkeyDesc {
  int offset;
  int type;
  int pos;
};

struct ColumnInfo {
  const ObColumnSchemaV2 *schema;
};

class TestRowBuilder;

class ObRowBuilder {
  public:
    friend class TestRowBuilder;
  public:
    enum RowkeyDataType{
      INT8,
      INT64,
      INT32,
      VARCHAR,
      DATETIME,
      INT16
    };

  static const int kMaxRowkeyDesc = 10;
  public:
    ObRowBuilder(ObSchemaManagerV2 *schema, const char *table_name, int input_column_nr, const RecordDelima &delima);
    ~ObRowBuilder();
    
    int set_column_desc(std::vector<ColumnDesc> &columns);

    int set_rowkey_desc(std::vector<RowkeyDesc> &rowkeys);

    bool check_valid();

    int build_tnx(RecordBlock &block, DbTranscation *tnx);

    int create_rowkey(ObString &rowkey, TokenInfo *tokens);
    int setup_content(RowMutator *mutator, TokenInfo *tokens);
    int make_obobj(int token_idx, ObObj &obj, TokenInfo *tokens);

    ColumnInfo columns_desc_[OB_MAX_COLUMN_NUMBER];
  private:
    ObSchemaManagerV2 *schema_;
    RecordDelima delima_;

    RowkeyDesc rowkey_desc_[kMaxRowkeyDesc];
    size_t rowkey_desc_nr_;

    int columns_desc_nr_;

    const char *table_name_;

    int input_column_nr_;

    atomic_t lineno_;

    int64_t rowkey_max_size_;
};

#endif
