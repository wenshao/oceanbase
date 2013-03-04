#ifndef __OB_IMPORT_PARAM_H__
#define  __OB_IMPORT_PARAM_H__

#include <vector>
#include <string>
#include "ob_import.h"
#include "tokenizer.h"

struct TableParam {
  TableParam() 
    : input_column_nr(0), delima('\01'), rec_delima('\n'), concurrency(10) { }

  std::vector<ColumnDesc> col_descs;
  std::vector<RowkeyDesc> rowkey_descs;
  std::string table_name;
  std::string data_file;
  int input_column_nr;
  RecordDelima delima;
  RecordDelima rec_delima;
  int concurrency;                              /* default 5 threads */
  const char *bad_file_;
};

class ImportParam {
  public:
    ImportParam();

    int load(const char *file);

    int get_table_param(const char *table_name, TableParam &param);

    void PrintDebug();
  private:
    std::vector<TableParam> params_;
};

#endif
