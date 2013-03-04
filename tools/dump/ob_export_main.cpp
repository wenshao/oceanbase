#include <string>
#include <vector>
#include <inttypes.h>
#include "db_utils.h"
#include "oceanbase_db.h"
#include "db_log_monitor.h"
#include "db_dumper_config.h"
#include "common/utility.h"
#include "common/file_utils.h"
#include "common/ob_string.h"
#include "common/serialization.h"
#include "ob_data_set.h"
#include "tokenizer.h"

using namespace oceanbase::api;
using namespace oceanbase::common;
using namespace std;

void usage(const char *program)
{
  fprintf(stdout, "Usage: %s -t table -c column -h host -p port\n"
          "\t\t-d delima -r rec_delima -m consistency\n"
          "\t\t-s start_key -e end_key -o output\n"
          "\t\t-k rowkey_split_str\n\n"
          "rowkey_split_str:\tsplit rowkey strategy, rule:\n"
          "\tcolumn_type(i or s, i for int, s for string) + byte_count + comma + next if exist.\n"
          "\te.g. There's rowkey with 21 bytes, which's composed by a int64,\n"
          "\t12 bytes string and a int8. Then `rowkey_split_str' should set `i8,s12,i1'.\n",
          program);
}

struct ExportParam {
  const char *table;
  vector<string> columns;
  const char *host;
  const char *output;
  unsigned short port;
  ObString start_key;
  ObString end_key;
  vector<int> rowkey_split_lens;
  vector<char> rowkey_split_types;
  char *rowkey_split;

  RecordDelima delima;
  RecordDelima rec_delima;

  ObMemBuf start_key_buff;
  ObMemBuf end_key_buff;

  bool consistency;

  ExportParam() : delima(9), rec_delima(10) {   /* default delima TAB -- rec_delima ENTER */
    table = NULL;
    host = NULL;
    output = NULL;
    port = static_cast<uint16_t>(-1);
    consistency = false;
  }

  void parse_delima(const char *str_delima, RecordDelima &out_delima) {
    const char *end_pos = str_delima + strlen(str_delima);

    if (find(str_delima, end_pos, ',') == end_pos) {
      out_delima = RecordDelima(static_cast<char>(atoi(str_delima)));
    } else {
      int part1, part2;

      sscanf(str_delima, "%d,%d", &part1, &part2);
      out_delima = RecordDelima(static_cast<char>(part1), static_cast<char>(part2));
    }
  }

  void parse_startkey(const char *str_key) {
    start_key_buff.ensure_space(strlen(str_key) / 2);
    int len = str_to_hex(str_key, static_cast<int32_t>(strlen(str_key)), start_key_buff.get_buffer(), 
                         static_cast<int32_t>(start_key_buff.get_buffer_size()));
    start_key.assign_ptr(start_key_buff.get_buffer(), len / 2);
  }

  void parse_endkey(const char *str_key) {
    end_key_buff.ensure_space(strlen(str_key) / 2);
    int len = str_to_hex(str_key, static_cast<int32_t>(strlen(str_key)), end_key_buff.get_buffer(), 
                         static_cast<int32_t>(end_key_buff.get_buffer_size()));
    end_key.assign_ptr(end_key_buff.get_buffer(), len / 2);
  }

  void parse_rowkey_split(char *split_str) {
    int len;
    char *p;
    
    p = strtok(split_str, ",");
    while (NULL != p) {
      len = atoi(p + 1);
      if ((p[0] != 's' && p[0] != 'i') || len == 0) {
        fprintf(stderr, "rowkey split parameter parsed error!");
        exit(-1);
      }
      rowkey_split_types.push_back(p[0]);
      rowkey_split_lens.push_back(len);
      
      p = strtok(NULL, ",");
    }

  }
  
  int check_param() {
    int ret = 0;
    if (host == NULL || table == NULL || columns.size() == 0 ||
        start_key.length() == 0 || end_key.length() == 0) {
      ret = -1;
    }

    return ret;
  }
  
  void PrintDebug() {
    fprintf(stderr, "#######################################################################\n");

    fprintf(stderr, "host=%s, port=%d, table name=[%s]\n", host, port, table);
        fprintf(stderr, "delima type = %d, part1 = %d, part2 = %d\n", delima.type_, 
            delima.part1_, delima.part2_ );
        fprintf(stderr, "rec_delima type = %d, part1 = %d, part2 = %d\n", rec_delima.type_, 
            rec_delima.part1_, rec_delima.part2_ );

        fprintf(stderr, "columns list as follows\n");
        for(size_t i = 0;i < columns.size(); i++) {
            fprintf(stderr, "ID:%ld\t%s\n", i, columns[i].c_str());
        }

        fprintf(stderr, "start rowkey = %s\n", print_string(start_key));
        fprintf(stderr, "end rowkey = %s\n", print_string(end_key));
    }
};

static ExportParam export_param;

int parse_options(int argc, char *argv[])
{
    int ret = 0;
    while ((ret = getopt(argc, argv, "o:t:c:h:p:d:r:s:e:m:k:")) != -1) {
        switch(ret) {
            case 't':
                export_param.table = optarg;
                break;
            case 'c':
                export_param.columns.push_back(optarg);
                break;
            case 'h':
                export_param.host = optarg;
                break;
            case 'p':
                export_param.port = static_cast<unsigned short>(atol(optarg));
                break;
            case 'd':
                export_param.parse_delima(optarg, export_param.delima);
                break;
            case 'r':
                export_param.parse_delima(optarg, export_param.rec_delima);
                break;
            case 's':
                export_param.parse_startkey(optarg);
                break;
            case 'e':
                export_param.parse_endkey(optarg);
                break;
            case 'm':
                export_param.consistency = static_cast<bool>(atol(optarg));
                break;
            case 'o':
                export_param.output = optarg;
                break;
            case 'k':
                export_param.parse_rowkey_split(optarg);
                break;
            default:
                usage(argv[0]);
                exit(0);
        }
    }

    return export_param.check_param();
}

static int append_delima(ObDataBuffer &buffer, const RecordDelima &delima)
{
    if (buffer.get_remain() < delima.length()) {
        return OB_ERROR;
    }

    delima.append_delima(buffer.get_data(), buffer.get_position(), buffer.get_capacity());
    delima.skip_delima(buffer.get_position());
    return OB_SUCCESS;
}

int write_record(FILE *out, DbRecord *recp)
{
#define MAX_RECORD_LEN 2 * 1024 * 1024
    static char write_buffer[MAX_RECORD_LEN];
    ObDataBuffer buffer(write_buffer, MAX_RECORD_LEN);

    int ret = OB_SUCCESS;
    vector<string>::iterator itr = export_param.columns.begin();
    ObCellInfo *cell = NULL;
    ObString rowkey;
    ret = recp->get_rowkey(rowkey);
    if (ret != OB_SUCCESS) {
        fprintf(stderr, "can't find rowkey from record, currupted data or bugs!\n");
        return ret;
    }

    ObString intstr;
    int len = 0;
    int64_t pos_start = 0;
    int8_t val8 = 0;
    int16_t val16 = 0;
    int32_t val32 = 0;
    int64_t val64 = 0;

    for (unsigned int i = 0; i < export_param.rowkey_split_types.size(); i++) {
      len = export_param.rowkey_split_lens.at(i);
      if (export_param.rowkey_split_types.at(i) == 's') {
        memcpy(buffer.get_data() + buffer.get_position(), rowkey.ptr() + pos_start, len);
        buffer.get_position() += len;
        pos_start += len;
      } else if (export_param.rowkey_split_types.at(i) == 'i') {
        switch (len) {
          case 1:
            serialization::decode_i8(rowkey.ptr(), rowkey.length(), pos_start, &val8);
            buffer.get_position() +=
              snprintf(buffer.get_data() + buffer.get_position(), buffer.get_remain(), "%d", val8);
            break;
          case 2:
            serialization::decode_i16(rowkey.ptr(), rowkey.length(), pos_start, &val16);
            buffer.get_position() +=
              snprintf(buffer.get_data() + buffer.get_position(), buffer.get_remain(), "%d", val16);
            break;
          case 4:
            serialization::decode_i32(rowkey.ptr(), rowkey.length(), pos_start, &val32);
            buffer.get_position() +=
              snprintf(buffer.get_data() + buffer.get_position(), buffer.get_remain(), "%d", val32);
            break;
          case 8:
            serialization::decode_i64(rowkey.ptr(), rowkey.length(), pos_start, &val64);
            buffer.get_position() +=
              snprintf(buffer.get_data() + buffer.get_position(), buffer.get_remain(), "%ld", val64);
            break;
          default:
            return OB_ERROR;
            break;
        }
      }
        
      ret = append_delima(buffer, export_param.delima);

      if (ret != OB_SUCCESS) {
        fprintf(stderr, "append delima error, buffer overflow\n");
        return ret;
      }
    }


    while (itr != export_param.columns.end()) {
      ret = recp->get(itr->c_str(), &cell);
      if (ret != OB_SUCCESS) {
        fprintf(stderr, "can't get column [%s]\n", itr->c_str());
        break;
      }

      if (serialize_cell(cell, buffer) < 0) {
        fprintf(stderr, "can't serialize_cell\n");
        break;
      }

      if (itr != export_param.columns.end() - 1) {
        ret = append_delima(buffer, export_param.delima);
        if (ret != OB_SUCCESS) {
          fprintf(stderr, "can't append delima, length=%ld\n", buffer.get_position());
          break;
        }
      }

      itr++;
    }

    ret = append_delima(buffer, export_param.rec_delima);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "can't append record delima, pos=%ld\n", buffer.get_position());
    } else {
      fwrite(buffer.get_data(), buffer.get_position(), 1, out);
    }

    return ret;
}

void scan_and_dump()
{
  OceanbaseDb db(export_param.host, export_param.port, 8 * kDefaultTimeout);
  if (db.init() != OB_SUCCESS) {
    fprintf(stderr, "can't init database, host=%s, port=%d\n", export_param.host, export_param.port);
    return;
  }
  db.set_consistency(export_param.consistency);
  ObDataSet data_set(&db);
  data_set.set_inclusie_start(false);

  int ret = data_set.set_data_source(export_param.table, export_param.columns, 
                                     export_param.start_key, export_param.end_key);
  if (ret != OB_SUCCESS) {
    fprintf(stderr, "can't init data source, please your config\n");
    return;
  }

  FILE *out = NULL;
  if (export_param.output == NULL) {
    out = stdout;
  } else {
    out = fopen(export_param.output, "w");
    if (out == NULL) {
      fprintf(stderr, "can't create output file, %s", export_param.output);
      return;
    }
  }

  DbRecord *recp = NULL;
  while (data_set.has_next()) {
    ret = data_set.get_record(recp);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "get record from data set failed\n");
      break;
    }

    ret = write_record(out, recp);
    if (ret != OB_SUCCESS) {
      fprintf(stderr, "can't write to output file, %s\n", export_param.output);
      break;
    }

    data_set.next();
  }

  fclose(out);
}

int main(int argc, char *argv[])
{
  OceanbaseDb::global_init(NULL, "info");

  int ret = parse_options(argc, argv);
  if (ret != 0) {
    fprintf(stderr, "wrong params is detected, please check it\n");
    usage(argv[0]);
    exit(1);
  }

  export_param.PrintDebug();
  scan_and_dump();
}
