#ifndef __FILE_READER_H__
#define  __FILE_READER_H__

#include "common/file_utils.h"
#include "slice.h"
#include "tokenizer.h"
#include <string>

class RecordBlock {
  public:
    RecordBlock();

    bool next_record(Slice &slice);

    int has_record() const { return curr_rec_ < rec_num_; }

    void reset() { curr_rec_ = 0; offset_ = 0; }

    std::string &buffer() { return buffer_; }

    void set_rec_num(int64_t num) { rec_num_ = num; }

    int64_t get_rec_num() const { return rec_num_; }
  private:
    std::string buffer_;
    int64_t rec_num_;
    int64_t curr_rec_;
    int64_t offset_;
};

class FileReader {
  static const int kReadBufferSize = 10 * 1024;  /* 2M */
  public:
    FileReader(const char *file_name);
    ~FileReader();

    //open file
    int open();

    int read();

    int get_records(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima);

    bool eof() const { return eof_; }

  private:
    void shrink(int64_t pos);

    int extract_record(RecordBlock &block, const RecordDelima &rec_delima, const RecordDelima &col_delima);

    void append_int32(std::string &buffer, uint32_t value);

    void handle_last_record(RecordBlock &block);

    bool is_partial_record(int64_t &pos, const RecordDelima &rec_delima, const RecordDelima &col_delima);

    const char *file_name_;

    char *reader_buffer_;
    int64_t buffer_pos_;
    int64_t buffer_size_;

    bool eof_;

    oceanbase::common::FileUtils file_;
};

#endif
