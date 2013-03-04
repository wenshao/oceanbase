#include "ob_import_comsumer.h"
#include "slice.h"

#include <string>
using namespace std;

ImportComsumer::ImportComsumer(oceanbase::api::OceanbaseDb *db, ObRowBuilder *builder, const TableParam &param) : param_(param)
{
  db_ = db;
  builder_ = builder;
  assert(db_ != NULL);
  assert(builder_ != NULL);
  bad_file_ = NULL;
}

int ImportComsumer::init()
{
  int ret = OB_SUCCESS;
  if (param_.bad_file_ != NULL) {
    TBSYS_LOG(INFO, "using bad file name = %s", param_.bad_file_);
    ret = AppendableFile::NewAppendableFile(param_.bad_file_, bad_file_);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't create appendable file %s", param_.bad_file_);
    }
  }

  return ret;
}

ImportComsumer::~ImportComsumer()
{
  if (bad_file_ != NULL) {
    delete bad_file_;
  }
}

int ImportComsumer::write_bad_record(RecordBlock &rec)
{
  Slice slice;

  size_t rec_delima_len = param_.rec_delima.length();
  size_t delima_len = param_.delima.length();
  char delima_buf[4];
  int ret = OB_SUCCESS;

  if (bad_file_ == NULL) {
    return 0;
  }

  param_.rec_delima.append_delima(delima_buf, 0, 4);
  rec.reset();
  while (rec.next_record(slice)) {
    if (delima_len > slice.size()) {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "empty or currupted row meet, skiping");
      continue;
    }

    if(bad_file_->Append(slice.data(), slice.size() - delima_len) != OB_SUCCESS ||
       bad_file_->Append(delima_buf, rec_delima_len) != OB_SUCCESS) {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "can't write to bad_file name = %s", param_.bad_file_);
      break;
    }
  }

  return ret;
}

int ImportComsumer::comsume(RecordBlock &obj)
{
    Slice slice;

    DbTranscation *tnx = NULL;
    int ret = db_->start_tnx(tnx);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't create new transcation");
    }

    if (ret == OB_SUCCESS) {
      ret = builder_->build_tnx(obj, tnx);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "ObRowBuilder can't build tnx");
      }
    }

    if (ret == OB_SUCCESS) {
      ret = tnx->commit();
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "commit transcation error");
      }
    } else {
      TBSYS_LOG(ERROR, "error ocurrs, so aborting transcation");
      tnx->abort();                             /* abort always success */
    }

    db_->end_tnx(tnx);

    if (ret != OB_SUCCESS) {
      int err = write_bad_record(obj);
      if (err != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't write bad record to file");
      }
    }

    return ret;
}
