#include "ob_import_producer.h"

#include "ob_import.h"
#include "tokenizer.h"
#include "slice.h"

using namespace oceanbase::common;

int ImportProducer::init()
{
  return reader_.open();
}

int ImportProducer::produce(RecordBlock &obj)
{
  int ret = ComsumerQueue<RecordBlock>::QUEUE_SUCCESS;

  if (!reader_.eof()) {
    if (reader_.get_records(obj, rec_delima_, delima_) != 0) {
      TBSYS_LOG(ERROR, "can't get record");
      ret = ComsumerQueue<RecordBlock>::QUEUE_ERROR;
    }
  } else {
    ret = ComsumerQueue<RecordBlock>::QUEUE_QUITING;
  }

  return ret;
}
