#include "ob_prefetch_data.h"
#include "ob_scanner.h"

using namespace oceanbase::common;

ObPrefetchData::ObPrefetchData()
{
}

ObPrefetchData::~ObPrefetchData()
{
}

void ObPrefetchData::reset(void)
{
  data_.reset();
}

bool ObPrefetchData::check_cell(const ObCellInfo & cell)
{
  return (OB_INVALID_ID != cell.table_id_) && (OB_INVALID_ID != cell.column_id_);
}

bool ObPrefetchData::is_empty(void) const
{
  return data_.is_empty();
}

int ObPrefetchData::add_cell(const ObCellInfo & cell)
{
  int ret = OB_SUCCESS;
  if (false == check_cell(cell))
  {
    TBSYS_LOG(WARN, "check cell is not id type:table[%lu], column[%lu]",
        cell.table_id_, cell.column_id_);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = data_.add_cell(cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add cell failed:ret[%d]", ret);
    }
  }
  return ret;
}

ObScanner & ObPrefetchData::get(void)
{
  return data_;
}

const ObScanner & ObPrefetchData::get(void) const
{
  return data_;
}

DEFINE_GET_SERIALIZE_SIZE(ObPrefetchData)
{
  return data_.get_serialize_size();
}

DEFINE_SERIALIZE(ObPrefetchData)
{
  return data_.serialize(buf, buf_len, pos);
}

DEFINE_DESERIALIZE(ObPrefetchData)
{
  return data_.deserialize(buf, data_len, pos);
}

