
#include "ob_ups_row.h"
using namespace oceanbase::common;

ObUpsRow::ObUpsRow() 
  : ObRow(),
  is_delete_row_(false)
{
}

ObUpsRow::~ObUpsRow()
{
}

