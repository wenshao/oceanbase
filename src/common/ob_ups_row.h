
#ifndef OCEANBASE_UPS_ROW_H_
#define OCEANBASE_UPS_ROW_H_

#include "common/ob_row.h"

namespace oceanbase
{
  namespace common 
  {
    class ObUpsRow : public ObRow
    {
      public:
        ObUpsRow();
        virtual ~ObUpsRow();

        int assign_to(ObRow &other) const;
        void set_delete_row(bool delete_row);
        bool is_delete_row() const;
        
      private:
        bool is_delete_row_;
    };

    inline void ObUpsRow::set_delete_row(bool delete_row)
    {
      is_delete_row_ = delete_row;
    }

    inline bool ObUpsRow::is_delete_row() const
    {
      return is_delete_row_;
    }
  }
}


#endif /* OCEANBASE_UPS_ROW_H_ */


