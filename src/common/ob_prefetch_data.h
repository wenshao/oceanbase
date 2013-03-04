#ifndef OCEANBASE_PREFETCH_DATA_H_
#define OCEANBASE_PREFETCH_DATA_H_

#include "ob_define.h"
#include "ob_scanner.h"

namespace oceanbase
{
  namespace common
  {
    class ObPrefetchData
    {
    public:
      ObPrefetchData();
      virtual ~ObPrefetchData();

    private:
      /// cell cell is id type
      bool check_cell(const ObCellInfo & cell);

    public:
      /// is empty no data
      bool is_empty(void) const;
      /// reset prefetch data
      void reset(void);
      /// add cell
      int add_cell(const ObCellInfo & cell);

      /// get prefetch data
      const ObScanner & get(void) const;
      ObScanner & get(void);
      
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      ObScanner data_;
    };
  }
}


#endif // OCEANBASE_PREFETCH_DATA_H_
