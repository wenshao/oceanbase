#ifndef OCEANBASE_SQL_BITMAPSET_H_
#define OCEANBASE_SQL_BITMAPSET_H_

#include <stdint.h>
#include "common/utility.h"

namespace oceanbase
{
  namespace sql
  {
    class ObBitSet
    {
    public:
      typedef uint32_t BitSetWord;

      ObBitSet();
      virtual ~ObBitSet() {}

      bool add_member(int32_t index);
      bool del_member(int32_t index);
      bool has_member(int32_t index) const;
      bool is_empty() const;
      bool is_subset(const ObBitSet& other) const;
      bool is_superset(const ObBitSet& other) const;
      void add_members(const ObBitSet& other);
      void clear();
      int32_t num_members() const;

      BitSetWord get_bitset_word(int32_t index) const;

      ObBitSet(const ObBitSet &other);
      ObBitSet& operator=(const ObBitSet &other); 

    private:
      static const int32_t PER_BITMAPWORD_BITS = 32;
      // Now we only need bitmap to remenber tables in single level query,
      // and the number of tables won't be too big, we thing 8*32 = 256 is enough.
      // If someday bitman is used other way, and it has to break this scope,
      // a new dynamic allocated space will be needed.
      static const int32_t MAX_BITMAPWORD = 8;

      BitSetWord bitset_words_[MAX_BITMAPWORD];
    };
  }
}

#endif //OCEANBASE_SQL_BITMAPSET_H_


