#include "ob_bit_set.h"
#include <stdint.h>
using namespace oceanbase::sql;

ObBitSet::ObBitSet()
{
  for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
    bitset_words_[i] = 0;
}

bool ObBitSet::add_member(int32_t index)
{
  if (index < 0)
  {
    TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
    return false;
  }
  if (index > MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
  {
    TBSYS_LOG(ERROR, "bitmap index exceeds the scope");
    return false;
  }
  bitset_words_[index / PER_BITMAPWORD_BITS] |= ((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS));
  return true;
}

bool ObBitSet::del_member(int32_t index)
{
  if (index < 0)
  {
    TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
    return false;
  }
  if (index > MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
    return true;
  bitset_words_[index / PER_BITMAPWORD_BITS] &= ~((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS));
  return true;
}

ObBitSet::BitSetWord ObBitSet::get_bitset_word(int32_t index) const
{
  if (index < 0 || index >= MAX_BITMAPWORD)
  {
    TBSYS_LOG(ERROR, "bitmap word index exceeds the scope");
    return 0;
  }
  return bitset_words_[index];
}

void ObBitSet::add_members(const ObBitSet& other)
{
  for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
  {
    bitset_words_[i] |= other.get_bitset_word(i);
  }
}

bool ObBitSet::has_member(int32_t index) const
{
  if (index < 0)
  {
    TBSYS_LOG(ERROR, "negative bitmapset member not allowed");
    return false;
  }
  if (index > MAX_BITMAPWORD * PER_BITMAPWORD_BITS)
  {
    TBSYS_LOG(ERROR, "bitmap index exceeds the scope");
    return false;
  }
  return ((bitset_words_[index / PER_BITMAPWORD_BITS]
      & ((BitSetWord) 1 << (index % PER_BITMAPWORD_BITS))) != 0);
}

bool ObBitSet::is_empty() const
{
	for (int32_t i = 0; i < MAX_BITMAPWORD; i++)
	{
		if (bitset_words_[i] != 0)
			return false;
	}
	return true;
}

bool ObBitSet::is_superset(const ObBitSet& other) const
{
  bool ret = true;
  if (other.is_empty())
    return true;
  else if (is_empty())
    return false;
  else
  {
    for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
    {
      if ((other.get_bitset_word(i)) & ~(bitset_words_[i]))
        return false;
    }
  }
  return ret;
}

bool ObBitSet::is_subset(const ObBitSet& other) const
{
  bool ret = true;
  if (is_empty())
    ret = true;
  else if (other.is_empty())
    ret = false;
  else
  {
    for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
    {
      if (bitset_words_[i] & ~(other.get_bitset_word(i)))
        return false;
    }
  }
  return ret;
}

void ObBitSet::clear()
{
  for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
  {
    memset(&bitset_words_[i], 0, sizeof(BitSetWord));
  }
}

int32_t ObBitSet::num_members() const
{
  int32_t num = 0;
  BitSetWord word;

  for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
  {
    word = get_bitset_word(i);
    if (!word)
      break;
    word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
    word = (word & UINT32_C(0x33333333)) + ((word >> 1) & UINT32_C(0x33333333));
    word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 1) & UINT32_C(0x0f0f0f0f));
    word = (word & UINT32_C(0x00ff00ff)) + ((word >> 1) & UINT32_C(0x00ff00ff));
    word = (word & UINT32_C(0x0000ffff)) + ((word >> 1) & UINT32_C(0x0000ffff));
    num += (int32_t)word;
  }
  return num;
}

ObBitSet::ObBitSet(const ObBitSet &other)
{
  *this = other;
}

ObBitSet& ObBitSet::operator=(const ObBitSet &other)
{
  for (int32_t i = 0; i < MAX_BITMAPWORD; i ++)
  {
    this->bitset_words_[i] = other.get_bitset_word(i);
  }
  return *this;
}
