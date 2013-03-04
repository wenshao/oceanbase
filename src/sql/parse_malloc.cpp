#include <string.h>
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

/* get memory from the thread obStringBuf, and not release untill thread quits */
void *parse_malloc(const size_t nbyte, void *malloc_pool)
{
  ObStringBuf* alloc_buf = static_cast<ObStringBuf*>(malloc_pool);
  if (alloc_buf == NULL)
  {
    TBSYS_LOG(ERROR, "parse_malloc gets string buffer error");
    return NULL;
  }

  /* ^_^ do not want to add any code to ob_string_buf */
  /* max len of one allocation is 2M, which is same as DEF_MEM_BLOCK_SIZE*/
  const size_t PARSER_MAX_MALLOC_SIZE = 2 * 1024L * 1024L;
  size_t headlen = sizeof(int64_t);
  // We leave 8 byte for malloc sizeo
  if (nbyte > PARSER_MAX_MALLOC_SIZE - headlen)
  {
    TBSYS_LOG(ERROR, "parse_malloc exceeds the max size");
    return NULL;
  }
  char str[PARSER_MAX_MALLOC_SIZE];
  ObString tmp_string(PARSER_MAX_MALLOC_SIZE, static_cast<int32_t>(nbyte + headlen), str);
  ObString res_string;
  if (alloc_buf->write_string(tmp_string, &res_string) != OB_SUCCESS)
    return NULL;
  char *ptr = res_string.ptr();
  *((int64_t *)ptr) = nbyte;
  return (void *)(ptr + headlen);
}

void *parse_realloc(void *ptr, size_t nbyte, void *malloc_pool)
{
  ObStringBuf* alloc_buf = static_cast<ObStringBuf*>(malloc_pool);
  if (alloc_buf == NULL)
  {
    TBSYS_LOG(ERROR, "parse_malloc gets string buffer error");
    return NULL;
  }

  /* ^_^ do not want to add any code to ob_string_buf */
  /* max len of one allocation is 2M, which is same as DEF_MEM_BLOCK_SIZE*/
  const size_t PARSER_MAX_MALLOC_SIZE = 2 * 1024L * 1024L;
  size_t headlen = sizeof(int64_t);
  // We leave 8 byte for malloc sizeo
  if (nbyte + headlen > PARSER_MAX_MALLOC_SIZE)
  {
    TBSYS_LOG(ERROR, "parse_realloc exceeds the max size");
    return NULL;
  }

  int64_t size = *(int64_t *)((char *)ptr - headlen);
  char str[PARSER_MAX_MALLOC_SIZE];
  char *newptr = str;
  *((int64_t *)newptr) = nbyte;
  memmove(newptr + headlen, ptr, size <= static_cast<int64_t>(nbyte) ? size : nbyte);
  ObString tmp_string(PARSER_MAX_MALLOC_SIZE, static_cast<int32_t>(nbyte + sizeof(int64_t)), str);
  ObString res_string;
  if (alloc_buf->write_string(tmp_string, &res_string) != OB_SUCCESS)
    return NULL;
  parse_free((void *)((char *)ptr - headlen));
  return (void *)(res_string.ptr() + headlen);
}

char *parse_strdup(const char *str, void *malloc_pool)
{
  if (!str)
    return NULL;

  ObStringBuf* alloc_buf = static_cast<ObStringBuf*>(malloc_pool);
  if (alloc_buf == NULL)
  {
    TBSYS_LOG(ERROR, "parse_malloc gets string buffer error");
    return NULL;
  }

  /* ^_^ do not want to add any code to ob_string_buf */
  /* max len of one allocation is 2M, which is same as DEF_MEM_BLOCK_SIZE*/
  const size_t PARSER_MAX_MALLOC_SIZE = 2 * 1024L * 1024L;
  size_t len = strlen(str);
  size_t headlen = sizeof(int64_t);
  if (len + headlen + 1 > PARSER_MAX_MALLOC_SIZE)
  {
    TBSYS_LOG(ERROR, "parse_strdup exceeds the max size");
    return NULL;
  }
  char tmp_str[PARSER_MAX_MALLOC_SIZE];
  char *ptr = tmp_str;
  *((int64_t *)ptr) = len + 1;
  memmove(ptr + headlen, str, len + 1);
  int32_t old_str_len = static_cast<int32_t>(headlen + len + 1);
  ObString old_str(old_str_len, old_str_len, ptr);
  ObString new_str;
  if (alloc_buf->write_string(old_str, &new_str) != OB_SUCCESS)
    return NULL;

  char *new_string = new_str.ptr();
  *(new_string + headlen + len) = '\0';
  return new_string + headlen;
}

void parse_free(void *ptr)
{
  UNUSED(ptr);
  /* do nothing, we don't really free the memory */
}
