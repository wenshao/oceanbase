#include <stdarg.h>
#include "common/utility.h"
#include "ob_utils.h"

const char* obj_type_repr(const ObObjType _type)
{
  const char* type_name[] = {"null", "int", "float", "double", "datetime", "precisedatetime", "varchar", "seq", "createtime", "modifytime", "extendtype"};
    // enum ObObjType
    // {
    //   ObMinType = -1,
    //   ObNullType,   // 空类型
    //   ObIntType,
    //   ObFloatType,
    //   ObDoubleType,
    //   ObDateTimeType,
    //   ObPreciseDateTimeType,
    //   ObVarcharType,
    //   ObSeqType,
    //   ObCreateTimeType,
    //   ObModifyTimeType,
    //   ObExtendType,
    //   ObMaxType,
    // };
  return (_type > ObMinType && _type < ObMaxType) ? type_name[_type]: "unknown";
}
int to_obj(ObObj& obj, const int64_t v)
{
  int err = OB_SUCCESS;
  obj.reset();
  obj.set_int(v);
  return err;
}

int to_obj(ObObj& obj, const ObString& v)
{
  int err = OB_SUCCESS;
  obj.reset();
  obj.set_varchar(v);
  return err;
}

int to_obj(ObObj& obj, const char* v)
{
  int err = OB_SUCCESS;
  ObString _v;
  int64_t max_str_len = 1024;
  _v.assign_ptr((char*)v, v? static_cast<ObString::obstr_size_t> (strnlen(v, max_str_len)): 0);
  obj.reset();
  obj.set_varchar(_v);
  return err;
}
    
int alloc_str(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* _str)
{
  int err = OB_SUCCESS;
  int64_t old_pos = pos;
  if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", _str)))
  {
    TBSYS_LOG(ERROR, "strformat(buf=%p, len=%ld, pos=%ld, _str=%s)=>%d", buf, len, pos, _str, err);
  }
  else
  {
    str.assign_ptr(buf + old_pos, static_cast<ObString::obstr_size_t>(pos - old_pos));
    pos++;
  }
  return err;
}

int alloc_str(char* buf, const int64_t len, int64_t& pos, ObString& str, const ObString _str)
{
  int err = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  UNUSED(str);
  UNUSED(_str);
  err = OB_NOT_SUPPORTED;
  return err;
}

int rand_str(char* str, int64_t len)
{
  int err = OB_SUCCESS;
  for(int64_t i = 0; i < len; i++)
  {
    str[i] = static_cast<char> ('a' + random()%26);
  }
  str[len-1] = 0;
  return err;
}

int rand_str(char* str, int64_t len, int64_t seed)
{
  int err = OB_SUCCESS;
  for(int64_t i = 0; i < len; i++)
  {
    str[i] = static_cast<char> ('a' + (seed = rand2(seed))%26);
  }
  str[len-1] = 0;
  return err;
}

int vstrformat(char* buf, const int64_t len, int64_t& pos, const char* format, va_list ap)
{
  int err = OB_SUCCESS;
  int64_t count = 0;
  if (NULL == buf || 0 > len || len < pos)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "strformat(buf=%s, len=%ld, pos=%ld, format='%s')=>%d", buf, len, pos, format, err);
  }
  else if (len - pos <= 0)
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "strformat(buf=%s, len=%ld, pos=%ld, format='%s')=>%d", buf, len, pos, format, err);
  }
  else
  {
    if (0 > (count = vsnprintf(buf + pos, len - pos - 1, format, ap)) || count > len - pos -1)
    {
      err = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(ERROR, "strformat(buf=%s, len=%ld, pos=%ld, format='%s')=>[count=%ld,err=%d]",
                buf, len, pos, format, count, err);
    }
  }
  
  if (OB_SUCCESS == err)
  {
    pos += count;
  }
  return err;
}

int strformat(char* buf, const int64_t len, int64_t& pos, const char* format, ...)
{
  int err = OB_SUCCESS;
  va_list ap;
  va_start(ap, format);
  err = vstrformat(buf, len, pos, format, ap);
  va_end(ap);
  return err;
}

int bin2hex(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* data, const int64_t size)
{
  int err = OB_SUCCESS;
  int32_t count = 0;
  char* char_ptr = buf + pos;
  if (0 >= (count = hex_to_str(data, static_cast<int32_t> (size),
	buf + pos, static_cast<int32_t> (len - pos - 1))))
  {}
  else
  {
    count *= 2;
    pos += count;
    buf[pos] = 0;
    str.assign_ptr(char_ptr, count);
  }
  return err;
}

int hex2bin(char* buf, const int64_t len, int64_t& pos, ObString& str, const char* data, const int64_t size)
{
  int err = OB_SUCCESS;
  int32_t count = 0;
  char* char_ptr = buf + pos;
  if (0 >= (count = str_to_hex(data, static_cast<int32_t> (size), buf + pos, static_cast<int32_t>(len - pos -1))))
  {}
  else
  {
    count /= 2;
    pos += count;
    buf[pos] = 0;
    str.assign_ptr(char_ptr, count);
  }
  return err;
}

int bin2hex(ObDataBuffer& buf, ObString& str, const char* data, const int64_t size)
{
  return bin2hex(buf.get_data(), buf.get_capacity(), buf.get_position(), str, data, size);
}

int hex2bin(ObDataBuffer& buf, ObString& str, const char* data, const int64_t size)
{
  return hex2bin(buf.get_data(), buf.get_capacity(), buf.get_position(), str, data, size);
}

int time_format(char* buf, const int64_t len, int64_t& pos, const int64_t time_us, const char *format)
{
  int err = OB_SUCCESS;
  int64_t count = 0;
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  if (NULL == buf || NULL == format)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "time_format(buf=%p, format=%p)=>%d", buf, format, err);
  }
  else if(NULL == localtime_r(&time_s, &time_struct))
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "localtime(time_us=%ld)=>NULL", time_us);
  }
  else if (len - pos <(count = strftime(buf + pos, len - pos, format, &time_struct)))
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "strftime(buf_size=%ld, format='%s')=>%d", len-pos, format, err);
  }
  return err;
}

#if 0
const char* DEFAULT_TIME_FORMAT2 = "%Y-%m-%d %H:%M:%S";
const char* time2str(const int64_t time_us, const char* format=DEFAULT_TIME_FORMAT);
const char* time2str(const int64_t time_us, const char* format)
{
  static __thread char buf[128];
  int err = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_SUCCESS != (err = time_format(buf, sizeof(buf), pos, time_us, format)))
  {
    TBSYS_LOG(ERROR, "time_format()=>%d", err);
  }
  return OB_SUCCESS == err? buf: NULL;
}
#endif

int repr(char* buf, const int64_t len, int64_t& pos, const ObObj& value)
{
  int err = OB_SUCCESS;
  ObObjType _type = value.get_type();
  int64_t int_value = 0;
  ObCreateTime create_time = 0;
  ObModifyTime modify_time = 0;
  ObDateTime datetime = 0;
  ObPreciseDateTime precise_datetime = 0;
  float float_value = 0.0;
  double double_value = 0.0;
  ObString vchar_value;
  //str.clear();
  switch(_type)
  {
    case ObNullType:
      if (OB_SUCCESS != (err = strformat(buf, len, pos, "null")))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObIntType:
      if (OB_SUCCESS != (err = value.get_int(int_value)))
      {
        TBSYS_LOG(ERROR, "value.get_int()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%ld", int_value)))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObVarcharType:
      if (OB_SUCCESS != (err = value.get_varchar(vchar_value)))
      {
        TBSYS_LOG(ERROR, "value.get_varchar()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "\'%.*s\'", vchar_value.length(), vchar_value.ptr())))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObFloatType:
      if (OB_SUCCESS != (err = value.get_float(float_value)))
      {
        TBSYS_LOG(ERROR, "value.get_float()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%.4f", float_value)))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObDoubleType:
      if (OB_SUCCESS != (err = value.get_double(double_value)))
      {
        TBSYS_LOG(ERROR, "value.get_float()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%.4lf", double_value)))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObDateTimeType:
      if (OB_SUCCESS != (err = value.get_datetime(datetime)))
      {
        TBSYS_LOG(ERROR, "value.get_datetime()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", time2str(datetime))))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObPreciseDateTimeType:
      if (OB_SUCCESS != (err = value.get_precise_datetime(precise_datetime)))
      {
        TBSYS_LOG(ERROR, "value.get_precise_datetime()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", time2str(precise_datetime))))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObCreateTimeType:
      if (OB_SUCCESS != (err = value.get_createtime(create_time)))
      {
        TBSYS_LOG(ERROR, "value.get_createtime()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", time2str(create_time))))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObModifyTimeType:
      if (OB_SUCCESS != (err = value.get_modifytime(modify_time)))
      {
        TBSYS_LOG(ERROR, "value.get_modifytime()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", time2str(modify_time))))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObSeqType:
      if (OB_SUCCESS != (err = strformat(buf, len, pos, "[seq]")))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    case ObExtendType:
      if (OB_SUCCESS != (err = value.get_ext(int_value)))
      {
        TBSYS_LOG(ERROR, "value.get_int()=>%d", err);
      }
      else if (OB_SUCCESS != (err = strformat(buf, len, pos, "[%ld]", int_value)))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
    default:
      if (OB_SUCCESS != (err = strformat(buf, len, pos, "obj[type=%d]", _type)))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
      break;
  }
  return err;
}

int repr(char* buf, const int64_t len, int64_t& pos, const char* _str)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s", _str)))
  {
    TBSYS_LOG(ERROR, "strformat(_str=%p)=>%d", &_str, err);
  }
  return err;
}

int copy_str(char* buf, const int64_t len, int64_t& pos, char*& str, const char* _str)
{
  str = buf + pos;
  return repr(buf, len, pos, _str);
}

int repr(char* buf, const int64_t len, int64_t& pos, const ObString& _str)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = strformat(buf, len, pos, "%.*s", _str.length(), _str.ptr())))
  {
    TBSYS_LOG(ERROR, "strformat(_str=%p)=>%d", &_str, err);
  }
  return err;
}

int repr(char* buf, const int64_t len, int64_t& pos, ObScanner& scanner, int64_t row_limit /*=-1*/)
{
  int err = OB_SUCCESS;
  int64_t row_count = 0;
  ObCellInfo* cell_info;
  ObString rowkey;
  bool is_row_changed = false;

  UNUSED(row_limit);
  //err = strformat(buf, len, pos, "scanner[%p]", &scanner);
  while(OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = scanner.next_cell()) && OB_ITER_END != err)
    {
      TBSYS_LOG(ERROR, "scanner.next_cell()=>%d", err);
    }
    else if (OB_ITER_END == err)
    {}
    else if (OB_SUCCESS != (err = scanner.get_cell(&cell_info, &is_row_changed)))
    {
      TBSYS_LOG(ERROR, "scanner.get_cell()=>%d", err);
    }
    else if (is_row_changed && ++row_count
             && !(OB_SUCCESS == (err = strformat(buf, len, pos, "\n"))
                  && OB_SUCCESS == (err = bin2hex(buf, len, pos, rowkey, cell_info->row_key_.ptr(), cell_info->row_key_.length()))
                  && OB_SUCCESS == (err = strformat(buf, len, pos, " "))))
    {}
    else if (OB_SUCCESS != (err = repr(buf, len, pos, cell_info->column_name_)))
    {
      TBSYS_LOG(ERROR, "repr()=>%d", err);
    }
    else if (OB_SUCCESS != (err = repr(buf, len, pos, "=")))
    {
      TBSYS_LOG(ERROR, "repr('\t')=>%d", err);
    }
    else if (OB_SUCCESS != (err = repr(buf, len, pos, cell_info->value_)))
    {
      TBSYS_LOG(ERROR, "repr()=>%d", err);
    }
    else if (OB_SUCCESS != (err = repr(buf, len, pos, "\t")))
    {
      TBSYS_LOG(ERROR, "repr('\t')=>%d", err);
    }
  }
  if (row_count <= 0)
  {
    repr(buf, len, pos, "\nnothing read!\n");
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int to_server(ObServer& server, const char* spec)
{
  int err = OB_SUCCESS;
  char* p = NULL;
  char ip[64] = "";
  int32_t port = 0;
  if (NULL == spec)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "spec == NULL");
  }
  else if (NULL == (p = strchr(const_cast<char*> (spec), ':')))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "strchr(spec='%s', ':')=>NULL", spec);
  }
  else 
  {
    strncpy(ip, spec, min(p - spec, (int64_t)sizeof(ip)));
    port = atoi(p+1);
  }
  if (OB_SUCCESS != err)
  {}
  else if (0 >= port)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "to_server(spec=%s)=>%d", spec, err);
  }
  else
  {
    TBSYS_LOG(DEBUG, "to_server(ip=%s, port=%d)=>%d", ip, port, err);
    server.set_ipv4_addr(ip, port);
  }
  
  return err;
}

int split(char* buf, const int64_t len, int64_t& pos, const char* str, const char* delim,
          int max_n_secs, int& n_secs, char** secs)
{
  int err = OB_SUCCESS;
  ObString _str;
  char* saveptr = NULL;
  int i = 0;
  if (NULL == buf || NULL == str || NULL == delim || 0 >= max_n_secs || NULL == secs)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "split(buf='%s', str='%s', delim='%s', max_n_secs='%d', secs=%p)=>%d",
              buf, str, delim, max_n_secs, secs, err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, len, pos, _str, str)))
  {
    TBSYS_LOG(ERROR, "alloc_str(buf=%p, len=%ld, pos=%ld, str='%s')=>%d", buf, len, pos, str, err);
  }
  else if (NULL == (secs[0] = strtok_r(_str.ptr(), delim, &saveptr)))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "strtok_r(str='%s')=>NULL", _str.ptr());
  }
  else
  {
    for(i = 1; i < max_n_secs; i++)
    {
      if (NULL == (secs[i] = strtok_r(NULL, delim, &saveptr)))
      {
        break;
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    n_secs = i;
  }
  return err;
}

int parse_range(char* buf, const int64_t len, int64_t& pos, const char* _range, char*& start, char*& end)
{
  int err = OB_SUCCESS;
  char* saveptr = NULL;
  char* range = NULL;
  const char* delim = ", ";
  if (NULL == buf || NULL == _range)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "parse_range(buf=%p[%ld], range=%s): INVALID_ARGUMENT", buf, len, _range);
  }
  else if (OB_SUCCESS != (err = copy_str(buf, len, pos, range, _range)))
  {
    TBSYS_LOG(ERROR, "alloc_str(buf=%p, len=%ld, pos=%ld, str='%s')=>%d", buf, len, pos, _range, err);
  }
  else if ('[' != range[0] || ']' != range[strlen(range)-1])
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "ill formated range: %s", range);
  }
  else if (NULL == (start = strtok_r(range + 1, delim, &saveptr)))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "strtok_r(str='%s')=>NULL", range);
  }
  else if (NULL == (end = strtok_r(NULL, delim, &saveptr)))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "strtok_r(str='%s')=>NULL", range);
  }
  else
  {
    end[strlen(end)-1] = 0;
  }
  return err;
}

int parse_range(ObDataBuffer& buf, const char* _range, char*& start, char*& end)
{
  return parse_range(buf.get_data(), buf.get_capacity(), buf.get_position(), _range, start, end);
}

int strformat(ObDataBuffer& buf, const char* format, ...)
{
  int err = OB_SUCCESS;
  va_list ap;
  va_start(ap, format);
  err = vstrformat(buf.get_data(), buf.get_capacity(), buf.get_position(), format, ap);
  va_end(ap);
  return err;
}

int split(ObDataBuffer& buf, const char* str, const char* delim, const int max_n_secs, int& n_secs, char** const secs)
{
  return split(buf.get_data(), buf.get_capacity(), buf.get_position(), str, delim, max_n_secs, n_secs, secs);
}

int parse_servers(const char* tablet_servers, const int max_n_servers, int& n_servers, ObServer *servers)
{
  int err = OB_SUCCESS;
  char server_spec_buf[MAX_STR_BUF_SIZE];
  ObDataBuffer buf(server_spec_buf, sizeof(server_spec_buf));
  char* server_specs[max_n_servers];
  int server_count = 0;
  if (NULL == tablet_servers || NULL == servers || 0 >= max_n_servers)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "parse_servers(table_server=%s, max_n_servers=%d, servers=%p)=>%d",
              tablet_servers, max_n_servers, servers, err);
  }
  else if ((int)ARRAYSIZEOF(server_specs) > max_n_servers)
  {
    err = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "ARRAYSIZEOF(server_specs)[%ld] > max_n_servers[%d]", (int64_t)ARRAYSIZEOF(server_specs), max_n_servers);
  }
  else if (OB_SUCCESS != (err = split(buf, tablet_servers, ", ", max_n_servers, server_count, server_specs)))
  {
    TBSYS_LOG(ERROR, "split(tablet_servers='%s', max_n_servers=%d)=>%d", tablet_servers, max_n_servers, err);
  }
  else
  {
    for(int i = 0; OB_SUCCESS == err && i < server_count; i++)
    {
      if (OB_SUCCESS != (err = to_server(servers[i], server_specs[i])))
      {
        TBSYS_LOG(ERROR, "to_server(spec='%s')=>%d", server_specs[i], err);
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    n_servers = server_count;
  }
  return err;
}

int64_t rand2(int64_t h)
{
  if (0 == h) return 1;
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccd;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53;
  h ^= h >> 33;
  return h;
}
void init_rng64(rng_t* state, uint64_t x)
{
  state->s[0] = 0;
  state->s[1] = 0;
  state->s[2] = x;
}

uint64_t rng64(rng_t *_s)
{
  uint64_t c = 7319936632422683419ULL;
  uint64_t* s = _s->s;
  uint64_t x = s[1];
  
  /* Increment 128bit counter */
  s[0] += c;
  s[1] += c + (s[0] < c);
  
  /* Two h iterations */
  x ^= (x >> 32) ^ s[2];
  x *= c;
  x ^= x >> 32;
  x *= c;
  
  /* Perturb result */
  return x + s[0];
}

#ifdef __TEST_OB_UTILS__
int main(int argc, char *argv[])
{
  return 0;
}
#endif
