
#include "ob_config.h"
#include "common/ob_mutator.h"
#include "common/ob_tsi_factory.h"

using namespace oceanbase;
using namespace common;

const char* ObConfig::SECTION_STR_RS = "root_server";
const char* ObConfig::SECTION_STR_UPS = "update_server";
const char* ObConfig::SECTION_STR_SCHEMA = "schema";
const char* ObConfig::SECTION_STR_CS = "chunk_server";
const char* ObConfig::SECTION_STR_MS = "merge_server";
const char* ObConfig::SECTION_STR_CLIENT = "client";
const char* ObConfig::SECTION_STR_OBI = "ob_instances";
const char* ObConfig::SECTION_STR_OBCONNECTOR = "obconnector";

ObConfig::ObConfig()
{
}

ObConfig::~ObConfig()
{
}

int ObConfig::load(const char* filename)
{
  int ret = OB_SUCCESS;
  int namelen = 0;
  if (NULL == filename || '\0' == filename[0])
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "config filename is NULL or empty, filename=%p",
              filename);
  }
  else if (OB_MAX_FILE_NAME_LENGTH <= (namelen = static_cast<int> (strlen(filename))))
  {
    ret = OB_BUF_NOT_ENOUGH;
    TBSYS_LOG(ERROR, "filename too long, len=%d", namelen);
  }
  else
  {
    memcpy(config_filename_, filename, namelen+1);
    ret = this->reload();
  }
  return ret;
}

int ObConfig::reload()
{
  int ret = OB_SUCCESS;
  tbsys::CConfig cconf;
  if ('\0' == config_filename_[0])
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "config filename is empty");
  }
  else if (EXIT_FAILURE == cconf.load(config_filename_))
  {
    TBSYS_LOG(ERROR, "failed to load config file, file=%s", 
              config_filename_);
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = this->load_flags(cconf)))
  {
    TBSYS_LOG(ERROR, "reload config error, err=%d file=%s", 
              ret, config_filename_);
  }
  else
  {
    TBSYS_LOG(INFO, "reload config succ, file=%s", config_filename_);
  }
  return ret;
}

const char* ObConfig::get_config_filename() const
{
  return config_filename_;
}

int ObConfig::load_flags(tbsys::CConfig & cconf)
{
  int ret = OB_SUCCESS;

  ObFlag<int>* int_flag = NULL;
  for(int32_t i=0;i<int_flag_array_.count() && OB_SUCCESS == ret;i++)
  {
    int_flag_array_.at(i, int_flag);
    LOAD_FLAG_INT(cconf, (*int_flag));
  }

  ObFlag<int64_t>* int64_flag = NULL;
  for(int32_t i=0;i<int64_flag_array_.count() && OB_SUCCESS == ret;i++)
  {
    int64_flag_array_.at(i, int64_flag);
    LOAD_FLAG_INT64(cconf, (*int64_flag));
  }

  ObFlag<const char*>* const_char_flag = NULL;
  for(int32_t i=0;i<const_char_flag_array_.count() && OB_SUCCESS == ret;i++)
  {
    const_char_flag_array_.at(i, const_char_flag);
    LOAD_FLAG_STRING(cconf, (*const_char_flag));
  }
  return ret;
}

void ObConfig::print() const
{
  ObFlag<int>* int_flag = NULL;
  for(int32_t i=0;i<int_flag_array_.count();i++)
  {
    int_flag_array_.at(i, int_flag);
    g_print_flag(*int_flag);
  }

  ObFlag<int64_t>* int64_flag = NULL;
  for(int32_t i=0;i<int64_flag_array_.count();i++)
  {
    int64_flag_array_.at(i, int64_flag);
    g_print_flag(*int64_flag);
  }

  ObFlag<const char*>* const_char_flag = NULL;
  for(int32_t i=0;i<const_char_flag_array_.count();i++)
  {
    const_char_flag_array_.at(i, const_char_flag);
    g_print_flag(*const_char_flag);
  }
}


