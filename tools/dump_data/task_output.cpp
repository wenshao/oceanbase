#include "common/ob_define.h"
#include "task_output.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

TaskOutput::TaskOutput()
{
}

TaskOutput::~TaskOutput()
{
}

int64_t TaskOutput::size(void) const
{
  return file_list_.size();
}

int TaskOutput::add(const uint64_t task_id, const int64_t peer_id, const string & file)
{
  int ret = OB_SUCCESS;
  OutputInfo info;
  info.peer_id_= peer_id;
  info.file_ = file;
  file_list_.insert(std::pair<uint64_t, OutputInfo>(task_id, info));
  return ret;
}

int TaskOutput::print(FILE * output)
{
  int ret = OB_SUCCESS;
  if (NULL == output)
  {
    ret = OB_ERROR;
  }
  else
  {
    int64_t count = 0;
    uint32_t ip = 0;
    unsigned char * bytes = NULL;
    map<uint64_t, OutputInfo>::iterator it;
    for (it = file_list_.begin(); it != file_list_.end(); ++it)
    {
      ip = (uint32_t)(it->second.peer_id_ & 0xffffffff);
      bytes = (unsigned char *) &ip;
      fprintf(output, "%d.%d.%d.%d:%s\n", bytes[0], bytes[1], bytes[2], bytes[3],
          it->second.file_.c_str());
      ++count;
    }
  }
  return ret;
}

