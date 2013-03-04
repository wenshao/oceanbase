#include <string.h>
#include "tbsys.h"
#include "common/ob_result.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_schema.h"
#include "task_server.h"
#include "task_packet.h"
#include "task_server_param.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::tools;

TaskServer::TaskServer(const char * result_file, const int64_t timeout_times,
  const int64_t max_count, const int64_t timeout, const ObServer & root_server)
  :task_manager_(timeout_times, max_count)
{
  status_ = INVALID_STAT;
  result_file_ = result_file;
  timeout_ = timeout;
  memtable_version_ = -1;
  modify_timestamp_= -1;
  root_server_ = root_server;
}

int TaskServer::init(const int64_t thread, const int64_t queue, RpcStub * rpc,
  const char * dev, const int32_t port)
{
  int ret = OB_SUCCESS;
  if ((INVALID_STAT != status_) || (NULL == rpc) || (NULL == dev) || (port <= 1024))
  {
    TBSYS_LOG(ERROR, "check status or rpc or dev or port failed:"
        "status[%d], rpc[%p], dev[%s], port[%d]", status_, rpc, dev, port);
    ret = OB_ERROR;
  }
  else
  {
    rpc_ = rpc;
    set_dev_name(dev);
    set_listen_port(port);
    set_batch_process(false);
    ret = set_default_queue_size(queue);
    if (OB_SUCCESS == ret)
    {
      ret = set_thread_count(thread);
      if (OB_SUCCESS == ret)
      {
        status_ = PREPARE_STAT;
      }
    }
  }
  return ret;
}

int TaskServer::init_service(void)
{
  int ret = OB_SUCCESS;
  if (status_ != PREPARE_STAT)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "check service status failed:status[%d]", status_);
  }
  else
  {
#if 1
    // step 1. fetch schema and check table name
    ret = rpc_->get_schema(root_server_, timeout_, 0, schema_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "get newest schema failed:ret[%d]", ret);
    }
#else
    {
      tbsys::CConfig cc1;
      bool succ = schema_.parse_from_file("schema.ini", cc1);
      if (!succ) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "can't load schema from file");
      }
    }
#endif
  }

  ObServer update_server;
  // step 2. get update server addr
  if (OB_SUCCESS == ret)
  {
    ret = rpc_->get_update_server(root_server_, timeout_, update_server);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get update server failed:ret[%d]", ret);
    }
    else
    {
      const int32_t MAX_SERVER_ADDR_SIZE = 128;
      char server_addr[MAX_SERVER_ADDR_SIZE];
      update_server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
      TBSYS_LOG(INFO, "get update server succ:server[%s]", server_addr);
    }
  }

  // step 3. get memtable version
  if (OB_SUCCESS == ret)
  {
    ret = rpc_->get_version(update_server, timeout_, memtable_version_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get memtable version failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "get memtable version succ:version[%ld]", memtable_version_);
    }
  }
  
  // step 4. init task factory
  if (OB_SUCCESS == ret)
  {
    ret = task_factory_.init(memtable_version_, timeout_, root_server_, &schema_,
        rpc_, &task_manager_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "factory init failed:ret[%d]", ret);
    }
  }
  
  // step 5. get all the tablets
  if (OB_SUCCESS == ret)
  {
    uint64_t count = 0;
    ret = task_factory_.get_all_tablets(count);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get all tablet failed:ret[%d]", ret);
    }
    else if (true == task_manager_.check_tasks(count))
    {
      ret = task_factory_.setup_tablets_version() ;
      if (OB_SUCCESS == ret)
      {
        status_ = READY_STAT;
        TBSYS_LOG(INFO, "get all tablet succ:count[%lu]", count);
      }
    }
    else
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "check all tablet failed:count[%lu]", count);
    }
  }
  return ret;
}


int TaskServer::do_request(ObPacket* base_packet)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if ((OB_SUCCESS == ret) && (READY_STAT == status_) && check_inner_stat())
  {
    switch (packet_code)
    {
    case FETCH_TASK_REQUEST:
      {
        ret = handle_fetch_task(ob_packet);
        break;
      }
    case REPORT_TASK_REQUEST:
      {
        ret = handle_finish_task(ob_packet);
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code:%d", packet_code);
        break;
      }
    }
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check handle failed:ret[%d]", ret);
    }
  }
  else if (FINISH_STAT == status_)
  {
    TBSYS_LOG(INFO, "all task finished succ:tablet_count[%lu]", task_manager_.get_count());
    stop();
  }
  else
  {
    TBSYS_LOG(WARN, "check inner stat failed:ret[%d]", ret);
  }
  return ret;
}


int TaskServer::handle_fetch_task(ObPacket * packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer * data = packet->get_buffer();
  if (NULL == data)
  {
    TBSYS_LOG(ERROR, "check get buffer failed:data[%p]", data);
    ret = OB_ERROR;
  }
  
  if (OB_SUCCESS == ret)
  {
    TaskInfo task;
    TaskCounter counter;
    ret = task_manager_.fetch_task(counter, task);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "check fetch task failed:ret[%d]", ret);
    }

    int old_ret = ret;
    ret = rpc_->response_fetch(ret, counter, task, packet);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send response to client failed:ret[%d], task[%lu]",
          old_ret, task.get_id());
      // TODO rollback the doing or waiting counter
    }
    else
    {
      TBSYS_LOG(DEBUG, "handle fetch task succ:ret[%d], task[%lu], index[%ld]", 
          old_ret, task.get_id(), task.get_index());
    }
  }
  return ret;
}


int TaskServer::handle_finish_task(ObPacket * packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer * data = packet->get_buffer();
  if (NULL == data)
  {
    TBSYS_LOG(ERROR, "check get buffer failed:data[%p]", data);
    ret = OB_ERROR;
  }
  
  int64_t result = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_i64(data->get_data(), data->get_capacity(), 
        data->get_position(), &result);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize finish result failed:ret[%d]", ret);
    }
  }

  TaskInfo task;
  if (OB_SUCCESS == ret)
  {
    ret = task.deserialize(data->get_data(), data->get_capacity(), data->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize finish task packet failed:ret[%d]", ret);
    }
  }
  
  const char * file_name = NULL;
  if ((OB_SUCCESS == ret) && (OB_SUCCESS == result))
  {
    int64_t len = 0;
    file_name = serialization::decode_vstr(data->get_data(), data->get_capacity(), 
        data->get_position(), &len);
    if (NULL == file_name)
    {
      TBSYS_LOG(ERROR, "%s", "deserialize file name failed");
      ret = OB_ERROR;
    }
  }
  
  int64_t peer_id = 0;
  if (OB_SUCCESS == ret)
  {
    tbnet::Connection * connection = packet->get_connection();
    if (NULL == connection)
    {
      TBSYS_LOG(ERROR, "check connection failed:conn[%p]", connection);
      ret = OB_ERROR;
    }
    else
    {
      peer_id = connection->getPeerId();
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = task_manager_.finish_task((OB_SUCCESS == result), task);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "report finish task failed:task[%lu], server[%s], ret[%d]", 
        task.get_id(), tbsys::CNetUtil::addrToString(peer_id).c_str(), ret);
    }
    else if (OB_SUCCESS == result)
    {
      TBSYS_LOG(INFO, "finish task succ:task[%lu], server[%s], output_file[%s]",
        task.get_id(), tbsys::CNetUtil::addrToString(peer_id).c_str(), file_name);
    }
    else
    {
      TBSYS_LOG(WARN, "report finish task failed:task[%lu], server[%s], result[%d]",
        task.get_id(), tbsys::CNetUtil::addrToString(peer_id).c_str(), result);
    }

    int old_ret = ret;
    // response for ack 
    ret = rpc_->response_finish(OB_SUCCESS, packet);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send response to client failed:task[%lu], server[%s], result[%d], ret[%d]",
          task.get_id(), tbsys::CNetUtil::addrToString(peer_id).c_str(), result, old_ret);
    }
    else
    {
      TBSYS_LOG(INFO, "handle finish task succ:task[%lu], server[%s], result[%d], ret[%d]", 
          task.get_id(), tbsys::CNetUtil::addrToString(peer_id).c_str(), result, old_ret);
    }
  }
  
  // add finish output list
  if ((OB_SUCCESS == ret) && (OB_SUCCESS == result))
  {
    ret = task_output_.add(task.get_id(), peer_id, file_name);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "add finish task failed:task[%lu], ret[%d]", task.get_id(), ret);
    }
  }
  
  // check finish
  if (task_manager_.check_finish())
  {
    status_ = FINISH_STAT;
  }
  return ret;
}


TaskServer::~TaskServer()
{
  // finish all the tasks
  if ((status_ == FINISH_STAT) && (task_output_.size() > 0) && (NULL != result_file_))
  {
    int ret = OB_SUCCESS;
    char temp_file[TaskServerParam::OB_MAX_FILE_NAME] = "";
    snprintf(temp_file, sizeof(temp_file), "%s.temp", result_file_);
    FILE * file = fopen(temp_file, "w");
    if (NULL == file)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "check fopen output file failed:file[%s]", result_file_);
    }
    else
    {
      fprintf(file, "#task token:%ld\n", task_manager_.get_token());
      int err = task_output_.print(file);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "task output print file failed:file[%s], ret[%d]", result_file_, err);
      }
      ret = fclose(file);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fclose file failed:file[%s], ret[%d]", result_file_, ret);
      }
      else if (OB_SUCCESS == err)
      {
        ret = rename(temp_file, result_file_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "rename temp file to result file failed:temp[%s], result[%s], ret[%d]",
              temp_file, result_file_, ret);
        }
      }

      if (ret != OB_SUCCESS)
      {
        ret = remove(temp_file);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "remove temp file failed:temp[%s], result[%s], ret[%d]",
              temp_file, result_file_, ret);
        }
      }
    }
  }
}

void TaskServer::dump_task_info(const char *file)
{
  task_manager_.dump_tablets(file);
}
