#include "bigquerytest.h"
#include "common/ob_malloc.h"


BigqueryTest::BigqueryTest()
{

}

BigqueryTest::~BigqueryTest()
{
}

int BigqueryTest::init()
{
  int ret = OB_SUCCESS;

  ret = bigquerytest_param_.load_from_config();
  bigquerytest_param_.dump_param();

  if (OB_SUCCESS == ret)
  {
    ret = client_.init(bigquerytest_param_.get_ob_ip(), bigquerytest_param_.get_ob_port());
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init client, ob_ip=%s, ob_port=%d, ret=%d",
          bigquerytest_param_.get_ob_ip(), bigquerytest_param_.get_ob_port(), ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = key_gen_.init(client_, bigquerytest_param_);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init key gen, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = prefix_info_.init(client_);
    if (0 != ret)
    {
      TBSYS_LOG(WARN, "failed to init prefix info, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = read_worker_.init(key_gen_, client_, prefix_info_);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to init read worker, ret=%d", ret);
    }
    else
    {
      ret = write_worker_.init(key_gen_, client_, prefix_info_, 
          bigquerytest_param_.get_ob_rs_ip(), bigquerytest_param_.get_ob_rs_port());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to init write worker, ret=%d", ret);
      }
    }
  }

  return ret;
}

int BigqueryTest::start()
{
  int ret                     = OB_SUCCESS; 
  int64_t write_thread_count  = 0;
  int64_t read_thread_count   = 0;

  ret = ob_init_memory_pool();
  if (OB_SUCCESS == ret)
  {
    ret = init();
  }

  if (OB_SUCCESS == ret)
  {
    write_thread_count = bigquerytest_param_.get_write_thread_count();
    read_thread_count = bigquerytest_param_.get_read_thread_count();

    if (write_thread_count > 0)
    {
      write_worker_.setThreadCount(write_thread_count);
      write_worker_.start();
    }

    if (read_thread_count > 0)
    {
      read_worker_.setThreadCount(read_thread_count);
      read_worker_.start();
    }

    if (OB_SUCCESS == ret)
    {
      ret = wait();
    }
  }

  return ret;
}

int BigqueryTest::stop()
{
  write_worker_.stop();
  read_worker_.stop();

  return OB_SUCCESS;
}

int BigqueryTest::wait()
{
  write_worker_.wait();
  read_worker_.wait();

  return OB_SUCCESS;
}


