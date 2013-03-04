#include "client/cpp/libobapi.h"
#include "client_wrapper.h"

using namespace oceanbase;
using namespace common;
using namespace client;

MKClient::~MKClient()
{
  cli_.destroy();
}

int MKClient::init(const char *addr, const int port)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  return cli_.init(dst_host);
}

int MKClient::apply(common::ObMutator &mutator)
{
  return cli_.ups_apply(mutator, TIMEOUT_US);
}

int MKClient::get(common::ObGetParam &get_param, common::ObScanner &scanner)
{
  return cli_.ups_get(get_param, scanner, TIMEOUT_US);
}

int MKClient::scan(common::ObScanParam &scan_param, common::ObScanner &scanner)
{
  return cli_.ups_scan(scan_param, scanner, TIMEOUT_US);
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void OBClient::api_cntl_(int32_t cmd, ...)
{
  va_list ap;
  va_start(ap, cmd);
  cli_.api_cntl(cmd, ap);
  va_end(ap);
}

int OBClient::init(const char *addr, const int port)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_ERR_SUCCESS;
  if (OB_ERR_SUCCESS != (tmp_ret = cli_.connect(addr, port, NULL, NULL)))
  {
    TBSYS_LOG(WARN, "connect to ob fail ret=%d", tmp_ret);
    ret = OB_ERROR;
  }
  else
  {
    int64_t ms_refresh_interval = MS_REFRESH_INTERVAL_US;
    int64_t timeout_s = TIMEOUT_US;
    api_cntl_(OB_S_REFRESH_INTERVAL, ms_refresh_interval);
    api_cntl_(OB_S_TIMEOUT_SET, timeout_s);
    api_cntl_(OB_S_TIMEOUT_GET, timeout_s);
    api_cntl_(OB_S_TIMEOUT_SCAN, timeout_s);
  }
  return ret;
}

int OBClient::apply(common::ObMutator &mutator)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_ERR_SUCCESS;
  if (OB_ERR_SUCCESS != (tmp_ret = cli_.set(mutator)))
  {
    TBSYS_LOG(WARN, "set to ob fail ret=%d", tmp_ret);
    ret = OB_ERROR;
  }
  return ret;
}

int OBClient::get(common::ObGetParam &get_param, common::ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  OB_RES *ob_res = NULL;
  if (NULL == (ob_res = cli_.get(get_param)))
  {
    TBSYS_LOG(WARN, "get from ob fail ret=%d", ob_errno());
    ret = OB_ERROR;
  }
  else
  {
    ret = copy(*(ob_res->cur_scanner), scanner);
    cli_.release_res_st(ob_res);
  }
  return ret;
}

int OBClient::scan(common::ObScanParam &scan_param, common::ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  OB_RES *ob_res = NULL;
  if (NULL == (ob_res = cli_.scan(scan_param)))
  {
    TBSYS_LOG(WARN, "scan from ob fail ret=%d", ob_errno());
    ret = OB_ERROR;
  }
  else
  {
    ret = copy(*(ob_res->cur_scanner), scanner);
    cli_.release_res_st(ob_res);
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ClientWrapper::ClientWrapper() : cli_(NULL)
{
}

ClientWrapper::~ClientWrapper()
{
}

int ClientWrapper::init(const char *addr, const int port)
{
  char addr_buf[1024];
  sprintf(addr_buf, "%s", addr);
  if ('@' == addr[strlen(addr) - 1])
  {
    cli_ = &mk_cli_;
    TBSYS_LOG(INFO, "using mk_cli addr=%s", addr);
    addr_buf[strlen(addr_buf) - 1] = '\0';
  }
  else
  {
    cli_ = &ob_cli_;
    TBSYS_LOG(INFO, "using ob_cli addr=%s", addr);
  }
  return cli_->init(addr_buf, port);
}

int ClientWrapper::apply(ObMutator &mutator)
{
  return cli_->apply(mutator);
}

int ClientWrapper::get(ObGetParam &get_param, ObScanner &scanner)
{
  return cli_->get(get_param, scanner);
}

int ClientWrapper::scan(ObScanParam &scan_param, ObScanner &scanner)
{
  return cli_->scan(scan_param, scanner);
}

