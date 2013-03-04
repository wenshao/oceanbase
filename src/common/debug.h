#ifdef __rs_debug__
oceanbase::rootserver::ObRootServer2* __rs = NULL;
oceanbase::common::ObRoleMgr* __role_mgr = NULL;
oceanbase::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __rs = &root_server_; __role_mgr = &role_mgr_;  __obi_role = (typeof(__obi_role))&root_server_.get_obi_role();
#endif
#ifdef __ups_debug__
oceanbase::updateserver::ObUpdateServer* __ups = NULL;
oceanbase::updateserver::ObUpsRoleMgr* __role_mgr = NULL;
oceanbase::updateserver::ObUpsLogMgr* __log_mgr = NULL;
oceanbase::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __ups = this; __role_mgr = &role_mgr_; __obi_role = &obi_role_; __log_mgr = &log_mgr_;
#endif
