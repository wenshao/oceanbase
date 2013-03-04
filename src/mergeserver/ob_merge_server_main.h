#ifndef OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_
#define OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_

#include "common/base_main.h"
#include "ob_merge_server.h"

extern const char* svn_version();
extern const char* build_date();
extern const char* build_time();

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerMain : public common::BaseMain
    {
      public:
        static ObMergeServerMain * get_instance();
        int do_work();
        void do_signal(const int sig);

      public:
        const ObMergeServer& get_merge_server() const { return server_; }

      protected:
        virtual void print_version();
      private:
        ObMergeServerMain();
      private:
        ObMergeServer server_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVERMAIN_H_ */
