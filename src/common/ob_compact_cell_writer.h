
/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_iterator.h,v 0.1 2010/08/18 13:24:51 chuanhui Exp $
 *
 * Authors:
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_COMMON_COMPACT_CELL_WRITER_H_ 
#define OCEANBASE_COMMON_COMPACT_CELL_WRITER_H_

#include "ob_buffer_helper.h"
#include "ob_cell_meta.h"
#include "ob_compact_cell_util.h"
#include "ob_compact_store_type.h"

namespace oceanbase
{
  namespace common
  {
    /*
     * 紧凑格式写入类
     */
    class ObCompactCellWriter
    {
      public:
        ObCompactCellWriter();
        virtual ~ObCompactCellWriter() { }

      public:
        int init(char *buf, int64_t size, enum ObCompactStoreType store_type = SPARSE);
        /*
         * 写入一个cell
         * 用于稀疏格式写入，column_id设置为OB_INVALID_ID也可用于稠密格式
         */
        int append(uint64_t column_id, const ObObj &value, ObObj *clone_value = NULL);

        /*
         * 写入一个cell，用于稠密格式不带column_id
         */
        int append(const ObObj &value, ObObj *clone_value = NULL);
        int append_escape(int64_t escape);

        inline int row_delete()
        {
          return append_escape(ObCellMeta::ES_DEL_ROW);
        }

        inline int row_nop()
        {
          return append_escape(ObCellMeta::ES_NOP_ROW);
        }

        inline int row_not_exist()
        {
          return append_escape(ObCellMeta::ES_NOT_EXIST_ROW);
        }

        inline int row_finish()
        {
          return append_escape(ObCellMeta::ES_END_ROW);
        }

        /* 写入rowkey结束以后需要插入一个换行符 */
        inline int rowkey_finish()
        {
          return append_escape(ObCellMeta::ES_END_ROW);
        }

        inline int64_t size()
        {
          return buf_writer_.pos();
        }

        inline char *get_buf()
        {
          return buf_writer_.buf();
        }

        inline void reset()
        {
          buf_writer_.reset();
        }

      protected:
        virtual int write_varchar(const ObObj &value, ObObj *clone_value);
        int write_decimal(const ObObj &decimal);

      protected:
        ObBufferWriter buf_writer_;
        enum ObCompactStoreType store_type_;
    };
  }
}

#endif /* OCEANBASE_COMMON_COMPACT_CELL_WRITER_H_ */

