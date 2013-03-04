/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         common_func.h is for what ...
 *
 *  Version: $Id: common_func.h 2010年11月17日 16时18分07秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "common/ob_range.h"
#include "common/ob_scanner.h"

int64_t random_number(int64_t min, int64_t max);
int parse_number_range(const char *number_string, 
    int32_t *number_array, int32_t &number_array_size);
int parse_range_str(const char* range_str, int hex_format, oceanbase::common::ObRange &range);
void dump_scanner(oceanbase::common::ObScanner &scanner);
int dump_tablet_info(oceanbase::common::ObScanner &scanner);
