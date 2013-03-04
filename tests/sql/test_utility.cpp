
#include "test_utility.h"

using namespace oceanbase::sql;


void test::split(char *line, const char *delimiters, char **tokens, int32_t &count)
{
  count = 0;
  char *pch = strtok(line, delimiters);
  while(pch != NULL)
  {
    tokens[count ++] = pch;
    pch = strtok(NULL, delimiters);
  }
}

