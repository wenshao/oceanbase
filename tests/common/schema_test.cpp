#include <gtest/gtest.h>
#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/file_directory_utils.h"
using namespace oceanbase;
using namespace oceanbase::common;
TEST(SchemaTestV2, basicTest)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("./test1.ini", c1));
  schema_manager->print_info();

  ASSERT_EQ(6,schema_manager->get_column_count());
  ASSERT_EQ(3,schema_manager->get_table_count());
  schema_manager->print_info();

  const ObColumnSchemaV2 *col = schema_manager->column_begin();
  for(;col != schema_manager->column_end(); ++col)
  {
    col->print_info();
  }

  int32_t size = 0;
  col = schema_manager->get_table_schema(1001,size);
  ASSERT_EQ(2,size);

  int32_t idx = 0;
  col = schema_manager->get_column_schema(1002,2,&idx);
  ASSERT_EQ(0,idx);
  ASSERT_TRUE(NULL != col);

  col = schema_manager->get_column_schema(1002,3,&idx);
  ASSERT_EQ(1,idx);
  ASSERT_TRUE(NULL != col);

  col = schema_manager->get_column_schema("collect_info","item_name");
  ASSERT_EQ(col->get_id(),2UL);

  const ObTableSchema *table = schema_manager->get_table_schema("collect_info");
  ASSERT_EQ(table->get_table_id(),(uint64_t)1002);

  ObString table_name_str;
  table_name_str.assign_ptr((char*)"collect_info",12);
  table = schema_manager->get_table_schema(table_name_str);
  ASSERT_EQ(table->get_table_id(),(uint64_t)1002);

  delete schema_manager;
}


TEST(SchemaTestV2, serialize)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));

  ASSERT_EQ((uint64_t)6,schema_manager->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager->get_modify_time_column_id(1001));

  int64_t len = schema_manager->get_serialize_size();
  char *buf =(char *)malloc(len);
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->serialize(buf,len,pos));

  ASSERT_EQ(pos,len);

  pos = 0;
  ObSchemaManagerV2 *schema_manager_1 = new ObSchemaManagerV2();
  ASSERT_EQ(0,schema_manager_1->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);

  ASSERT_EQ(schema_manager_1->get_serialize_size(),len);
  char *buf_2 = (char *)malloc(len);
  assert(buf_2 != NULL);

  ASSERT_EQ((uint64_t)6,schema_manager_1->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager_1->get_modify_time_column_id(1001));

  pos = 0;
  schema_manager_1->serialize(buf_2,len,pos);
  ASSERT_EQ(pos,len);

  ASSERT_EQ(0,memcmp(buf,buf_2,len));


  delete schema_manager;
  delete schema_manager_1;
  free(buf);
  buf = NULL;
  free(buf_2);
  buf_2 = NULL;
}

TEST(SchemaTestV2,column_group)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));

  int32_t size = OB_MAX_COLUMN_GROUP_NUMBER;
  uint64_t group[OB_MAX_COLUMN_GROUP_NUMBER];
  ASSERT_EQ(0,schema_manager->get_column_groups(1001,group,size));

  ASSERT_EQ(4,size);
  ASSERT_EQ((uint64_t)11,group[0]);
  ASSERT_EQ((uint64_t)12,group[1]);
  ASSERT_EQ((uint64_t)13,group[2]);
  ASSERT_EQ((uint64_t)14,group[3]);

  delete schema_manager;
}

TEST(SchemaTestV2,column_group_all)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));
  uint64_t column_id = 0;
  int64_t duration = 0;
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)3, column_id);
  ASSERT_EQ(7, duration);
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)4, column_id);
  ASSERT_EQ(10, duration);
  int32_t size = OB_MAX_COLUMN_GROUP_NUMBER;
  uint64_t group[OB_MAX_COLUMN_GROUP_NUMBER];
  ASSERT_EQ(0,schema_manager->get_column_groups(1001,group,size));

  ASSERT_EQ(4,size);
  ASSERT_EQ((uint64_t)11,group[0]);
  ASSERT_EQ((uint64_t)12,group[1]);

  const ObColumnSchemaV2 *col = schema_manager->get_table_schema(1001,size);
  UNUSED(col);
  ASSERT_EQ(32,size);

  int32_t index[OB_MAX_COLUMN_GROUP_NUMBER];
  size = OB_MAX_COLUMN_GROUP_NUMBER;
  int ret= schema_manager->get_column_index("collect_info","info_user_nick",index,size);
  ASSERT_EQ(0,ret);
  ASSERT_EQ(4,size);

  for(int32_t i=0;i<size;++i)
  {
    ASSERT_EQ((uint64_t)2,schema_manager->get_column_schema(index[i])->get_id());
  }

  ObColumnSchemaV2* columns[OB_MAX_COLUMN_GROUP_NUMBER];
  size = OB_MAX_COLUMN_GROUP_NUMBER;

  ret = schema_manager->get_column_schema(1001,5,columns,size);
  ASSERT_EQ(0,ret);
  ASSERT_EQ(2,size);

  for(int32_t i=0;i<size;++i)
  {
    ASSERT_EQ((uint64_t)5,columns[i]->get_id());
  }

  //drop column group
  int64_t len = schema_manager->get_serialize_size();
  char *buf =(char *)malloc(len);
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->serialize(buf,len,pos));
  ASSERT_EQ(pos,len);

  ObSchemaManagerV2 *schema_manager_1 = new ObSchemaManagerV2();
  schema_manager_1->set_drop_column_group();
  pos = 0;
  ASSERT_EQ(0,schema_manager_1->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);
  ASSERT_EQ(42,schema_manager_1->get_column_count());

  schema_manager_1->print_info();

  delete schema_manager;
}



TEST(SchemaTestV2,equal)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));

  uint64_t column_id = 0;
  int64_t duration = 0;
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)3, column_id);
  ASSERT_EQ(7, duration);
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)4, column_id);
  ASSERT_EQ(10, duration);

  ObSchemaManagerV2 *schema_manager_1 = new ObSchemaManagerV2();
  *schema_manager_1 = *schema_manager;

  int64_t len = schema_manager->get_serialize_size();
  char *buf =(char *)malloc(len);
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->serialize(buf,len,pos));
  ASSERT_EQ(pos,len);

  ASSERT_EQ(schema_manager_1->get_serialize_size(),len);

  char *buf_2 = (char *)malloc(len);
  assert(buf_2 != NULL);

  pos = 0;
  ASSERT_EQ(0,schema_manager_1->serialize(buf_2,len,pos));
  ASSERT_EQ(pos,len);

  ASSERT_EQ(0,memcmp(buf,buf_2,len));

  delete buf;
  delete buf_2;
  delete schema_manager;
  delete schema_manager_1;
}

TEST(SchemaTestV2, btefff)
{
  tbsys::CConfig *c1 = new tbsys::CConfig();
  //tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2(1);
  TBSYS_LOG(DEBUG,"c1:%lu,schema_manager:%lu",sizeof(*c1),sizeof(*schema_manager));
  TBSYS_LOG(DEBUG,"c1:%p,schema_manager:%p",c1,schema_manager);
  ASSERT_EQ(true, schema_manager->parse_from_file("./collect_schema.ini", *c1));
  TBSYS_LOG(DEBUG,"c1:%lu,schema_manager:%lu",sizeof(*c1),sizeof(*schema_manager));
  TBSYS_LOG(DEBUG,"c1:%p,schema_manager:%p",c1,schema_manager);

  //schema_manager->print_info();

  delete schema_manager;

  TBSYS_LOG(DEBUG,"end--");
  delete c1;
}
TEST(SchemaTest, SchemaV2notorder)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 schema_manager(1);
  ASSERT_EQ(true, schema_manager.parse_from_file("./schema.ini", c1));
  schema_manager.print_info();
  ASSERT_EQ(2, schema_manager.table_end() - schema_manager.table_begin());
  const ObTableSchema *table_schema = schema_manager.table_begin();
  ASSERT_EQ((uint64_t)1001, table_schema->get_table_id());
  ASSERT_EQ(1, table_schema->get_table_type());
  ASSERT_EQ(0, strcmp("collect_info", table_schema->get_table_name()));
  ASSERT_EQ((uint64_t)22, table_schema->get_max_column_id());
  ++table_schema;
  ASSERT_EQ((uint64_t)1002, table_schema->get_table_id());
  ASSERT_EQ(1, table_schema->get_table_type());
  ASSERT_EQ(0, strcmp("item_info", table_schema->get_table_name()));
  ASSERT_EQ((uint64_t)12, table_schema->get_max_column_id());

  int32_t table_size = 0;
  const ObTableSchema* table = schema_manager.get_table_schema((uint64_t)1001);
  ASSERT_EQ(true, table != NULL);
  ASSERT_EQ((uint64_t)1001, table->get_table_id());
  ASSERT_EQ(80, table->get_block_size());
  ASSERT_EQ(true, table->is_use_bloomfilter());
  ASSERT_EQ(0, strcmp("lalala", table->get_compress_func_name()));

  const ObColumnSchemaV2* table_schema_column = schema_manager.get_table_schema((uint64_t)1001, table_size);
  ASSERT_EQ(true, table_schema_column != NULL);
  ASSERT_EQ(23, table_size);


  int32_t group_size = OB_MAX_COLUMN_GROUP_NUMBER;
  uint64_t column_groups[OB_MAX_COLUMN_GROUP_NUMBER];

  int ret = schema_manager.get_column_groups((uint64_t)1001, column_groups, group_size);
  ret++;
  ASSERT_EQ((uint64_t)0, column_groups[0]);
  ASSERT_EQ(5, group_size);
  for (int64_t index = 1; index < group_size; ++index)
  {
    ASSERT_EQ((uint64_t)10+index, column_groups[index]);
  }

  const ObColumnSchemaV2* default_group_schema = schema_manager.get_group_schema((uint64_t)1001,(uint64_t) 0, group_size);
  ASSERT_EQ(2, group_size);
  ASSERT_EQ(0, strcmp("user_nick", default_group_schema->get_name()));
  ASSERT_EQ((uint64_t)2, default_group_schema->get_id());
  ASSERT_EQ((uint64_t)1001, default_group_schema->get_table_id());

  const ObColumnSchemaV2* first_group_schema = schema_manager.get_group_schema(1001, 11, group_size);
  ASSERT_EQ(3, group_size);
  ASSERT_EQ(0, strcmp("note", first_group_schema->get_name()));
  ASSERT_EQ((uint64_t)4, first_group_schema->get_id());

  const ObColumnSchemaV2* column = schema_manager.get_column_schema((uint64_t)1001, (uint64_t)11);
  ASSERT_EQ(true, column != NULL);
  ASSERT_EQ((uint64_t)11, column->get_id());
  ASSERT_EQ(0, strcmp("title", column->get_name()));
  ASSERT_EQ((uint64_t)12, column->get_column_group_id());
  int32_t index_array[OB_MAX_COLUMN_GROUP_NUMBER];
  int32_t index_size = OB_MAX_COLUMN_GROUP_NUMBER;
  ret = schema_manager.get_column_index((uint64_t)1001, (uint64_t)11, index_array, index_size);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(2, index_size);
  ASSERT_EQ(7, index_array[0]);
  ASSERT_EQ(18,index_array[1]);
}

TEST(SchemaTest, checkExpire)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema_error_expire.ini",c1));

  uint64_t column_id = 0;
  int64_t duration = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1003)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)3, column_id);
  ASSERT_EQ(0, duration);

  ASSERT_EQ((uint64_t)6,schema_manager->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager->get_modify_time_column_id(1001));
}

TEST(SchemaTest, expireInfo)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));

  uint64_t column_id = 0;
  int64_t duration = 0;
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)3, column_id);
  ASSERT_EQ(7, duration);
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)4, column_id);
  ASSERT_EQ(10, duration);

  ASSERT_EQ((uint64_t)6,schema_manager->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager->get_modify_time_column_id(1001));

  int64_t len = schema_manager->get_serialize_size();
  char *buf =(char *)malloc(len);
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->serialize(buf,len,pos));

  ASSERT_EQ(pos,len);

  pos = 0;
  ObSchemaManagerV2 *schema_manager_1 = new ObSchemaManagerV2();
  ASSERT_EQ(0,schema_manager_1->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);

  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)3, column_id);
  ASSERT_EQ(7, duration);
  ASSERT_EQ(OB_SUCCESS, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  ASSERT_EQ((uint64_t)4, column_id);
  ASSERT_EQ(10, duration);

  ASSERT_EQ(schema_manager_1->get_serialize_size(),len);
  char *buf_2 = (char *)malloc(len);
  assert(buf_2 != NULL);

  ASSERT_EQ((uint64_t)6,schema_manager_1->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager_1->get_modify_time_column_id(1001));

  pos = 0;
  schema_manager_1->serialize(buf_2,len,pos);
  ASSERT_EQ(pos,len);

  ASSERT_EQ(0,memcmp(buf,buf_2,len));


  delete schema_manager;
  delete schema_manager_1;
  free(buf);
  buf = NULL;
  free(buf_2);
  buf_2 = NULL;

  tbsys::CConfig c2;
  ObSchemaManagerV2 *schema_manager2 = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager2->parse_from_file("./schema.ini",c2));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager2->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager2->get_table_schema(1002)->get_expire_condition(column_id, duration));

  len = schema_manager2->get_serialize_size();
  buf =(char *)malloc(len);
  assert(buf != NULL);
  pos = 0;
  ASSERT_EQ(0,schema_manager2->serialize(buf,len,pos));

  ASSERT_EQ(pos,len);

  pos = 0;
  ObSchemaManagerV2 *schema_manager3 = new ObSchemaManagerV2();
  ASSERT_EQ(0,schema_manager3->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager3->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager3->get_table_schema(1002)->get_expire_condition(column_id, duration));
  delete schema_manager2;
  delete schema_manager3;
  free(buf);
  buf = NULL;
}

TEST(SchemaTest, compatible_with_v020)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  FileDirectoryUtils fu;
  int64_t size = fu.get_size("schemamanagerbuffer020");
  char *buf =(char *)malloc(size);
  assert(buf != NULL);
  int fd = open("schemamanagerbuffer020", O_RDONLY);
  read(fd, buf, size);
  close(fd);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->deserialize(buf,size,pos));
  ASSERT_EQ(pos,size);

  uint64_t column_id = 0;
  int64_t duration = 0;
  ASSERT_EQ((uint64_t)6,schema_manager->get_create_time_column_id(1001));
  ASSERT_EQ((uint64_t)7,schema_manager->get_modify_time_column_id(1001));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager->get_table_schema(1001)->get_expire_condition(column_id, duration));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, schema_manager->get_table_schema(1002)->get_expire_condition(column_id, duration));
  free(buf);
  buf = NULL;
}

TEST(SchemaTest, expire_condition)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager->parse_from_file("./collect_schema.ini",c1));

  ASSERT_EQ(0, strcmp(schema_manager->get_table_schema(1001)->get_expire_condition(), ""));
  ASSERT_EQ(1, schema_manager->get_table_schema(1001)->get_expire_frequency());
  ASSERT_EQ(0, schema_manager->get_table_schema(1001)->get_block_size());
  ASSERT_EQ(0, schema_manager->get_table_schema(1001)->get_max_sstable_size());

  ASSERT_EQ(0, strcmp(schema_manager->get_table_schema(1002)->get_expire_condition(), ""));
  ASSERT_EQ(1, schema_manager->get_table_schema(1002)->get_expire_frequency());
  ASSERT_EQ(0, schema_manager->get_table_schema(1002)->get_block_size());
  ASSERT_EQ(0, schema_manager->get_table_schema(1002)->get_max_sstable_size());

  int64_t len = schema_manager->get_serialize_size();
  char *buf =(char *)malloc(len);
  assert(buf != NULL);
  int64_t pos = 0;
  ASSERT_EQ(0,schema_manager->serialize(buf,len,pos));

  ASSERT_EQ(pos,len);

  pos = 0;
  ObSchemaManagerV2 *schema_manager_1 = new ObSchemaManagerV2();
  ASSERT_EQ(0,schema_manager_1->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);

  ASSERT_EQ(0, strcmp(schema_manager_1->get_table_schema(1001)->get_expire_condition(), ""));
  ASSERT_EQ(1, schema_manager_1->get_table_schema(1001)->get_expire_frequency());
  ASSERT_EQ(0, schema_manager_1->get_table_schema(1001)->get_block_size());
  ASSERT_EQ(0, schema_manager_1->get_table_schema(1001)->get_max_sstable_size());

  ASSERT_EQ(0, strcmp(schema_manager_1->get_table_schema(1002)->get_expire_condition(), ""));
  ASSERT_EQ(1, schema_manager_1->get_table_schema(1002)->get_expire_frequency());
  ASSERT_EQ(0, schema_manager_1->get_table_schema(1002)->get_block_size());
  ASSERT_EQ(0, schema_manager_1->get_table_schema(1002)->get_max_sstable_size());

  ASSERT_EQ(schema_manager_1->get_serialize_size(),len);
  char *buf_2 = (char *)malloc(len);
  assert(buf_2 != NULL);

  pos = 0;
  schema_manager_1->serialize(buf_2,len,pos);
  ASSERT_EQ(pos,len);

  ASSERT_EQ(0,memcmp(buf,buf_2,len));

  delete schema_manager;
  delete schema_manager_1;
  free(buf);
  buf = NULL;
  free(buf_2);
  buf_2 = NULL;

  const char* info_expire_cond =
    "(`info_gm_modified` < $SYS_DATE AND (`item_category` = 1 OR `info_status` = 1))";
  const char* item_expire_cond =
    "(`item_gm_modified` < $SYS_DATE AND (`item_category` = 1 OR `item_status` = 1))";
  tbsys::CConfig c2;
  ObSchemaManagerV2 *schema_manager2 = new ObSchemaManagerV2();
  ASSERT_EQ(true,schema_manager2->parse_from_file("./collect_schema_expire.ini",c2));
  ASSERT_EQ(0, strcmp(schema_manager2->get_table_schema(1001)->get_expire_condition(), info_expire_cond));
  ASSERT_EQ(1, schema_manager2->get_table_schema(1001)->get_expire_frequency());
  ASSERT_EQ(65536, schema_manager2->get_table_schema(1001)->get_block_size());
  ASSERT_EQ(268435456, schema_manager2->get_table_schema(1001)->get_max_sstable_size());

  ASSERT_EQ(0, strcmp(schema_manager2->get_table_schema(1002)->get_expire_condition(), item_expire_cond));
  ASSERT_EQ(1, schema_manager2->get_table_schema(1002)->get_expire_frequency());
  ASSERT_EQ(65536, schema_manager2->get_table_schema(1002)->get_block_size());
  ASSERT_EQ(268435456, schema_manager2->get_table_schema(1002)->get_max_sstable_size());

  len = schema_manager2->get_serialize_size();
  buf =(char *)malloc(len);
  assert(buf != NULL);
  pos = 0;
  ASSERT_EQ(0,schema_manager2->serialize(buf,len,pos));

  ASSERT_EQ(pos,len);

  pos = 0;
  ObSchemaManagerV2 *schema_manager3 = new ObSchemaManagerV2();
  ASSERT_EQ(0,schema_manager3->deserialize(buf,len,pos));
  ASSERT_EQ(pos,len);
  ASSERT_EQ(0, strcmp(schema_manager3->get_table_schema(1001)->get_expire_condition(), info_expire_cond));
  ASSERT_EQ(1, schema_manager3->get_table_schema(1001)->get_expire_frequency());
  ASSERT_EQ(65536, schema_manager3->get_table_schema(1001)->get_block_size());
  ASSERT_EQ(268435456, schema_manager3->get_table_schema(1001)->get_max_sstable_size());

  ASSERT_EQ(0, strcmp(schema_manager3->get_table_schema(1002)->get_expire_condition(), item_expire_cond));
  ASSERT_EQ(1, schema_manager3->get_table_schema(1002)->get_expire_frequency());
  ASSERT_EQ(65536, schema_manager3->get_table_schema(1002)->get_block_size());
  ASSERT_EQ(268435456, schema_manager3->get_table_schema(1002)->get_max_sstable_size());
  delete schema_manager2;
  delete schema_manager3;
  free(buf);
  buf = NULL;
}

TEST(SchemaHelper, test_parse_time)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  int64_t schema_timestamp = tbsys::CTimeUtil::getTime();
  ASSERT_EQ(true, schema_manager->parse_from_file("dw_schema.ini", c1));
  int64_t schema_timestamp2 = tbsys::CTimeUtil::getTime();
  printf("west time = %ld", schema_timestamp2 - schema_timestamp);
}

TEST(SchemaManager,test_join_table)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("schema.ini", c1));

  ASSERT_EQ(1,schema_manager->get_join_table_num());
  ASSERT_TRUE(schema_manager->is_join_table(1002UL));

  delete schema_manager;
  schema_manager = NULL;
}
TEST(SchemaManager, test_is_compatible)
{
  tbsys::CConfig c1;
  ObSchemaManagerV2 *schema_manager = new ObSchemaManagerV2();
  ASSERT_EQ(true, schema_manager->parse_from_file("schema.ini", c1));

  //减少一个column
  {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("less_column.ini", c2));
    ASSERT_EQ(true, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
  //修改table_id
  {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("modify_table_id.ini", c2));
    ASSERT_EQ(true, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
  //修改column_id

{
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("error_modify_column_id2.ini", c2));
    ASSERT_EQ(false, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
  //varchar 长度变化
  {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("vchar.ini", c2));
    ASSERT_EQ(true, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
 {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("err_vchar.ini", c2));
    ASSERT_EQ(false, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }

//增加一个表
  {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("add_table.ini", c2));
    ASSERT_EQ(true, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
  //增加一个column
  {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("add_column.ini", c2));
    ASSERT_EQ(true, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }
 {
    tbsys::CConfig c2;
    ObSchemaManagerV2 *tmp_schema_manager = new ObSchemaManagerV2();
    ASSERT_EQ(true, tmp_schema_manager->parse_from_file("err_add_column.ini", c2));
    ASSERT_EQ(false, schema_manager->is_compatible(*tmp_schema_manager));
    delete tmp_schema_manager;
    tmp_schema_manager = NULL;
  }

delete schema_manager;
  schema_manager = NULL;
}
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
