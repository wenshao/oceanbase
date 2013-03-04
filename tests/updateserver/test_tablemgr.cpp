#include "updateserver/ob_update_server_main.h"
#include "updateserver/ob_update_server.h"
#include "updateserver/ob_table_mgr.h"
#include "test_helper.h"
#include "test_utils.h"

using namespace oceanbase;
using namespace common;
using namespace updateserver;

#define UPS ObUpdateServerMain::get_instance()->get_update_server()

void fill_memtable(MemTable &memtable)
{
  ObUpsMutator mutator;
  ObMutatorCellInfo cellinfo;
  uint64_t trans_descriptor = 0;

  cellinfo.op_type.set_ext(ObActionFlag::OP_INSERT);
  cellinfo.cell_info.table_id_ = 1001;
  cellinfo.cell_info.row_key_.assign((char*)"rk0001_00000000000", 17);
  cellinfo.cell_info.column_id_ = 5;
  cellinfo.cell_info.value_.set_int(1023);
  int ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.value_.set_int(1, true);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.row_key_.assign((char*)"rk0002_00000000000", 17);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.row_key_.assign((char*)"rk0003_00000000000", 17);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  cellinfo.cell_info.table_id_ = 1002;
  cellinfo.cell_info.value_.set_int(1024);
  ret = mutator.get_mutator().add_cell(cellinfo);
  assert(OB_SUCCESS == ret);

  prepare_mutator(mutator.get_mutator());

  ret = memtable.start_transaction(WRITE_TRANSACTION, trans_descriptor);
  assert(OB_SUCCESS == ret);
  ret = memtable.start_mutation(trans_descriptor);
  assert(OB_SUCCESS == ret);
  ret = memtable.set(trans_descriptor, mutator);
  assert(OB_SUCCESS == ret);
  ret = memtable.end_mutation(trans_descriptor, false);
  assert(OB_SUCCESS == ret);
  ret = memtable.end_transaction(trans_descriptor, false);
  assert(OB_SUCCESS == ret);
}

void get_table(TableMgr &tm, const ObList<ITableEntity*> &tlist)
{
  int ret = OB_SUCCESS;
  ITableEntity::Guard guard(tm.get_resource_pool());
  uint64_t table_id = 1001;
  ObString row_key(17, 17, (char*)"rk0001_00000000000");
  ObList<ITableEntity*>::const_iterator iter;
  int64_t index = 0;
  for (iter = tlist.begin(); iter != tlist.end(); iter++, index++)
  {
    ITableEntity *table_entity = *iter;

    ITableUtils *table_utils = table_entity->get_tsi_tableutils(index);
    assert(NULL != table_utils);
    table_utils->reset();

    uint64_t trans_descriptor = table_utils->get_trans_descriptor();

    ITableIterator *titer = table_entity->alloc_iterator(tm.get_resource_pool(), guard);;
    assert(NULL != titer);

    ColumnFilter *cf = ITableEntity::get_tsi_columnfilter();
    assert(NULL != cf);
    cf->add_column(0);

    ret = table_entity->start_transaction(trans_descriptor);
    assert(OB_SUCCESS == ret);

    //ret = table_entity->get(trans_descriptor, table_id, row_key, cf, titer);
    //assert(OB_SUCCESS == ret);
    ObRange range;
    range.table_id_ = table_id;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.start_key_.assign_ptr((char*)"rk0001_0000000000", 17);
    range.end_key_.assign_ptr((char*)"rk0002_0000000000", 17);
    //range.border_flag_.set_min_value();
    //range.border_flag_.set_max_value();
    ObScanParam scan_param;
    //scan_param.set_is_result_cached(true);
    scan_param.set(table_id, ObString(), range);
    scan_param.add_column(5);
    scan_param.add_column(6);
    scan_param.add_column(7);
    ret = table_entity->scan(trans_descriptor, scan_param, titer);

    while (OB_SUCCESS == titer->next_cell())
    {
      ObCellInfo *cell_info = NULL;
      bool is_row_changed = false;
      if (OB_SUCCESS == titer->get_cell(&cell_info, &is_row_changed))
      {
        fprintf(stderr, "[result] %s %d\n", common::print_cellinfo(cell_info), is_row_changed);
      }
    }
    fprintf(stderr, "[result] ==========\n");

    table_utils->reset();
    ret = table_entity->end_transaction(trans_descriptor);
    assert(OB_SUCCESS == ret);

  }
  thread_read_complete();
}

int main(int argc, char **argv)
{
  if (4 != argc)
  {
    fprintf(stderr, "./test_tablemgr [store_root] [raid_regex] [store_regex]\n");
    exit(-1);
  }

  ob_init_memory_pool();
  char *root = argv[1];
  char *raid = argv[2];
  char *store = argv[3];
  TableMgr tm;
  SSTableMgr &sstm = UPS.get_sstable_mgr();
  CommonSchemaManagerWrapper schema_mgr;
  tbsys::CConfig config;
  schema_mgr.parse_from_file("test_schema.ini", config);
  UPS.get_table_mgr().set_schemas(schema_mgr);

  common::ObiRole obi_role;
  ObUpsRoleMgr role_mgr;
  ObUpsSlaveMgr slave_mgr;
  ObReplayLogSrc replay_log_src;
  role_mgr.set_role(ObUpsRoleMgr::MASTER);
  int ret = slave_mgr.init(&role_mgr, &UPS.get_ups_rpc_stub(), 1000000);
  assert(OB_SUCCESS == ret);
  ret = UPS.get_log_mgr().init("./commitlog", 64 * 1024 * 1024, &replay_log_src, &(UPS.get_table_mgr()), &slave_mgr, &obi_role, &role_mgr, 0);
  assert(OB_SUCCESS == ret);
  ret = UPS.get_log_mgr().replay_local_log();
  assert(OB_SUCCESS == ret);
  UPS.get_log_mgr().start_log(5, 30);

  ret = tm.init();
  assert(OB_SUCCESS == ret);

  sstable::ObBlockCacheConf bc_conf;
  bc_conf.block_cache_memsize_mb = 100;
  sstable::ObBlockIndexCacheConf bic_conf;
  bic_conf.cache_mem_size = 100 * 1024 * 1024;
  ret = UPS.get_sstable_query().init(bc_conf, bic_conf);
  assert(OB_SUCCESS == ret);

  ret = sstm.init(root, raid, store);
  assert(OB_SUCCESS == ret);

  ret = sstm.reg_observer(&tm);
  assert(OB_SUCCESS == ret);

  sstm.load_new();

  TableItem *ti = tm.get_active_memtable();
  assert(NULL != ti);
  fill_memtable(ti->get_memtable());
  tm.revert_active_memtable(ti);

  ret = tm.replay_freeze_memtable(SSTableID::get_id(11, 2, 2), SSTableID::get_id(11, 1, 1), 7);
  assert(OB_SUCCESS == ret);

  tm.sstable_scan_finished(3);
  tm.log_table_info();

  ObVersionRange vg;
  vg.start_version_ = 10;
  vg.end_version_ = 10;
  vg.border_flag_.set_inclusive_start();
  vg.border_flag_.set_inclusive_end();
  SSTableID max_version;
  ObList<ITableEntity*> tlist;
  bool is_final_minor = false;
  ret = tm.acquire_table(vg, max_version.id, tlist, is_final_minor);
  assert(OB_SUCCESS == ret);
  assert(10 == max_version.major_version);
  assert(3 == tlist.size());
  tm.revert_table(tlist);

  tm.try_dump_memtable();

  ti = tm.get_active_memtable();
  fill_memtable(ti->get_memtable());
  tm.revert_active_memtable(ti);
  uint64_t new_version = 0;
  uint64_t frozen_version = 0;
  uint64_t clog_id = 9;
  int64_t time_stamp = 0;
  bool major_version_changed = false;
  ret = tm.try_freeze_memtable(0, 0, 0, new_version, frozen_version, clog_id, time_stamp, major_version_changed);
  assert(OB_SUCCESS == ret);
  assert(new_version == SSTableID::get_id(12, 1, 1));
  assert(frozen_version == SSTableID::get_id(11, 2, 2));
  assert(major_version_changed);

  tm.try_dump_memtable();

  vg.start_version_ = 10;
  vg.end_version_ = 100;
  vg.border_flag_.set_inclusive_start();
  vg.border_flag_.set_inclusive_end();
  vg.border_flag_.set_max_value();
  ret = tm.acquire_table(vg, max_version.id, tlist, is_final_minor);
  assert(OB_SUCCESS == ret);
  assert(12 == max_version.major_version);
  assert(6 == tlist.size());
  get_table(tm, tlist);

  tm.log_table_info();
  sstm.log_sstable_info();

  tm.try_drop_memtable(true);
  //tm.try_erase_sstable(true);
  tm.try_dump_memtable();
  system("mv /tmp/*.sst /tmp/trash");
  system("mv ~/*.sst ~/trash");
  system("cp *.sst /tmp/");
  system("cp *.sst ~/");
  sstm.umount_store("~");
  sstm.check_broken();
  system("ln -s ~ data/raid1/store1");
  tm.revert_table(tlist);
  tm.log_table_info();
  sstm.log_sstable_info();

  ObString dump_dir(2, 2, (char*)"./");
  tm.dump_memtable2text(dump_dir);

  tm.destroy();

  fprintf(stderr, "MemTableUtils=%ld SSTableUtils=%ld GetParam=%ld SSTableGetter=%ld SSTableScanner=%ld\n",
          sizeof(MemTableUtils), sizeof(SSTableUtils), sizeof(ObGetParam), sizeof(sstable::ObSSTableGetter), sizeof(sstable::ObSSTableScanner));
}

