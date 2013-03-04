#include "ob_import.h"
#include "ob_import_producer.h"
#include "ob_import_comsumer.h"
#include "ob_import_param.h"

void usage(const char *prog)
{
  fprintf(stderr, "Usage:%s -c [config file] \n"
          "\t-t [table name]\n"
          "\t-h host -p port\n"
          "\t-l [log file]\n"
          "\t-q [queue size, default 1000]\n"
          "\t[-f datafile]\n"
          "\t-g [log level]\n", prog);
}

int run_comsumer_queue(TableParam &param, ObRowBuilder *builder, 
                       OceanbaseDb *db, size_t queue_size)
{
  int ret = OB_SUCCESS;
  ImportProducer producer(param.data_file.c_str(), param.delima, param.rec_delima);
  ImportComsumer comsumer(db, builder, param);

  TBSYS_LOG(INFO, "[delima]: type = %d, part1 = %d, part2 = %d", param.delima.delima_type(), 
            param.delima.part1_, param.delima.part2_);
  TBSYS_LOG(INFO, "[rec_delima]: type = %d, part1 = %d, part2 = %d", param.rec_delima.delima_type(), 
            param.rec_delima.part1_, param.rec_delima.part2_);

  ComsumerQueue<RecordBlock> queue(&producer, &comsumer, queue_size);
  if (queue.produce_and_comsume(1, param.concurrency) != 0) {
    ret = OB_ERROR;
  } else {
    queue.dispose();
  }

  return ret;
}

int do_work(const char *config_file, const char *table_name, 
            const char *host, unsigned short port, size_t queue_size,
            const char *table_datafile)
{
  ImportParam param;
  TableParam table_param;

  int ret = param.load(config_file);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't load config file, please check file path");
    return ret;
  }
  ret = param.get_table_param(table_name, table_param);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "no table=%s in config file, please check it", table_name);
    return ret;
  }

  if (table_datafile != NULL) {                 /* if cmd line specifies data file, use it */
    table_param.data_file = table_datafile;
  }

  if (table_param.data_file.empty()) {
    TBSYS_LOG(ERROR, "no datafile is specified, no work to do, quiting");
    return OB_ERROR;
  }

  OceanbaseDb db(host, port, 8 * kDefaultTimeout);
  ret = db.init();
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "can't init database,%s:%d", host, port);
  }
  ObSchemaManagerV2 *schema = NULL;
  if (ret == OB_SUCCESS) {
    schema = new(std::nothrow) ObSchemaManagerV2;

    if (schema == NULL) {
      TBSYS_LOG(ERROR, "no enough memory for schema");
      ret = OB_ERROR;
    }
  }

  if (ret == OB_SUCCESS) {
    ret = db.fetch_schema(*schema);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't fetch schema from root server [%s:%d]", host, port);
    }
  }

  if (ret == OB_SUCCESS) {
    /* setup ObRowbuilder */
    ObRowBuilder builder(schema, table_name, table_param.input_column_nr, table_param.delima);

    ret = builder.set_column_desc(table_param.col_descs);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't setup column descripes");
    } else {
      ret = builder.set_rowkey_desc(table_param.rowkey_descs);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't setup rowkey descripes");
      }
    }

    if (ret == OB_SUCCESS) {
      ret = run_comsumer_queue(table_param, &builder, &db, queue_size);
    }
  }

  if (schema != NULL) {
    delete schema;
  }

  return ret;
}

int main(int argc, char *argv[])
{
  const char *config_file = NULL;
  const char *table_name = NULL;
  const char *host = NULL;
  const char *log_file = NULL;
  const char *log_level = "INFO";
  const char *data_file = NULL;
  unsigned short port = 0;
  size_t queue_size = 1000;
  int ret = 0;

  while ((ret = getopt(argc, argv, "h:p:t:c:l:g:q:f:")) != -1) {
    switch (ret) {
     case 'c':
       config_file = optarg;
       break;
     case 't':
       table_name = optarg;
       break;
     case 'h':
       host = optarg;
       break;
     case 'l':
       log_file = optarg;
       break;
     case 'g':
       log_level = optarg;
       break;
     case 'p':
       port = static_cast<unsigned short>(atoi(optarg));
       break;
     case 'q':
       queue_size = static_cast<size_t>(atol(optarg)); 
       break;
     case 'f':
       data_file = optarg;
       break;
     default:
       usage(argv[0]);
       exit(0);
       break;
    }
  }

  if (!config_file || !table_name || !host || !port) {
    usage(argv[0]);
    exit(0);
  }

  if (log_file != NULL)
    TBSYS_LOGGER.setFileName(log_file);

  OceanbaseDb::global_init(log_file, log_level);

  return do_work(config_file, table_name, host, port, queue_size, data_file);
}
