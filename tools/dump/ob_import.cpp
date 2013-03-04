#include "ob_import.h"
#include "db_utils.h"
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include <string>
using namespace std;

using namespace oceanbase::common;

ObRowBuilder::ObRowBuilder(ObSchemaManagerV2 *schema, const char *table_name, 
                           int input_column_nr, const RecordDelima &delima)
{
  schema_ = schema;
  table_name_ = table_name;
  memset(columns_desc_, 0, sizeof(columns_desc_));
  memset(rowkey_desc_, 0, sizeof(rowkey_desc_));
  input_column_nr_ = input_column_nr;
  atomic_set(&lineno_, 0);
  rowkey_max_size_ = OB_MAX_ROW_KEY_LENGTH;
  delima_ = delima;
}


ObRowBuilder::~ObRowBuilder()
{
  TBSYS_LOG(INFO, "Processed lines = %d", atomic_read(&lineno_));
}

int ObRowBuilder::set_column_desc(std::vector<ColumnDesc> &columns)
{
  int ret = OB_SUCCESS;

  columns_desc_nr_ = static_cast<int32_t>(columns.size());
  for (size_t i = 0; i < columns.size(); i++) {
    const ObColumnSchemaV2 *col_schema = 
      schema_->get_column_schema(table_name_, columns[i].name.c_str());

    int offset = columns[i].offset;
    if (offset >= input_column_nr_) {
      TBSYS_LOG(ERROR, "wrong config table=%s, columns=%s", table_name_, columns[i].name.c_str());
      ret = OB_ERROR;
      break;
    }

    if (col_schema) {
      //ObModifyTimeType, ObCreateTimeType update automaticly, skip
      if (col_schema->get_type() != ObModifyTimeType &&
          col_schema->get_type() != ObCreateTimeType)
        columns_desc_[offset].schema = col_schema;
      else                   
        columns_desc_[offset].schema = NULL;
    } else {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "column:%s is not a legal column in table %s", 
                columns[i].name.c_str(), table_name_);
      break;
    }
  }

  return ret;
}


int ObRowBuilder::set_rowkey_desc(std::vector<RowkeyDesc> &rowkeys)
{
  int ret = OB_SUCCESS;

  const ObTableSchema *tab_schema = schema_->get_table_schema(table_name_);
  if (tab_schema == NULL) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "no such table in schema");
  } else {
    rowkey_max_size_  = tab_schema->get_rowkey_max_length();
  }

  if (ret == OB_SUCCESS) {
    rowkey_desc_nr_ = rowkeys.size();

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      rowkey_desc_[i].pos = -1;
    }

    for(size_t i = 0;i < rowkeys.size(); i++) {
      int pos = rowkeys[i].pos;
      int offset = rowkeys[i].offset;

      if (offset >= input_column_nr_) {
        TBSYS_LOG(ERROR, "wrong config, table=%s, offset=%d", table_name_, offset);
        ret = OB_ERROR;
        break;
      }
      rowkey_desc_[pos] = rowkeys[i];
    }

    for (size_t i = 0;i < rowkey_desc_nr_;i++) {
      if (rowkey_desc_[i].pos == -1) {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "rowkey config error, intervals in it, pos=%ld, please check it", i);
        break;
      }
    }
  }

  return ret;
}

bool ObRowBuilder::check_valid()
{
  bool valid = true;

  if (input_column_nr_ <= 0) {
    TBSYS_LOG(ERROR, "input data file has 0 column");
    valid = false;
  }

  return valid;
}


int ObRowBuilder::build_tnx(RecordBlock &block, DbTranscation *tnx)
{
  int ret = OB_SUCCESS;
  int token_nr = kMaxRowkeyDesc + OB_MAX_COLUMN_NUMBER;
  TokenInfo tokens[token_nr];

  Slice slice;
  block.reset();
  while (block.next_record(slice)) {
    Tokenizer::tokenize(slice, delima_, token_nr, tokens);
    if (token_nr != input_column_nr_) {
      TBSYS_LOG(ERROR, "corrupted data files, please check it, input_col_nr=%d, rear_nr=%d", input_column_nr_, token_nr);
      ret = OB_ERROR;
      break;
    }

    atomic_add(1, &lineno_);

    ObString rowkey;
    ret = create_rowkey(rowkey, tokens);
    if (ret != OB_SUCCESS) {
      TBSYS_LOG(ERROR, "can't rowkey, quiting");
    }

    RowMutator *mutator = NULL;
    if (ret == OB_SUCCESS) {
      ret = tnx->insert_mutator(table_name_, rowkey, mutator);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't create insert mutator , table=%s", table_name_);
      }
    }

    if (ret == OB_SUCCESS) {
      ret = setup_content(mutator, tokens);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't transcation content, table=%s", table_name_);
      }
    }

    if (mutator != NULL) {
      tnx->free_row_mutator(mutator);
    }

    if (ret != OB_SUCCESS)
      break;
  }

  return ret;
}

int ObRowBuilder::create_rowkey(ObString &rowkey, TokenInfo *tokens)
{
  int ret = OB_SUCCESS;
  char *buffer = NULL;
  int64_t pos = 0;

  ObMemBuf *mbuf = GET_TSI_MULT(ObMemBuf, TSI_MBUF_ID);
  if (mbuf == NULL || mbuf->ensure_space(rowkey_max_size_)) {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "can't create object from TSI, or ObMemBuff can't alloc memory");
  }

  if (ret == OB_SUCCESS) {
    int64_t buff_len = mbuf->get_buffer_size();
    buffer = mbuf->get_buffer();

    for(size_t i = 0;i < rowkey_desc_nr_; i++) {
      RowkeyDesc &desc = rowkey_desc_[i];

      TokenInfo *token = &tokens[desc.offset];
      switch (desc.type) {
       case INT8:
         {
           int8_t v = 0;
           if (token->len != 0) {
             v = static_cast<int8_t>(atoi(token->token));
           }
           ret = serialization::encode_i8(buffer, buff_len, pos, v);

           if (ret != OB_SUCCESS) {
             TBSYS_LOG(ERROR, "can't serialize rowkey, INT8, pos=%zu",i );
           }
         }
         break;
       case INT16:
         {
           int16_t v = 0;
           if (token->len != 0) {
             v = static_cast<int16_t>(atoi(token->token));
           }

           ret = serialization::encode_i16(buffer, buff_len, pos, v);
           if (ret != OB_SUCCESS) {
             TBSYS_LOG(ERROR, "can't serialize rowkey, INT16, pos=%zu", i);
           }
         }
         break;
       case INT64:
         {
           int64_t v = 0;
           if (token->len != 0) {
             v = static_cast<int64_t>(atol(token->token));
           }

           ret = serialization::encode_i64(buffer, buff_len, pos, v);
           if (ret != OB_SUCCESS) {
             TBSYS_LOG(ERROR, "can't serialize rowkey, INT64, pos=%zu",i);
           }
         }
         break;
       case INT32:
         {
           int32_t v = 0;
           if (token->len != 0) {
             v = static_cast<int32_t>(atoi(token->token));
           }

           ret = serialization::encode_i32(buffer, buff_len, pos, v);
           if (ret != OB_SUCCESS) {
             TBSYS_LOG(ERROR, "can't serialize rowkey, INT32, pos=%zu", i);
           }
         }
         break;
       case VARCHAR:
         {
           if ((buff_len - pos) < token->len) {
             ret = OB_ERROR;
             TBSYS_LOG(ERROR, "can't serialize rowkey VARCHR, buff_len=%ld, token_len = %ld",
                       buff_len, token->len);
           } else {
             memcpy(buffer + pos, token->token, token->len);
             pos += token->len;
           }
           TBSYS_LOG(DEBUG, "VARCHAR token=%s, len=%ld", token->token, token->len);
         }
         break;
       case DATETIME:
         {
           ObDateTime t;
           ret = transform_date_to_time(token->token, static_cast<int32_t>(token->len), t);
           if (OB_SUCCESS != ret)
           {
             TBSYS_LOG(ERROR, "transform_date_to_time error");
           }
           else
           {
             ret = serialization::encode_i64(buffer, buff_len, pos,
                  t * 1000 * 1000L);
             if (ret != OB_SUCCESS) {
               TBSYS_LOG(ERROR, "can't serialize rowkey, INT64, pos=%ld",i);
             }
           }
         }
         break;
       default:
         TBSYS_LOG(ERROR, "not supported rowkey type");
      }

      if (ret != OB_SUCCESS) {                  /* break loop */
        break;
      }
    }
  }

  if (ret == OB_SUCCESS) {
    rowkey.assign_ptr(buffer, static_cast<int32_t>(pos));
  }

  return ret;
}

int ObRowBuilder::make_obobj(int token_idx, ObObj &obj, TokenInfo *tokens)
{
  int type = columns_desc_[token_idx].schema->get_type();
  const char *token = tokens[token_idx].token;
  int token_len = static_cast<int32_t>(tokens[token_idx].len);
  int ret = OB_SUCCESS;

  switch(type) {
   case ObIntType:
     {
       int64_t lv = 0;
       if (token_len != 0)
         lv = atol(token);

       obj.set_int(lv);
     }
     break;
   case ObFloatType:
     if (token_len != 0)
       obj.set_float(strtof(token, NULL));
     else
       obj.set_float(0);
     break;

   case ObDoubleType:
     if (token_len != 0)
       obj.set_double(strtod(token, NULL));
     else
       obj.set_double(0);
     break;
   case ObDateTimeType:
     {
       ObDateTime t = 0;
       ret = transform_date_to_time(token, token_len, t);
       if (OB_SUCCESS != ret)
       {
         TBSYS_LOG(ERROR,"transform_date_to_time error");
       }
       else
       {
         obj.set_datetime(t);
       }
     }
     break;
   case ObVarcharType:
     {
       ObString bstring;
       bstring.assign_ptr(const_cast<char *>(token),token_len);
       obj.set_varchar(bstring);
     }
     break;
   case ObPreciseDateTimeType:
     {
       ObDateTime t = 0;
       ret = transform_date_to_time(token, token_len, t);
       if (OB_SUCCESS != ret)
       {
         TBSYS_LOG(ERROR,"transform_date_to_time error");
       }
       else
       {
         obj.set_precise_datetime(t * 1000 * 1000L); //seconds -> ms
       }
     }
     break;
   default:
     TBSYS_LOG(ERROR,"unexpect type index: %d", type);
     ret = OB_ERROR;
     break;
  }

  return ret;
}

int ObRowBuilder::setup_content(RowMutator *mutator, TokenInfo *tokens)
{
  int ret = OB_SUCCESS;

  for(int i = 0;i < OB_MAX_COLUMN_NUMBER && ret == OB_SUCCESS; i++) {
    const ObColumnSchemaV2 *schema = columns_desc_[i].schema;
    if (NULL == schema) {
      continue;
    }

    ObObj obj;
    ret = make_obobj(i, obj, tokens);
    if (ret == OB_SUCCESS) {
      ret = mutator->add(schema->get_name(), obj);
      if (ret != OB_SUCCESS) {
        TBSYS_LOG(ERROR, "can't add column to row mutator");
        break;
      } else {
        TBSYS_LOG(DEBUG, "obj idx=%d, name=%s", i, schema->get_name());
        obj.dump();
      }
    } else {
      TBSYS_LOG(ERROR, "can't make obobj, column=%s", schema->get_name());
    }
  }

  return ret;
}

