package com.taobao.ycsb;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.util.Const;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.ObMutatorBase;
import com.taobao.oceanbase.vo.QueryInfo;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;
import com.taobao.oceanbase.vo.UpdateMutator;
import com.taobao.oceanbase.vo.Value;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class ObDB extends DB {

	private static ClientImpl ob_client = null;

	private List<VarCharType> varcharColumns = new ArrayList<VarCharType>();

	private HashMap<String, Type> maps = new HashMap<String, Type>();

	private BasicRowkey maxEndkey;

	public static final String ROWKEY_MAX_LENGTH_PROPERTY = "rowkey_max_length";

	public synchronized static ClientImpl getOBClient() {
		if (ob_client == null) {
			ob_client = new ClientImpl();
			return ob_client;
		} else {
			return ob_client;
		}
	}

	@Override
	public void init() throws DBException {

		super.init();
		getOBClient();
		String max_length = getProperties().getProperty(
				ROWKEY_MAX_LENGTH_PROPERTY);
		int rowkey_max_length = 0;
		if (max_length.equals("")) {
			rowkey_max_length = 16;

		} else {
			int max = Integer.parseInt(max_length);
			if (max <= 0 || max > 64 * 1024) {
				rowkey_max_length = 16;
			} else {
				rowkey_max_length = max;
			}
		}

		byte[] bytes = new byte[rowkey_max_length];
		for (int i = 0; i < rowkey_max_length; i++) {
			bytes[i] = (byte) 0xff;
		}
		this.maxEndkey = new BasicRowkey(bytes);

		String ip = getProperties().getProperty("ip");

		String varchar_columns = getProperties().getProperty("varcharColumns");
		if (varchar_columns.equals("")) {
			// no varchar columns
		} else {
			String[] v_columns = varchar_columns.split(",");

			int i = 0;
			for (i = 0; i < v_columns.length; i++) {

				String[] each = v_columns[i].split(":");
				if (each.length != 2) {
					System.err.println("property:varcharColumns format error");
					System.exit(-1);
				}
				varcharColumns.add(new VarCharType(each[0], Integer
						.parseInt(each[1])));
				maps.put(each[0], Type.VarcharType);
			}
		}

		String int_columns = getProperties().getProperty("intColumns");
		if (int_columns.equals("")) {
			// no int columns
		} else {
			String[] i_columns = int_columns.split(",");

			int i = 0;
			for (i = 0; i < i_columns.length; i++) {

				maps.put(i_columns[i], Type.intType);
			}
		}

		String time_columns = getProperties().getProperty("timeColumns");
		if (time_columns.equals("")) {
			// no time columns
		} else {
			String[] t_columns = time_columns.split(",");

			int i = 0;
			for (i = 0; i < t_columns.length; i++) {
				maps.put(t_columns[i], Type.timeType);
			}
		}
		int port = Integer.parseInt(getProperties().getProperty("port"));
		int timeout = Integer.parseInt(getProperties().getProperty("timeout"));
		//ob_client.setIp(ip);
		//ob_client.setPort(port);
    List<String> instances = new ArrayList<String>();
    instances.add(ip + ":" + port);
    ob_client.setInstanceList(instances);

		ob_client.setTimeout(timeout);
		ob_client.init();

	}

	@Override
	public int delete(String table, String key) {

		BasicRowkey rowkey = null;
		try {
			rowkey = new BasicRowkey(key.getBytes(Const.NO_CHARSET));

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		Result<Boolean> result = ob_client.delete(table, rowkey);
		return result.getResult() == true ? 0 : -1;
	}

	/**
	 * Success 0 failed -1
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {

		BasicRowkey rowkey = null;
		try {
			rowkey = new BasicRowkey(key.getBytes(Const.NO_CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		InsertMutator insertMutator = new InsertMutator(table, rowkey);

		for (String fieldName : values.keySet()) {
			final ByteIterator value = values.get(fieldName);
			Type type = maps.get(fieldName);
			if (type == Type.intType) {
				insertMutator.addColumn(fieldName, new Value() {
					{
						setNumber(Integer.parseInt(value.toString()));
					}

				});
			} else if (type == Type.VarcharType) {
				insertMutator.addColumn(fieldName, new Value() {
					{
						setString(value.toString());
					}

				});
			} else if (type == Type.timeType) {
				// not need to implement
			}
		}

		Result<Boolean> ret = ob_client.insert(insertMutator);

		if (ret == null || !ret.getResult()) {
			System.err.println("insert failed " + ret.getCode());
			return -1;
		}
		return 0;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {

		Set<String> columns = new HashSet<String>();
		// read all fields
		if (fields == null) {
			for (String fieldName : maps.keySet()) {
				columns.add(fieldName);
			}
		}
		BasicRowkey rowkey = null;
		try {
			rowkey = new BasicRowkey(key.getBytes(Const.NO_CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		Result<RowData> ret = ob_client.get(table, rowkey, columns);
		if (ret == null) {
			System.err.println("get failed");
			return -1;
		} else {

			return 0;
		}

	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

		QueryInfo query_info = new QueryInfo();

		BasicRowkey s_rowkey = null;
    //BasicRowkey s_endkey = null;
    //String endkey = (Integer.parseInt(startkey) + recordcount) + "";
		try {
			s_rowkey = new BasicRowkey(startkey.getBytes(Const.NO_CHARSET));
     // s_endkey = new BasicRowkey(endkey.getBytes(Const.NO_CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		query_info.setStartKey(s_rowkey);
    //query_info.setEndKey(s_endkey);
		query_info.setEndKey(this.maxEndkey);
		query_info.setLimit(recordcount);

		for (String fieldName : maps.keySet()) {
			query_info.addColumn(fieldName);
		}

    System.err.println("[SCAN] start_key=" + s_rowkey + ", end_key=" + this.maxEndkey + ",table=" + table
        + ",limit=" + recordcount);
		Result<List<RowData>> results = ob_client.query(table, query_info);

		if (results == null) {
			System.err.println("scan failed");
			return -1;
		} else {
			// fill results?
			return 0;
		}
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {

		List<ObMutatorBase> mutatorList = new ArrayList<ObMutatorBase>();

		BasicRowkey rowkey = null;
		try {
			rowkey = new BasicRowkey(key.getBytes(Const.NO_CHARSET));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		UpdateMutator updateMutator = new UpdateMutator(table, rowkey);

		for (String fieldName : values.keySet()) {
			final ByteIterator value = values.get(fieldName);
			Type type = maps.get(fieldName);
			if (type == Type.intType) {
				updateMutator.addColumn(fieldName, new Value() {
					{
						setNumber(Integer.parseInt(value.toString()));
					}

				}, false);
			} else if (type == Type.VarcharType) {
				updateMutator.addColumn(fieldName, new Value() {
					{
						setString(value.toString());
					}

				}, false);
			} else if (type == Type.timeType) {
				// not need to implement
			}
		}

		mutatorList.add(updateMutator);

		Result<Boolean> ret = ob_client.update(mutatorList);
		if (ret == null || !ret.getResult()) {
			System.err.println("update failed");
			return -1;
		}
		return 0;

	}

}

class VarCharType {
	private String fieldName;
	private int len;

	public VarCharType(String fieldName, int len) {
		super();
		this.fieldName = fieldName;
		this.len = len;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}
}
