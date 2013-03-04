package com.taobao.ycsb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

public class ObWorkload extends Workload {

	public static final String READ_PROPORTION_PROPERTY = "readproportion";

	/**
	 * The default proportion of transactions that are reads.
	 */
	public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

	/**
	 * The name of the property for the proportion of transactions that are
	 * updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

	/**
	 * The default proportion of transactions that are updates.
	 */
	public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

	/**
	 * The name of the property for the proportion of transactions that are
	 * inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

	/**
	 * The default proportion of transactions that are inserts.
	 */
	public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the property for the proportion of transactions that are
	 * scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

	/**
	 * The default proportion of transactions that are scans.
	 */
	public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

	public static final String DEL_PROPORTION_PROPERTY = "delproportion";

	public static final String DEL_PROPORTION_PROPERTY_DEFAULT = "0.0";

	/**
	 * The name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY = "table";

	/**
	 * The default name of the database table to run queries against.
	 */
	public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

	public static final String ROWKEY_IS_FIXED_LENGTH_PROPERTY = "rowkey_is_fixed_length";

	public static final String ROWKEY_IS_FIXED_LENGTH_PROPERTY_DEFAULT = "1";

	public static final String ROWKEY_MAX_LENGTH_PROPERTY = "rowkey_max_length";

	public static final String SCAN_MAX_LENGTH_PROPERTY = "scan_max_length";
	public static final String SCAN_MAX_LENGTH_PROPERTY_DEFAULT = "200";

  public static final String INSERT_START_PROPERTY = "insertstart";
  public static final String INSERT_START_PROPERTY_DEFAULT = "1";

	// schema info
	private List<VarCharType> varcharColumns = new ArrayList<VarCharType>();
	private List<String> intColumns = new ArrayList<String>();
	private List<String> timeColumns = new ArrayList<String>();
	private List<UniformIntegerGenerator> varcharLengthGenerator = new ArrayList<UniformIntegerGenerator>();
	private List<UniformIntegerGenerator> intValueGenerator = new ArrayList<UniformIntegerGenerator>();

	private int rowkey_is_fixed_length;
	private int rowkey_max_length;

	private int scan_max_length;
	private IntegerGenerator rowkeyLengthGenerator;
	private DiscreteGenerator operationchooser;

	// private IntegerGenerator insertSequence;
	// private CounterGenerator transactionInsertSequence;

	private IntegerGenerator scanLengthGenerator;

	// private IntegerGenerator keySelector;

	private String table;

	@Override
	public void init(Properties p) throws WorkloadException {
		super.init(p);

		operationchooser = new DiscreteGenerator();

		double readproportion = Double.parseDouble(p.getProperty(
				READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));

		if (readproportion > 0) {
			operationchooser.addValue(readproportion, "READ");
		}

		double updateproportion = Double
				.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,
						UPDATE_PROPORTION_PROPERTY_DEFAULT));

		if (updateproportion > 0) {
			operationchooser.addValue(updateproportion, "UPDATE");
		}

		double insertproportion = Double
				.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,
						INSERT_PROPORTION_PROPERTY_DEFAULT));

		if (insertproportion > 0) {
			operationchooser.addValue(insertproportion, "INSERT");
		}

		double scanproportion = Double.parseDouble(p.getProperty(
				SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
		if (scanproportion > 0) {
			operationchooser.addValue(scanproportion, "SCAN");
		}
		double delproportion = Double.parseDouble(p.getProperty(
				DEL_PROPORTION_PROPERTY, DEL_PROPORTION_PROPERTY_DEFAULT));
		if (delproportion > 0) {
			operationchooser.addValue(delproportion, "DELETE");
		}

		this.scan_max_length = Integer.parseInt(p.getProperty(
				SCAN_MAX_LENGTH_PROPERTY, SCAN_MAX_LENGTH_PROPERTY_DEFAULT));
		this.scanLengthGenerator = new UniformIntegerGenerator(1,
				this.scan_max_length);

		// this.recordcount = Integer.parseInt(p.getProperty(RECORD_COUNT));

    int insertstart = Integer.parseInt(p.getProperty(
          INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
		// used by doInsert()
		// this.insertSequence = new CounterGenerator(insertstart);
		//
		// this.transactionInsertSequence = new CounterGenerator(recordcount);

		// Generate a popularity distribution of items, skewed to favor recent
		// items significantly more than older items
		// this.keychooser = new
		// SkewedLatestGenerator(transactionInsertSequence);

		this.table = p.getProperty(TABLENAME_PROPERTY,
				TABLENAME_PROPERTY_DEFAULT);

		// this.keySelector = new UniformIntegerGenerator(1, this.recordcount);

		this.rowkey_is_fixed_length = Integer.parseInt(p.getProperty(
				ROWKEY_IS_FIXED_LENGTH_PROPERTY,
				ROWKEY_IS_FIXED_LENGTH_PROPERTY_DEFAULT));
		if (this.rowkey_is_fixed_length != 0
				&& this.rowkey_is_fixed_length != 1) {
			System.err
					.println("parameter rowkey_is_fixed_length error, must be 0 or 1");
			System.exit(-1);
		}
		this.rowkey_max_length = Integer.parseInt(p
				.getProperty(ROWKEY_MAX_LENGTH_PROPERTY));

		if (this.rowkey_max_length > 64 * 1024 || this.rowkey_max_length <= 0) {
			System.err
					.println("parameter rowkey_max_length must be less than 64K, greater than 0");
			System.exit(-1);
		}

		if (this.rowkey_is_fixed_length == 1) {
			this.rowkeyLengthGenerator = new ConstantIntegerGenerator(
					this.rowkey_max_length);
		} else {
			this.rowkeyLengthGenerator = new UniformIntegerGenerator(1,
					this.rowkey_max_length);
		}

		String varchar_columns = p.getProperty("varcharColumns");
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
				varcharLengthGenerator.add(new UniformIntegerGenerator(1,
						Integer.parseInt(each[1])));
			}
		}

		String int_columns = p.getProperty("intColumns");
		if (int_columns.equals("")) {
			// no int columns
		} else {
			String[] i_columns = int_columns.split(",");

			int i = 0;
			for (i = 0; i < i_columns.length; i++) {

				intColumns.add(i_columns[i]);
				intValueGenerator.add(new UniformIntegerGenerator(0, 100));
			}
		}

		String time_columns = p.getProperty("timeColumns");
		if (time_columns.equals("")) {
			// no time columns
		} else {
			String[] t_columns = time_columns.split(",");

			int i = 0;
			for (i = 0; i < t_columns.length; i++) {
				timeColumns.add(t_columns[i]);
			}
		}

	}

	@Override
	public boolean doInsert(DB db, Object threadstate) {

		ByteIterator rowkey = new RandomByteIterator(
				rowkeyLengthGenerator.nextInt());

		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

		int i = 0;

		for (i = 0; i < varcharColumns.size(); ++i) {
			ByteIterator fieldValue = new RandomByteIterator(
					varcharLengthGenerator.get(i).nextInt());
			values.put(varcharColumns.get(i).getFieldName(), fieldValue);
		}
		for (i = 0; i < intColumns.size(); ++i) {
			EncodeIntegerIterator value = new EncodeIntegerIterator(
					intValueGenerator.get(i).nextInt());
			values.put(intColumns.get(i), value);

		}
		return db.insert(table, rowkey.toString(), values) == 0 ? true : false;

	}

	/**
	 * threadstate not used
	 */
	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		// choose one kind of operation
		String op = operationchooser.nextString();

		if (op.compareTo("READ") == 0) {
			doTransactionRead(db);
		} else if (op.compareTo("UPDATE") == 0) {
			doTransactionUpdate(db);
		} else if (op.compareTo("INSERT") == 0) {
			doTransactionInsert(db);
		} else if (op.compareTo("SCAN") == 0) {
			doTransactionScan(db);
		} else if (op.compareTo("DELETE") == 0) {
			doTransactionDel(db);
		}

		return true;
	}

	public void doTransactionDel(DB db) {
		//ByteIterator rowkey = new RandomByteIterator(
	//			rowkeyLengthGenerator.nextInt());
	//	db.delete(table, rowkey.toString());
	}

	public void doTransactionRead(DB db) {

		ByteIterator rowkey = new RandomByteIterator(
				rowkeyLengthGenerator.nextInt());
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		db.read(table, rowkey.toString(), null, result);

	}

	/**
	 * update all fields
	 * 
	 * @param db
	 */
	public void doTransactionUpdate(DB db) {

		ByteIterator rowkey = new RandomByteIterator(
				rowkeyLengthGenerator.nextInt());
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

		int i = 0;

		for (i = 0; i < varcharColumns.size(); ++i) {
			ByteIterator fieldValue = new RandomByteIterator(
					varcharLengthGenerator.get(i).nextInt());
			values.put(varcharColumns.get(i).getFieldName(), fieldValue);
		}
		for (i = 0; i < intColumns.size(); ++i) {
			EncodeIntegerIterator value = new EncodeIntegerIterator(
					intValueGenerator.get(i).nextInt());
			values.put(intColumns.get(i), value);

		}

		db.update(table, rowkey.toString(), values);

	}

	public void doTransactionInsert(DB db) {

		ByteIterator rowkey = new RandomByteIterator(
				rowkeyLengthGenerator.nextInt());

		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

		int i = 0;

		for (i = 0; i < varcharColumns.size(); ++i) {
			ByteIterator fieldValue = new RandomByteIterator(
					varcharLengthGenerator.get(i).nextInt());
			values.put(varcharColumns.get(i).getFieldName(), fieldValue);
		}
		for (i = 0; i < intColumns.size(); ++i) {
			EncodeIntegerIterator value = new EncodeIntegerIterator(
					intValueGenerator.get(i).nextInt());
			values.put(intColumns.get(i), value);

		}

		db.insert(table, rowkey.toString(), values);
	}

	public void doTransactionScan(DB db) {

		int scanCount = this.scanLengthGenerator.nextInt();
    int startkey = rowkeyLengthGenerator.nextInt();

		ByteIterator rowkey = new RandomByteIterator(startkey);

		Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
		db.scan(table, rowkey.toString(), scanCount, null, results);

	}

}
