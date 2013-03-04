package com.taobao.mrsstable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.taobao.mrsstable.MRGenSstable;
import com.taobao.mrsstable.TextInputSampler;

public class MRGenSstable extends Configured implements Tool {

	private static String PRIMARY_DELIM;
	public enum ColumnType {INT8, INT16, INT32, LONG, STRING, DATETIME}
	
	private static final String LIB_MRSSTABLE_NAME = "libmrsstable.so";
	private static final String LIB_NONE_OB_NAME = "libnone.so";	
	private static final String LIB_LZO_ORG_NAME = "liblzo2.so";
	private static final String LIB_LZO_OB_NAME = "liblzo_1.0.so";
	private static final String LIB_SNAPPY_ORG_NAME = "libsnappy.so";
	private static final String LIB_SNAPPY_OB_NAME = "libsnappy_1.0.so";
	
	private static final String RANGE_FILE_NAME = "config/range.lst";
	private static final String PARTITIOIN_FILE_NAME = "_partition.lst";
	private static final String CONFIG_DIR = "config";

	public static String getEscapeStr(Configuration conf, String confSetting,
			String defaultSetting) {
		String delim = conf.get(confSetting);
		return delim == null ? defaultSetting : StringEscapeUtils
				.unescapeJava(delim);
	}
	
	public static class RowKeyDesc {
		private static final String INDEX_DELIM = "-";
		private int[] orgColumnIndex;
		private int[] binColumnIndex;
		private int[] columnType;
		
		public RowKeyDesc buildRowKeyDesc(JobConf conf) {
			try {
				String[] rowKeyDesc = conf.get("mrsstable.rowkey.desc", "0-0-0")
					.split(",", -1);
				orgColumnIndex = new int[rowKeyDesc.length];
				binColumnIndex = new int[rowKeyDesc.length];
				columnType = new int[rowKeyDesc.length];			
				
				for (int i = 0; i < rowKeyDesc.length; i++) {
					String[] columnDesc = rowKeyDesc[i].split(INDEX_DELIM, -1);
					if (columnDesc.length == 3) {		
						binColumnIndex[i] = Integer.parseInt(columnDesc[0]);
						if (binColumnIndex[i] >= rowKeyDesc.length
								|| (i > 0 && binColumnIndex[i] <= binColumnIndex[i - 1])) {
							throw new Exception("invalid column index in rowkey :" + binColumnIndex[i]
							         + "column count in rowkey: " + rowKeyDesc.length
							         + "rowkey_desc='col_idx_in_rowkey','col_idx_in_org_line','col_type'");
						}
						orgColumnIndex[i] = Integer.parseInt(columnDesc[1]);																		
						columnType[i] = Integer.parseInt(columnDesc[2]);
					} else {
						throw new Exception("each column desc of rowkey must include 3 part");
					}
				}
			} catch (NumberFormatException e) {
				usage("Parameter mrsstable.rowkey.desc is not set properly. "
						+ "Use comma seperated number list");
			} catch (Exception e) {
				e.printStackTrace();
			}
			assert (orgColumnIndex.length > 0);		
			
			return this;
		}
		
		public int[] getOrgColumnIndex() {
			return orgColumnIndex;
		}
		
		public int[] getBinColumnIndex() {
			return binColumnIndex;
		}
		
		public int[] getColumnType() {
			return columnType;
		}
	}

	public static class TokenizerMapper implements
			Mapper<Object, Text, Text, Text> {

		private final Log LOG = LogFactory.getLog(TokenizerMapper.class);
		private static String PRIMARY_DELIM;
		private Text k = new Text();
		RowKeyDesc rowKeyDesc;
		private int[] keyColumnIndex;
		private int minValueColumnIdx = 0;
		boolean isSkipInvalidRow = true;

		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] vec = value.toString().split(PRIMARY_DELIM, -1);
			if (vec.length > minValueColumnIdx) 
			{
				if (isSkipInvalidRow && !isRowkeyValid(vec)) {
					StringBuilder err_sb = new StringBuilder("invalid rowkey, skip this line, ");
					for (int i = 0; i < keyColumnIndex.length; i++) {
						err_sb.append("[index:" + i + ", type:" + rowKeyDesc.getColumnType()[i] 
						    + ", val:" + vec[keyColumnIndex[i]] + "], ");
					}					
					LOG.info(err_sb.toString());
					return;
				}
				
				StringBuilder sb = new StringBuilder(vec[keyColumnIndex[0]]);
				for (int i = 1; i < keyColumnIndex.length; i++) {
					sb.append(PRIMARY_DELIM).append(vec[keyColumnIndex[i]]);
				}
				k.set(sb.toString());
				output.collect(k, value);
			} else {
				LOG.info("value hasn't enough columns " + "vec.length=" +
						vec.length + "minValueColumnNum=" + (minValueColumnIdx + 1));
			}
		}

		@Override
		public void configure(JobConf conf) {
			PRIMARY_DELIM = getEscapeStr(conf, "mrsstable.primary.delimeter",
					"\001");
			String skipInvalidRow = conf.get("mrsstable.skip.invalid.row", "1");
			if (skipInvalidRow.equalsIgnoreCase("0")) {
				isSkipInvalidRow = false;
			}
			rowKeyDesc = new RowKeyDesc();
			keyColumnIndex = rowKeyDesc.buildRowKeyDesc(conf).getOrgColumnIndex();
			if (keyColumnIndex.length <= 0) {
				throw new IllegalArgumentException("key columns count must greater than 0" +
						keyColumnIndex.length);
			}
			for (int i = 1; i < keyColumnIndex.length; i++) {
				if (keyColumnIndex[i] > minValueColumnIdx) {
					minValueColumnIdx = keyColumnIndex[i];
				}
			}			
		}
		
		public boolean isRowkeyValid(String[] vec) {
			boolean is_valid = true;
			
			for (int i = 0; i < keyColumnIndex.length; ++i) {
				if (rowKeyDesc.getColumnType()[i] == ColumnType.LONG.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT32.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT16.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT8.ordinal()) {
					try {
						Long.parseLong(vec[keyColumnIndex[i]]);
					} catch (NumberFormatException e) {
						is_valid = false;
						break;
					}
				} else if (rowKeyDesc.getColumnType()[i] == ColumnType.DATETIME.ordinal()) {
					SimpleDateFormat df;
					
					if (vec[keyColumnIndex[i]].indexOf("-") > -1) {
						if (vec[keyColumnIndex[i]].indexOf(":") > -1) {
							df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						} else {
							df = new SimpleDateFormat("yyyy-MM-dd");				
						}
					} else {
						if (vec[keyColumnIndex[i]].indexOf(":") > -1) {
							df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");				
						} else if (vec[keyColumnIndex[i]].length() > 8) {
							df = new SimpleDateFormat("yyyyMMddHHmmss");						
						} else {
							df = new SimpleDateFormat("yyyyMMdd");				
						}
					}
					
					try {
			      df.parse(vec[keyColumnIndex[i]]);
		      } catch (ParseException e) {
		      	is_valid = false;
		      	break;
		      }				
				}
			}			
			
			return is_valid;
		}

		@Override
		public void close() throws IOException {

		}
	}

	public static class SSTableReducer implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void configure(JobConf conf) {
		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public void reduce(Text key, Iterator<Text> value,
				OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {
			while (value.hasNext()) {
				output.collect(key, value.next());
			}
		}
	}

	public static class TaggedKeyComparator extends Configured implements
			RawComparator<Text> {

		private static String PRIMARY_DELIM;
		RowKeyDesc rowKeyDesc;
		private boolean is_inited = false;

		public void init() {
			if (!is_inited) {
				if (PRIMARY_DELIM == null) {
					PRIMARY_DELIM = getEscapeStr(getConf(),
							"mrsstable.primary.delimeter", "\001");
				}
				
				if (rowKeyDesc == null) {
					rowKeyDesc = new RowKeyDesc();
					rowKeyDesc.buildRowKeyDesc((JobConf)getConf());
				}
				is_inited = true;
			}
		}
		
		public long getTaggedInteger(String str) {
			long rtval;
			try {
				rtval = Long.parseLong(str);
			} catch (NumberFormatException e) {
				rtval = 0;
			}
			
			return rtval;
		}
		
		public Date getTaggedDate(String str) {
			SimpleDateFormat df;
			Date date;
			
			if (str.indexOf("-") > -1) {
				if (str.indexOf(":") > -1) {
					df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				} else {
					df = new SimpleDateFormat("yyyy-MM-dd");				
				}
			} else {
				if (str.indexOf(":") > -1) {
					df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");				
				} else if (str.length() > 8) {
					df = new SimpleDateFormat("yyyyMMddHHmmss");						
				} else {
					df = new SimpleDateFormat("yyyyMMdd");				
				}
			}
			
			try {
	      date = df.parse(str);
      } catch (ParseException e) {
      	date = new Date();
	      e.printStackTrace();
      }
      
			return date;
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);	
			Text left = new Text(new String(b1, s1 + n1, l1 - n1));
			Text right = new Text(new String(b2, s2 + n2, l2 - n2));
			if (left == null || right == null) {
				return -1;
			} else {
				return compare(left, right);
			}
		}
		
		public int bytesCompare(byte[] left, byte[] right) {
			int i = 0;
			int len = left.length > right.length ? right.length : left.length;
			
			for (i = 0; i < len; ++i) {
				if (left[i] != right[i]) {
					if (left[i] < 0 && right[i] >= 0) return 1;
					else if (left[i] >= 0 && right[i] < 0) return -1;
					else return left[i] - right[i] > 0 ? 1 : -1;
				}
			}
			
			if (i == len) {
				if (left.length > right.length) {
					return 1;
				}
				else if (left.length == right.length) {
					return 0;
				}
				else 
				{
					return -1;
				}
			}
			
			return 0;
		}

		@Override
		public int compare(Text left, Text right) {
			if (!is_inited) {
				init();
			}
			
			String[] left_columns = left.toString().split(PRIMARY_DELIM, -1);
			String[] right_columns = right.toString().split(PRIMARY_DELIM, -1);
			
			for (int i = 0; i < rowKeyDesc.getBinColumnIndex().length; ++i) {
				if (rowKeyDesc.getColumnType()[i] == ColumnType.LONG.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT32.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT16.ordinal()
						|| rowKeyDesc.getColumnType()[i] == ColumnType.INT8.ordinal()) {
					long left_val = getTaggedInteger(left_columns[i]);
					long right_val = getTaggedInteger(right_columns[i]);
					if (left_val != right_val) {
						if ((left_val >= 0 && right_val >= 0) 
								|| (left_val < 0 && right_val < 0)) { 
							return left_val - right_val > 0 ? 1 : -1;
						} else if ((left_val >= 0 && right_val < 0) 
								|| (left_val < 0 && right_val >= 0)) {
							return left_val - right_val < 0 ? 1 : -1;
						}
					}
				} else if (rowKeyDesc.getColumnType()[i] == ColumnType.STRING.ordinal()) {
					int ret = 0;
          try {
	          ret = bytesCompare(left_columns[i].getBytes("UTF-8"), 
	          		right_columns[i].getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
          	System.err.printf("failed to decode string with UTF-8, left_column: "
          			+ left_columns[i] + " rigth_column: " + right_columns[i]);
	          e.printStackTrace();
          }
					if (0 != ret)	{
						return ret;
					}
				} else if (rowKeyDesc.getColumnType()[i] == ColumnType.DATETIME.ordinal()) {
					Date left_val = getTaggedDate(left_columns[i]);
					Date right_val = getTaggedDate(right_columns[i]);
					int ret = left_val.compareTo(right_val);
					if (0 != ret)	{
						return ret;
					}					
				}
			}
			
			return 0;
		}
	}

	/**
	 * Partitioner effecting a total order by reading split points from an
	 * externally generated source.
	 */
  public static class TotalOrderPartitioner
			implements Partitioner<Text, Text> {

  	private static String RANGE_FILE_DELIM;
		private Node<Text> partitions;
		private Node<Text> ranges;

		public TotalOrderPartitioner() {
		}

		/**
		 * Read in the partition file and build indexing data structures. 
		 * Keys will be located using a binary
		 * search of the partition keyset using the
		 * {@link org.apache.hadoop.io.RawComparator} defined for this job. The
		 * input file must be sorted with the same comparator and contain
		 * {@link org.apache.hadoop.mapred.JobConf#getNumReduceTasks} - 1 keys.
		 */
		@SuppressWarnings("unchecked")
		// keytype from conf not static
		public void configure(JobConf job) {
			try {
				final Path partFile = new Path(PARTITIOIN_FILE_NAME);
				final Path rangeFile = new Path(RANGE_FILE_NAME);
				final FileSystem fs =FileSystem.getLocal(job); // assume in DistributedCache
				RANGE_FILE_DELIM = getEscapeStr(job, "mrsstable.range.file.delimeter", " ");
				Text[] splitPoints;
					
				RawComparator<Text> comparator = (RawComparator<Text>) job
					.getOutputKeyComparator();
				String samplerType = job.get("mrsstable.presort.sampler", "specify");
				if (samplerType.equals("specify") && fs.exists(rangeFile))
				{
					splitPoints = readPartitions(fs, rangeFile, job);
					Arrays.sort(splitPoints, comparator);
				} else if (!samplerType.equals("specify") && fs.exists(partFile)) {
					splitPoints = readPartitions(fs, partFile, job);
				} else {
					throw new IOException("partition file or range file don't exist " +
							"in distribute cache, sampler type: " + samplerType);
				}
					
				for (int i = 0; i < splitPoints.length - 1; ++i) {
					if (comparator.compare(splitPoints[i], splitPoints[i + 1]) >= 0) {
						throw new IOException("Split points are out of order");
					}
				}		
				
				int reduceNum = job.getNumReduceTasks();				
				if (splitPoints.length > reduceNum - 1) {
					float stepSize = splitPoints.length / (float) reduceNum;
					int last = 0;
					ArrayList<Text> partsPointsList = new ArrayList<Text>();
					for (int i = 1; i < reduceNum; ++i) {
						int k = Math.round(stepSize * i);
						while (comparator.compare(splitPoints[last], splitPoints[k]) >= 0) {
							++k;
						}
						partsPointsList.add(splitPoints[k]);
						last = k;
					}
					Text[] partsPoints = partsPointsList.toArray(new Text[partsPointsList.size()]);
					partitions = new BinarySearchNode(partsPoints, comparator);	
				} else {
					partitions = new BinarySearchNode(splitPoints, comparator);					
				}	
				ranges = new BinarySearchNode(splitPoints, comparator);
			} catch (IOException e) {
				throw new IllegalArgumentException("Can't read partitions file", e);
			}
		}

		public int getPartition(Text key, Text value, int numPartitions) {
			return partitions.findPartition(key);
		}
		
		public int getRange(Text key) {
			return ranges.findPartition(key);
		}		

		/**
		 * Set the path to the SequenceFile storing the sorted partition keyset.
		 * It must be the case that for <tt>R</tt> reduces, there are
		 * <tt>R-1</tt> keys in the SequenceFile.
		 */
		public static void setPartitionFile(JobConf job, Path p) {
			job.set("total.order.partitioner.path", p.toString());
		}	

		/**
		 * Get the path to the SequenceFile storing the sorted partition keyset.
		 * 
		 * @see #setPartitionFile(JobConf,Path)
		 */
		public static String getPartitionFile(JobConf job) {
			return job.get("total.order.partitioner.path", PARTITIOIN_FILE_NAME);
		}

		/**
		 * Interface to the partitioner to locate a key in the partition keyset.
		 */
		interface Node<T> {
			/**
			 * Locate partition in keyset K, st [Ki..Ki+1) defines a partition,
			 * with implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
			 */
			int findPartition(T key);
		}

		/**
		 * For types that are not {@link org.apache.hadoop.io.BinaryComparable}
		 * or where disabled by <tt>total.order.partitioner.natural.order</tt>,
		 * search the partition keyset with a binary search.
		 */
		class BinarySearchNode implements Node<Text> {
			private final Text[] splitPoints;
			private final RawComparator<Text> comparator;

			BinarySearchNode(Text[] splitPoints, RawComparator<Text> comparator) {
				this.splitPoints = splitPoints;
				this.comparator = comparator;
			}

			public int findPartition(Text key) {
				final int pos = Arrays.binarySearch(splitPoints, key,
						comparator) + 1;
				return (pos <= 0) ? -pos : pos - 1;
			}
		}

		/**
		 * Read the cut points from the given IFile.
		 * 
		 * @param fs
		 *            The file system
		 * @param p
		 *            The path to read
		 * @param job
		 *            The job config
		 * @throws IOException
		 */
		// matching key types enforced by passing in
		// map output key class
		private Text[] readPartitions(FileSystem fs, Path p, 
				JobConf job) throws IOException {
			FileStatus[] fileStatus = fs.listStatus(p);
      FSDataInputStream fileIn = fs.open(p);      
			RecordReader<LongWritable, Text> reader = 
				new LineRecordReader(fileIn, 0, fileStatus[0].getLen(), job);
			LongWritable key = reader.createKey();
			Text value = reader.createValue();
			ArrayList<Text> parts = new ArrayList<Text>();
			while (reader.next(key, value)) {
				String rowkey = value.toString().split(RANGE_FILE_DELIM, 2)[0];
				parts.add(new Text(rowkey));
				key = reader.createKey();
			}
			reader.close();
			return parts.toArray(new Text[parts.size()]);
		}
	}
	
	private void initPartitioner(JobConf conf) throws IOException, URISyntaxException {
		// Set paramters
		PRIMARY_DELIM = getEscapeStr(conf, "mrsstable.primary.delimeter",
				"\001");
		RowKeyDesc rowKeyDesc = new RowKeyDesc();
		rowKeyDesc.buildRowKeyDesc(conf);

		// Sample original files
		Path input = FileInputFormat.getInputPaths(conf)[0];
		input = input.makeQualified(input.getFileSystem(conf));
		Path partitionFile = new Path(input, PARTITIOIN_FILE_NAME);
		if (conf.get("mrsstable.partition.file", "") != "") {
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddhhmmssSSS");
			partitionFile = new Path(
						conf.get("mrsstable.partition.file", "") + dateFormatter.format(new Date()));
			;
			System.out.printf("Partition Filename is %s\n", partitionFile.toString());
		}
		conf.setPartitionerClass(TotalOrderPartitioner.class);
		
		int reduceNum = conf.getNumReduceTasks();
		TextInputSampler.Sampler sampler = null;
		boolean writePartsFile = false;
		String samplerType = conf.get("mrsstable.presort.sampler", "specify");
		if (samplerType.equals("split")) {
			System.out.println("Using split sampler...");
			String defaultNumSampleStr = Integer.toString(reduceNum * 10);
			String numSampleStr = conf.get("mrsstable.sample.number", defaultNumSampleStr);
			int numSample = Integer.parseInt(numSampleStr);			
			String defaultMaxSplitStr = Integer.toString(reduceNum / 5);
			String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
			int maxSampleSplit = Integer.parseInt(maxSplitStr);
			sampler = new TextInputSampler.SplitSampler(numSample,
					maxSampleSplit, rowKeyDesc.getOrgColumnIndex(), PRIMARY_DELIM);
			writePartsFile = true;
			
		} else if (samplerType.equals("interval")) {
			System.out.println("Using interval sampler...");
			String defaultChoosePercentpStr = Double.toString(0.001);
			String choosePercentStr = conf.get("mrsstable.sample.choose.percent", defaultChoosePercentpStr);
			double choosePercent = Double.parseDouble(choosePercentStr);
			int maxSplits = (int) Math.max(reduceNum * 0.05, 1);
			String defaultMaxSplitStr = Integer.toString(maxSplits);
			String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
			int maxSampleSplit = Integer.parseInt(maxSplitStr);			
			sampler = new TextInputSampler.IntervalSampler(choosePercent, maxSampleSplit,
					rowKeyDesc.getOrgColumnIndex(), PRIMARY_DELIM);
			writePartsFile = true;
			
		} else if (samplerType.equals("random")) {
			System.out.println("Using random sampler...");
			String defaultChoosePercentpStr = Double.toString(0.001);
			String choosePercentStr = conf.get("mrsstable.sample.choose.percent", defaultChoosePercentpStr);
			double choosePercent = Double.parseDouble(choosePercentStr);			
			String defaultNumSampleStr = Integer.toString(reduceNum * 10);
			String numSampleStr = conf.get("mrsstable.sample.number", defaultNumSampleStr);
			int numSample = Integer.parseInt(numSampleStr);				
			String defaultMaxSplitStr = Integer.toString(reduceNum / 5);
			String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
			int maxSampleSplit = Integer.parseInt(maxSplitStr);
			sampler = new TextInputSampler.RandomSampler(choosePercent, numSample, 
					maxSampleSplit, rowKeyDesc.getOrgColumnIndex(), PRIMARY_DELIM);
			writePartsFile = true;	
			
		} else {
			// user specify range list
			System.out.println("User specify range list...");
			TotalOrderPartitioner.setPartitionFile(conf, new Path(RANGE_FILE_NAME));
		}
		
		String[] archivesStr = conf.get("tmparchives", "").split(",", -1);
		if (archivesStr.length == 0 && archivesStr[0] == "") {
			throw new IllegalArgumentException("not specify config archive file");
		} else {
			URI configArchiveUri = new URI(archivesStr[0] + "#" + CONFIG_DIR);
			DistributedCache.addCacheArchive(configArchiveUri, conf);
		}

		if (writePartsFile) {
			TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
			TextInputSampler.writePartitionFile(conf, sampler);

			// Add to DistributedCache
			URI partitionUri = new URI(partitionFile.toString() + "#" + PARTITIOIN_FILE_NAME);
			DistributedCache.addCacheFile(partitionUri, conf);		
		}		
	}
	
	private void deletePartitionFile(JobConf conf) throws IOException {
		Path partitionFile = new Path(TotalOrderPartitioner.getPartitionFile(conf));
		System.out.printf("Delete Partition File %s\n", partitionFile.toString());
		FileSystem fs = partitionFile.getFileSystem(conf);
		fs.delete(partitionFile, false);
	}
	
	private void initDistributeCache(JobConf conf) throws URISyntaxException {
		Path nativeLibPath;
		String nativeLibStr = conf.get("mrsstable.native.lib.path", "");
		if (nativeLibStr != "") {
			nativeLibPath = new Path(conf.get("mrsstable.native.lib.path", ""));
		} else {
			throw new IllegalArgumentException("not specify native lib path");
		}
		
		// Add libnone to DistributedCache
		Path libNoneFile = new Path(nativeLibPath, LIB_NONE_OB_NAME);
		if (conf.get("mrsstable.libnone.ob.file", "") != "") {
			libNoneFile = new Path(conf.get("mrsstable.libnone.ob.file", ""));
		}		
		URI libNoneUri = new URI(libNoneFile.toString() + "#" + LIB_NONE_OB_NAME);
		DistributedCache.addCacheFile(libNoneUri, conf);	
		
		// Add org liblzo to DistributedCache
		Path liblzoOrgFile = new Path(nativeLibPath, LIB_LZO_ORG_NAME);
		if (conf.get("mrsstable.liblzo.org.file", "") != "") {
			liblzoOrgFile = new Path(conf.get("mrsstable.liblzo.org.file", ""));
		}		
		URI liblzoOrgUri = new URI(liblzoOrgFile.toString() + "#" + LIB_LZO_ORG_NAME);
		DistributedCache.addCacheFile(liblzoOrgUri, conf);			
		
		// Add ob liblzo to DistributedCache
		Path liblzoObFile = new Path(nativeLibPath, LIB_LZO_OB_NAME);
		if (conf.get("mrsstable.liblzo.ob.file", "") != "") {
			liblzoObFile = new Path(conf.get("mrsstable.liblzo.ob.file", ""));
		}		
		URI liblzoObUri = new URI(liblzoObFile.toString() + "#" + LIB_LZO_OB_NAME);
		DistributedCache.addCacheFile(liblzoObUri, conf);	
		
		// Add org libsnappy to DistributedCache
		Path libsnappyOrgFile = new Path(nativeLibPath, LIB_SNAPPY_ORG_NAME);
		if (conf.get("mrsstable.libsnappy.org.file", "") != "") {
			libsnappyOrgFile = new Path(conf.get("mrsstable.libsnappy.org.file", ""));
		}		
		URI libsnappyOrgUri = new URI(libsnappyOrgFile.toString() + "#" + LIB_SNAPPY_ORG_NAME);
		DistributedCache.addCacheFile(libsnappyOrgUri, conf);			
		
		// Add ob libsnappy to DistributedCache
		Path libsnappyObFile = new Path(nativeLibPath, LIB_SNAPPY_OB_NAME);
		if (conf.get("mrsstable.libsnappy.ob.file", "") != "") {
			libsnappyObFile = new Path(conf.get("mrsstable.libsnappy.ob.file", ""));
		}		
		URI libsnappyObUri = new URI(libsnappyObFile.toString() + "#" + LIB_SNAPPY_OB_NAME);
		DistributedCache.addCacheFile(libsnappyObUri, conf);	
		
		// Add libmrsstable to DistributedCache
		Path mrsstableFile = new Path(nativeLibPath, LIB_MRSSTABLE_NAME);
		if (conf.get("mrsstable.jni.so.file", "") != "") {
			mrsstableFile = new Path(conf.get("mrsstable.jni.so.file", ""));
		}		
		URI mrsstableUri = new URI(mrsstableFile.toString() + "#" + LIB_MRSSTABLE_NAME);
		DistributedCache.addCacheFile(mrsstableUri, conf);			
	}	
	
	@Override
	public int run(String[] args) throws Exception {		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input dirs> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
		}
		
		JobConf conf = new JobConf(getConf(), MRGenSstable.class);
		FileInputFormat.setInputPaths(conf, args[0]);
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));		
		String reduceNumStr = conf.get("mapred.reduce.tasks", "700");
		conf.setNumReduceTasks(Integer.parseInt(reduceNumStr));	
		System.out.println("Reduce num:" + conf.getNumReduceTasks());

		String inputFormat = conf.get("mrsstable.input.format", "text");
		if (inputFormat.equals("text")) {
			System.out.println("Use text input format");
			conf.setInputFormat(TextInputFormat.class);
		} else if (inputFormat.equals("sequence")) {
			System.out.println("Use sequence input format");
			conf.setInputFormat(SequenceFileAsTextInputFormat.class);
		} else {
			System.err.printf("Unsupport input format: " + inputFormat);
			return -1;
		}
		
		conf.setOutputFormat(SSTableOutputformat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setMapperClass(TokenizerMapper.class);
		conf.setReducerClass(SSTableReducer.class);
		conf.setOutputKeyComparatorClass(TaggedKeyComparator.class);
		
		if (conf.getJobName().isEmpty()) {
			conf.setJobName("MRGenSstable");
		}
		initDistributeCache(conf);
		initPartitioner(conf);
		DistributedCache.createSymlink(conf);
		RunningJob job = JobClient.runJob(conf);
		deletePartitionFile(conf);

		if (job.getJobState() != JobStatus.SUCCEEDED) {
			return -1;
		} else {
			return 0;
		}
	}

	private static void usage(String info) {
		System.err.println(info);
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MRGenSstable(), args);
		System.exit(res);
	}
}
