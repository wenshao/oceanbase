package com.taobao.mrsstable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.taobao.mrsstable.MRGenSstable.TotalOrderPartitioner;

public class SSTableOutputformat extends FileOutputFormat<Text, Text> 
	implements Configurable {
	
	private JobConf jobConf = null;
	private String baseName;
	private String tableId;
	
  protected class SSTableWriter implements RecordWriter<Text, Text> {
	
  	private static final String LIB_SSTABLE_NAME = "mrsstable";
  	private static final String LIB_NONE_OB_NAME = "none";	
  	private static final String LIB_LZO_ORG_NAME = "lzo2";
  	private static final String LIB_LZO_OB_NAME = "lzo_1.0";
  	private static final String LIB_SNAPPY_ORG_NAME = "snappy";
  	private static final String LIB_SNAPPY_OB_NAME = "snappy_1.0";  	
  	private static final String SCHEMA_FILE_NAME = "config/schema.ini";
  	private static final String DATA_SYNTAX_FILE_NAME = "config/data_syntax.ini";
  	private static final int DIRECT_BUF_SIZE = 2 * 1024 * 1024;
  	
  	private final Log LOG = LogFactory.getLog(SSTableWriter.class);
  	
	  protected DataOutputStream out;
		private TotalOrderPartitioner partitioner;
	  private SSTableBuilder sstableBuilder;
	  private ByteBuffer byteBuffer;
	  private int prev_range_no = -1;		  
	  private boolean is_first = false;
	  private boolean is_inited = false;
	  
    private static final String utf8 = "UTF-8";
    private byte[] newline;
		
	  public SSTableWriter() {
	  }
	  
	  private void init() {
	  	if (!is_inited) {
	  		try 
	  		{ 
	  			System.loadLibrary(LIB_LZO_ORG_NAME);
	  			System.loadLibrary(LIB_LZO_OB_NAME);
	  			System.loadLibrary(LIB_SNAPPY_ORG_NAME);
	  			System.loadLibrary(LIB_SNAPPY_OB_NAME);
	  			System.loadLibrary(LIB_NONE_OB_NAME);
	  			System.loadLibrary(LIB_SSTABLE_NAME);
	  		} 
	  		catch(UnsatisfiedLinkError e) 
	  		{   			
	  			throw new UnsatisfiedLinkError("Cannot load sstable library:\n " + 
	            e.toString());
	  		} 
	  		
	      try {
	        newline = "\n".getBytes(utf8);
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }	  		
	  		
	  		partitioner = new TotalOrderPartitioner();
	  		partitioner.configure(jobConf);
	  		sstableBuilder = new SSTableBuilder();
	  		int err = sstableBuilder.init(SCHEMA_FILE_NAME, DATA_SYNTAX_FILE_NAME);
	  		if (err != 0) {
	  			LOG.error("init sstable builder failed");
	  			throw new IllegalArgumentException("can't init sstable builder");
	  		}
	  		byteBuffer = ByteBuffer.allocateDirect(DIRECT_BUF_SIZE);
	  		byteBuffer.clear();
	  		prev_range_no = -1;		
	  		is_inited = true;	  		
	  	}
	  }
	  
	  private String generateFileName(String name, int range_no) {
	  	StringBuilder strBuilder = new StringBuilder(256);
	  	strBuilder.append(name).append("-").append(tableId)
	  		.append("-").append(new DecimalFormat("000000").format(range_no));
	  	return strBuilder.toString();
	  }
	  
	  private void crateNewSSTableFile(Text start_key) 
	  	throws IOException {
	  	int range_no = partitioner.getRange(start_key);
	  	String name = generateFileName(baseName, range_no);
			Path file = FileOutputFormat.getTaskOutputPath(jobConf, name);
			FileSystem fs = file.getFileSystem(jobConf);
			out = fs.create(file);
			is_first = true;
	  }
	  
	  private void writeCurrentByteBuffer(boolean is_last) throws IOException {
	  	byteBuffer.flip();
	  	ByteBuffer tmpByteBuf = byteBuffer.slice();
	  	ByteBuffer output = sstableBuilder.append(tmpByteBuf, is_first, is_last);
			if (output.capacity() > 0) {
				byte[] reslutBuf = new byte[output.capacity()];
				output.get(reslutBuf);
				out.write(reslutBuf);
			}	else {
				LOG.error("after sstable builder append, return" 
						+ output.capacity() + "bytes");
				throw new IOException("failed to append data to sstable");
			}
			byteBuffer.clear();
			is_first = false;
	  }
	
	  public synchronized void write(Text key, Text value)
	    throws IOException {
	  	
	  	if (!is_inited) {
	  		init();
	  	}
	  	
	  	int cur_range_no = partitioner.getRange(key);	  	
	  	if (cur_range_no < 0 ) {
	  		LOG.error("invalid range number" + cur_range_no);
	  		return;
	  	} else if (value.getLength() > DIRECT_BUF_SIZE) {
	  		LOG.error("value is too large to append" + value.getLength());
	  		return;
	  	}		  	
	  	
	  	if (prev_range_no == -1) {
	  		crateNewSSTableFile(key);
	  	}

	  	if (prev_range_no != -1 && cur_range_no >= 0
	  			&& cur_range_no != prev_range_no) {
  			writeCurrentByteBuffer(true);
  			out.close();
	  		crateNewSSTableFile(key);		
	  	} else if (byteBuffer.remaining() < value.getLength() + newline.length) {
	  		writeCurrentByteBuffer(false);
	  	}
	  	
  		byteBuffer.put(value.getBytes(), 0, value.getLength());
  		byteBuffer.put(newline);
  		prev_range_no = cur_range_no;
	  }
	
	  public synchronized void close(Reporter reporter) throws IOException {
	  	if (is_inited) {
		  	writeCurrentByteBuffer(true);
		    out.close();
		    sstableBuilder.close();
	  	}
	  }
	}	

	@Override
	public void setConf(Configuration conf) {
		jobConf = (JobConf)conf;
		tableId = jobConf.get("mrsstable.table.id", "");
		if (tableId == "") {
			throw new IllegalArgumentException("not specify table id");
		}
	}

	@Override
	public Configuration getConf() {
		return jobConf;
	}

	@Override
  public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
		baseName = new String(name);
		return new SSTableWriter();
  }
}