package com.taobao.mrsstable;

import java.nio.ByteBuffer;

public class SSTableBuilder { 
	public native int init(String schema, String syntax);
	
	public native ByteBuffer append(ByteBuffer input, boolean is_first, 
			 boolean is_last);
	
	public native void close();
}