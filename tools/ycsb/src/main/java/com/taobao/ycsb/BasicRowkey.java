package com.taobao.ycsb;

import com.taobao.oceanbase.vo.Rowkey;

public class BasicRowkey extends Rowkey {

	private byte[] bytes;

	public BasicRowkey(byte[] bytes) {
		this.bytes = bytes;
	}

	@Override
	public byte[] getBytes() {

		return this.bytes;
	}

}
