package com.taobao.ycsb;

import com.yahoo.ycsb.ByteIterator;

public class EncodeIntegerIterator extends ByteIterator {

	private int i;

	public EncodeIntegerIterator(int i) {
		this.i = i;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public byte nextByte() {
		return 0;
	}

	@Override
	public long bytesLeft() {
		return 0;
	}

	@Override
	public String toString() {
		return "" + i;

	}

}
