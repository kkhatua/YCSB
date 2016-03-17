/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.db;

import org.ojai.Value;
import org.ojai.util.Values;

import com.yahoo.ycsb.ByteIterator;

public class ValueByteIterator extends ByteIterator {

  private Value value;

  public ValueByteIterator(Value value) {
    this.value = value;
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
    return Values.asJsonString(value);
  }
  
}