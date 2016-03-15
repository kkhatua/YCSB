/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;

import com.mapr.db.MapRDB;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public class YCSBDocumentBuilder extends DocumentBuilder {

  public YCSBDocumentBuilder(Config config) throws DBException {
    super(config);
  }

  @Override
  public Document newDocument0(HashMap<String, ByteIterator> values) {
    Document document = (Document) MapRDB.newDocument();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      document.set(entry.getKey(), entry.getValue().toArray());
    }
    return document;
  }

  @Override
  public DocumentMutation newMutation(HashMap<String, ByteIterator> values) {
    DocumentMutation mutation = MapRDB.newMutation();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      mutation.setOrReplace(entry.getKey(), ByteBuffer.wrap(entry.getValue().toArray()));
    }
    return mutation;
  }

  @Override
  public HashMap<String, ByteIterator> buildRowResult(Document document) {
    return buildRowResult(document, null);
  }

  @Override
  public HashMap<String, ByteIterator> buildRowResult(Document document,
      HashMap<String, ByteIterator> result) {
    if (document != null) {
      if (result == null) {
        result = new HashMap<String, ByteIterator>();
      }
      for (Entry<String, Value> kv : document) {
        result.put(kv.getKey(), new ValueByteIterator(kv.getValue()));
      }
    }
    return result;
  }

}
