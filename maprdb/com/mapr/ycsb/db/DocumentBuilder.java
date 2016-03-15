/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.ojai.Document;
import org.ojai.store.DocumentMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public abstract class DocumentBuilder {
  final private static Logger _logger = LoggerFactory.getLogger(DocumentBuilder.class);

  protected Config config;

  private static volatile List<Document> recordList;
  private static volatile int maxDocuments = 0;
  private static volatile int numDocuments = 0;

  Random rand = new Random(System.currentTimeMillis());

  public DocumentBuilder(Config config) throws DBException {
    this.config = config;
    if (config.fixedset) {
      synchronized (DocumentBuilder.class) {
        if (maxDocuments == 0) {
          maxDocuments = config.fixedset_count;
          recordList = Lists.newArrayListWithCapacity(maxDocuments);
        }
      }
    }
  }

  public Document newDocument(HashMap<String, ByteIterator> values) {
    Document record = null;
    if (config.fixedset) {
      if (numDocuments < maxDocuments) {
        synchronized (recordList) {
          if (numDocuments < maxDocuments) {
            record = newDocument0(values);
            recordList.add(record);
            numDocuments++;
          }
        }
        if (numDocuments == maxDocuments) {
          _logger.info("Finished generating {} records.", numDocuments);
        }
      }
      return (record == null) ? recordList.get(rand.nextInt(numDocuments)) : record;
    } else {
      return newDocument0(values);
    }
  }

  public abstract Document newDocument0(HashMap<String, ByteIterator> values);

  public abstract DocumentMutation newMutation(HashMap<String, ByteIterator> values);

  public abstract HashMap<String, ByteIterator> buildRowResult(Document record);

  public abstract HashMap<String, ByteIterator> buildRowResult(Document record, HashMap<String, ByteIterator> result);

  public static class Config {
    public String type = "default";

    public boolean fixedset = false;

    public int fixedset_count = 10000;

    private Map<String, ?> schema = null;
    public Map<String, ?> getSchema() {
      return schema;
    }
  }

}
