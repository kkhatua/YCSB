/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.ojai.Document;
import org.ojai.FieldPath;

import com.mapr.db.rowcol.KeyValueBuilder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

public class SchemaDocumentBuilder extends YCSBDocumentBuilder {

  private static final FieldPath SALT = FieldPath.parseFrom("salt");

  private static volatile Map<String, ?> schema = null;

  Random rand = new Random(System.currentTimeMillis());

  public SchemaDocumentBuilder(Config config) throws DBException {
    super(config);
    synchronized (SchemaDocumentBuilder.class) {
      if (schema == null) {
        schema = config.getSchema();
      }
    }
  }

  @Override
  public Document newDocument0(HashMap<String, ByteIterator> values) {
    Document r = (Document) KeyValueBuilder.initFrom(schema);
    r.set(SALT, rand.nextInt());
    return r;
  }

}