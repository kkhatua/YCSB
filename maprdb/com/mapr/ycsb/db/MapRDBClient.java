/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.db;

import static org.ojai.DocumentConstants.ID_FIELD;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.Table.TableOption;
import com.mapr.ycsb.db.DocumentBuilder.Config;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * MapR-DB client for YCSB framework
 */
public class MapRDBClient extends com.yahoo.ycsb.DB {
  final private static Logger _logger = LoggerFactory.getLogger(MapRDBClient.class);

  public static final int Ok=0;
  public static final int ServerError=-1;
  public static final int HttpError=-2;
  public static final int NoMatchingRecord=-3;

  private volatile Table table_ = null;

  private DocumentBuilder recordBuilder;

  private static volatile boolean logOnce = false;

  @Override
  public void init() throws DBException {
    Config config = getConfig(getProperty("dbconfigfile"));
    switch (config.type) {
    case "withschema":
      recordBuilder = new SchemaDocumentBuilder(config);
      break;
    case "default":
    default:
      recordBuilder = new YCSBDocumentBuilder(config);
    }

    synchronized (this) {
      if (!logOnce) {
        logOnce = true;
        _logger.info("Using record builder {}.", recordBuilder.getClass().getName());
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    if (table_ != null) {
      try {
        table_.close();
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      Table table = getTable(tableName);
      Document record = table.findById(key, getFieldPaths(fields));
      recordBuilder.buildRowResult(record, result);
      return (record == null) ? Status.NOT_FOUND : Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      Table table = getTable(tableName);
      QueryCondition condition = MapRDB.newCondition()
          .is(ID_FIELD, Op.GREATER_OR_EQUAL, startkey)
          .build();
      try (DocumentStream stream = table.find(condition, getFieldPaths(fields));) {
        int numResults = 0;
        for (Document record : stream) {
          result.add(recordBuilder.buildRowResult(record));
          numResults++;
          if (numResults >= recordcount) {
            break;
          }
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key,
      HashMap<String, ByteIterator> values) {
    try {
      Table table = getTable(tableName);
      DocumentMutation mutation = recordBuilder.newMutation(values);
      table.update(key, mutation);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key,
      HashMap<String, ByteIterator> values) {
    try {
      Table table = getTable(tableName);
      Document record = recordBuilder.newDocument(values);
      table.insertOrReplace(key, record);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      Table table = getTable(tableName);
      table.delete(key);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  private Table getTable(String tableName) throws DBException, IOException {
    if (table_ == null) {
      synchronized (this) {
        if (table_ == null) {
          table_ = MapRDB.getTable(tableName);
          table_.setOption(TableOption.EXCLUDEID, true);
          table_.setOption(TableOption.BUFFERWRITE, getPropertyBool("maprdb.bufferWrite", true));
        }
      }
    }
    return table_;
  }

  private String[] getFieldPaths(Set<String> fields) {
    if (fields != null) {
      return fields.toArray(new String[fields.size()]);
    }
    return null;
  }

  private static volatile Config config_;
  private static synchronized Config getConfig(String file) throws DBException {
    if (config_ == null) {
      try {
        if (file != null) {
          config_ = com.mapr.ycsb.config.Config.load(new File(file)).getBenchmark().getDocumentBuilderConfig();
        } else {
          config_ = new Config();
        }
      } catch (IOException e) {
        throw new DBException("Unable to load the specified config file: " + file, e);
      }
    }
    return config_;
  }

}