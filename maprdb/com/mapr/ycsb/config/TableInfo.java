/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/*import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TableInfo {
  final private static Logger _logger = LoggerFactory.getLogger(TableInfo.class);

  private String table = "usertable";
  private String family = "family";
  private String type = "hbase";
  private String keyprefix = "user";
  private String compression = "NONE";
  private int numRegions = 100;
  private boolean insertOrder = false;
  private String adminClass = " com.yahoo.ycsb.db.HBaseAdminClient ";
      
  public String getTable() {
    return table;
  }

  public String getFamily() {
    return family;
  }

  public String getKeyprefix() {
    return keyprefix;
  }

  @JsonProperty(value = "num_initial_regions")
  public int getNumRegions() {
    return numRegions;
  }
  
  @JsonProperty(value = "insertion_order")
  public boolean getInsertOrder() {
    return insertOrder;
  }

  public String getType() {
    return type;
  }

  public String getCompression() {
    return compression;
  }

  public boolean createTable(com.mapr.ycsb.config.Config config) {
    int err = 0;
    File logFile = new File (config.getWorkingDir(), "./logs/tablecreate_out.log");
    logFile.getParentFile().mkdirs();
    try (OutputStream logStream = new FileOutputStream(logFile);) {
      
      if (getType().trim().toLowerCase().equals("json")){
        adminClass = " com.mapr.ycsb.db.MapRDBAdminClient ";
    }
      
      _logger.info("Running {}", adminClass);
      
/*      CommandLine cmdLine = config.getJavaCmdLine()
          .addArgument(adminClass)
          .addArgument(" -Dtype=" + getType())
          .addArgument(" -Dkey_prefix=" + getKeyprefix())
          .addArgument(" -Dnum_regions=" + getNumRegions())
          .addArgument(" -Dcompression=" + getCompression())
          .addArgument(" -Dfamily=" + config.getTableInfo().getFamily())
          .addArgument(" -Dinsertion_order=" + getInsertOrder())
          .addArgument(" CREATE ")
          .addArgument(config.getTableInfo().getTable());

      DefaultExecutor executor = new DefaultExecutor();
      executor.setStreamHandler(new PumpStreamHandler(logStream));

      try {
        _logger.info("Creating table, logging output to '{}'", logFile);
        _logger.debug("Executing {}.", cmdLine);
        err = executor.execute(cmdLine); // sync execution
      } catch (ExecuteException e) {
        err = e.getExitValue();
        _logger.error("Table creation failed. Error {}. See logfile for more detail.", err);
        _logger.debug(e.getMessage(), e);
      }*/
    } catch (Exception e) {
      err = 1;
      _logger.error(e.getMessage());
      _logger.debug(e.getMessage(), e);
    }
    return err == 0;
  }

}