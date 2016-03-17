/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.ycsb.config;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/*import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
/*import com.mapr.ycsb.Driver;
import com.mapr.ycsb.Launcher;
import com.mapr.ycsb.workload.Benchmark;
import com.mapr.ycsb.workload.Workload;*/

public class Config {
  final private static Logger _logger = LoggerFactory.getLogger(Config.class);

  private static final File CURRENT_DIR = new File(System.getProperty("user.dir"));

  private File jarFile;

  private static ObjectMapper mapper;
  static {
    mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
    mapper.configure(Feature.ALLOW_COMMENTS, true);
    mapper.registerSubtypes(Workload.getSubTypes());
  }

  /*
  private JavaInfo javaInfo;

  private Driver.Config driverConfig;

  private Launcher.Config launcherConfig;

  private Benchmark benchmark;
  */
  
  private String javaCmdLine = null;

  private File configFile;

  private TableInfo tableInfo;
  /*
  @JsonProperty(value = "java")
  public JavaInfo getJavaInfo() {
    return javaInfo;
  }
  */
  /*
  @JsonProperty(value = "tableinfo")
  public TableInfo getTableInfo() {
    return tableInfo;
  }
  */
  /*
  @JsonProperty(value = "driver")
  public Driver.Config getDriverConfig() {
    return driverConfig;
  }
  */
  /*
  @JsonProperty(value = "launcher")
  public Launcher.Config getLauncherConfig() {
    return launcherConfig;
  }
  */
  /*
  @JsonProperty(value = "benchmark")
  public Benchmark getBenchmark() {
    return benchmark.setConfig(this);
  }
  */
  @JsonIgnore
  public File getJarFile() {
    return jarFile;
  }

  @JsonIgnore
  public File getWorkingDir() {
    return CURRENT_DIR;
  }

  /*
  @JsonIgnore
  public synchronized CommandLine getJavaCmdLine() {
    if (javaCmdLine == null) {
      String hadoopClasspath = "";
      if (javaInfo.includeHadoopClasspath()) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        CommandLine cmdLine = CommandLine.parse("hadoop classpath");
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(baos, System.err));
        try {
          _logger.info("Calculating Hadoop classpath.");
          _logger.debug("Executing {}.", cmdLine);
          executor.execute(cmdLine); // sync execution
          hadoopClasspath = new String(baos.toByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
          _logger.debug(e.getMessage(), e);
          throw new IllegalStateException("Unable to launch 'hadoop classpath'. See logfile for more detail.");
        }
      }
      javaCmdLine = new StringBuilder().append(javaInfo.getBin()).append(" ")
          .append(" -cp ").append(javaInfo.getClasspath()) // user specified classpath first
          .append(File.pathSeparatorChar).append(getJarFile().getAbsolutePath()) // followed by this jar
          .append(File.pathSeparatorChar).append(hadoopClasspath) // and hadoop classpath at last
        .toString();
    }

    CommandLine cmdLine = CommandLine.parse(javaCmdLine);
    cmdLine.addArguments(javaInfo.getArgs(), false);
    return cmdLine;
  }
  */
  @JsonIgnore
  public File getConfigFile() {
    return configFile;
  }

  private Config init(File configFile) throws IOException {
    this.configFile = configFile;
    String jarPath = Config.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (!jarPath.endsWith(".jar")) {
      // IDE?
      jarFile = new File(System.getProperty("user.dir"), "target/ycsb-driver-5.2.0-mapr-SNAPSHOT.jar");
    } else {
      jarFile = new File(jarPath);
    }

    if (!jarFile.exists()) {
      throw new IllegalStateException("This command must be run from its JAR file. Unable to find " + jarFile);
    }
    return this;
  }

  public static Config load(File configFile)
      throws JsonParseException, JsonMappingException, IOException {
    return mapper.readValue(configFile, Config.class).init(configFile);
  }

}