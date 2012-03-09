package com.klose.hash.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;



/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
public class MRJobContext  {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  protected static final String MAP_CLASS_ATTR = "mapreduce.map.class";
  protected static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";

  protected final MRConfig conf;
  private final String jobId;
  
  public MRJobContext(MRConfig mrConfig,String jobId) {
    this.conf = mrConfig;
    this.jobId = jobId;
  }

  public MRConfig  getConfiguration() {
    return conf;
  }

  public String getJobID() {
    return jobId;
  }
  
  public int getNumReduceTasks() {
    return conf.getReduceTaskNum();
  }

  public Path getWorkingDirectory() throws IOException {
    return conf.getWorkingDirectory();
  }


  public String getJobName() {
    return conf.getJobName();
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Mapper<?,?>> getMapperClass() 
     throws ClassNotFoundException {
    return (Class<? extends Mapper<?,?>>) conf.getMapClass();
  }


  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>)  conf.getReduceClass();
  }


  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getReduceClass();
  }

}


