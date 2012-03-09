package com.klose.hash.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



/**
 * configure the arguments of map reduce.
 * @author jiangbing
 *
 */
public class  MRConfig {
	/*there are default values*/
	private  String jobName = "testJob";
	private  int mapTaskMem = 100*1024*1024;
	private  int mapTaskNum = 1;
	private  int reduceTaskMem = 100*1024*1024;
	private  int reduceTaskNum = 1;
	private  static int fetchThreadNum = 3; 
	
	private  long splitFileSize = 64*1024*1024; //use a hdfs block size as default value.
	private  Class mapContextKeyClass = String.class;
	private  Class mapContextValueClass = Integer.class;
	private   Class<? extends Mapper> mapClass;
	private   Class<? extends Reducer> reduceClass;
	private   Path workingDirectory;
	private String tempMapOutFilesPathPrefix;
	private  String[] inputFileName;
	private  String[] outputFileName;
	private  static Configuration conf;
	private  static FileSystem fs;
	static {
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
//			splitFileSize = fs.getDefaultBlockSize();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	public MRConfig(String jobName) {
		setJobName(jobName);
	}
	public  String getJobName() {
		return jobName;
	}
	public  void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public  int getMapTaskMem() {
		return mapTaskMem;
	}
	public  void setMapTaskMem(int mapTaskMem) {
		this.mapTaskMem = mapTaskMem;
	}
	
	public  int getMapTaskNum(Path path) {
		try {
			FileStatus status = fs.getFileStatus(path);
			long fileLen = status.getLen();
			if (fileLen % splitFileSize != 0) {
				mapTaskNum = (int)(fileLen/splitFileSize + 1);
			}
			else {
				mapTaskNum = (int)(fileLen/splitFileSize);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapTaskNum;
	}
	
	public  int getReduceTaskMem() {
		return reduceTaskMem;
	}
	public  void setReduceTaskMem(int reduceTaskMem) {
		this.reduceTaskMem = reduceTaskMem;
	}
	public  int getReduceTaskNum() {
		return reduceTaskNum;
	}
	public  void setReduceTaskNum(int reduceTaskNum) {
		this.reduceTaskNum = reduceTaskNum;
	}
	public static int getFetchThreadNum() {
		return fetchThreadNum;
	}
	public static void setFetchThreadNum(int threadNum) {
		fetchThreadNum = threadNum;
	}

	public  long getSplitFileSize() {
		return splitFileSize;
	}
	public  void setSplitFileSize(long size) {
		this.splitFileSize = size;
	}
	public  long getDefaultHDFSBlockSize() {
		return fs.getDefaultBlockSize();
	}
	public   Class getMapContextKeyClass() {
		return mapContextKeyClass;
	}
	public  void setMapContextKeyClass(Class mapContextKeyClass) {
		this.mapContextKeyClass = mapContextKeyClass;
	}
	public  Class getMapContextValueClass() {
		return mapContextValueClass;
	}
	public  void setMapContextValueClass(Class mapContextValueClass) {
		this.mapContextValueClass = mapContextValueClass;
		
	}
	public  Class<? extends Mapper> getMapClass() {
		return mapClass;
	}
	public  void setMapClass(Class<? extends Mapper> mapClass) {
		this.mapClass =  mapClass;
	}
	public  String getTempMapOutFilesPathPrefix() {
		return tempMapOutFilesPathPrefix;
	}
	public  void setTempMapOutFilesPathPrefix(String tempMapOutFilesPathPrefix) {
		tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
	}
	public  Class<? extends Reducer> getReduceClass() {
		return reduceClass;
	}
	public  void setReduceClass(Class<? extends Reducer> reduceClass) {
		this.reduceClass = reduceClass;
	}
	public  Path getWorkingDirectory() {
		return workingDirectory;
	}
	public  void setWorkingDirectory(Path workingDirectory) {
		this.workingDirectory = workingDirectory;
	}
	public  String[] getInputFileName() {
		return inputFileName;
	}
	public  void setInputFileName(String[] inputFile) {
		inputFileName = inputFile;
	}
	public  String[] getOutputFileName() {
		return outputFileName;
	}
	public  void setOutputFileName(String[] outputFile) {
		outputFileName = outputFile;
	}
	
	/**
	 * Parse the run-time scheduler required information.
	 * @return
	 */
	public Map<String, String> parseMRConfig() {
		Map<String, String> configMap = new HashMap<String, String>();
		configMap.put("mapper.class", getMapClass().getName());
		configMap.put("reducer.class", getReduceClass().getName());
		configMap.put("map.task.num", String.valueOf(this.mapTaskNum));
		configMap.put("reduce.task.num", String.valueOf(this.getReduceTaskNum()));	
		configMap.put("mapper.file.spilt.size", String.valueOf(this.getSplitFileSize()));
		return configMap;
	}
}
