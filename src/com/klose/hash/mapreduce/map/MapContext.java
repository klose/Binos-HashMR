package com.klose.hash.mapreduce.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;

import com.klose.hash.mapreduce.FileSplitIndex;
import com.klose.hash.mapreduce.HdfsFileLineReader;
import com.klose.hash.mapreduce.TaskIO;
import com.longyi.databus.clientapi.TaskOutput;
import com.longyi.databus.define.DATABUS;
import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
/**
 * prepare data and context for Mapper
 * @author jiangbing
 *
 * @param <KEY>
 * @param <VALUE>
 */
public class MapContext<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(MapContext.class);
	
	public static long timeUsed = 0;
	private final String inputPath;
	private final String jobId;
	private final int reducerNum;
	private FileSplitIndex splitIndex = new FileSplitIndex();
	private HdfsFileLineReader lineReader = new HdfsFileLineReader();
	private final TaskOutput taskOutput ;
	private final static HashPartitioner<String, Object> hashPar  = new HashPartitioner<String, Object>(); 
	public MapContext(String inputPath, String jobId, int reducerNum) {
		this.inputPath = inputPath;
		this.jobId = jobId;
		this.reducerNum = reducerNum;
		InputStream ins;
		try {
			ins = BinosDataClient.getInputStream(new BinosURL(new Text(inputPath)));
			splitIndex.readFields(ins);
			lineReader.initialize(splitIndex);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		taskOutput = new TaskOutput(jobId, DATABUS.JOB_VALUE_BYTE);
		
	}
	public boolean hasNextLine() throws IOException {
		return lineReader.nextKeyValue();
	}

	public String getNextLine() {
		return lineReader.getCurrentValue().toString();
	}
//	public void output(String key, Object value) {
//		long start = System.currentTimeMillis(); 
//		ByteArrayOutputStream baos;
//		ObjectOutputStream oos;
//		try {
//			baos = new ByteArrayOutputStream();
//			oos = new ObjectOutputStream(baos);
//			oos.writeObject(value);
//			this.taskOutput.putkeyByte(String.valueOf(hashPar.getPartition(key, reducerNum)), key, Arrays.asList(baos.toByteArray()));
//			baos.close();
//			oos.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		timeUsed += System.currentTimeMillis() - start; 
//	}
	
	public void output(String key, Object value) {
		long start = System.currentTimeMillis(); 	
		//System.out.println(key + " " + value.toString());
		this.taskOutput.putkeyByte(String.valueOf(hashPar.getPartition(key, reducerNum)), key, Arrays.asList(value.toString().getBytes()));
		timeUsed += System.currentTimeMillis() - start; 
	}
}
