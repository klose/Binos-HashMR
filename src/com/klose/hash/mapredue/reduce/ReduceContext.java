package com.klose.hash.mapredue.reduce;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.ByteString;
import com.klose.hash.mapreduce.TaskIO;
import com.klose.hash.mapreduce.KeyValue.KVList;
import com.longyi.databus.clientapi.TaskInput;
import com.longyi.databus.daemon.PartionUpdateThread;
import com.longyi.databus.define.DATABUS;
import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.TransmitType;

import cn.ict.binos.transmit.MessageClientChannel;

public class ReduceContext <KEYIN, VALUEIN, KEYOUT, VALUEOUT>{


	private final static Log LOG = LogFactory.getLog(ReduceContext.class);
	private final static DataState state = DataState.SHARE_MEMORY;
	private final String jobId;
	private final int partitionId;
	private final String outputPath;
	private final TaskInput taskInput;
	private String currentKey = null;
	private List<Object> currentValue = null;
	private OutputStream out = null;
	private PartionUpdateThread fetchDataThread;
	/*private String key = null;
	private Iterable<Integer> vlist = null;
	String[] reduceRemoteReadPaths;
	String tmpLocalFilePath;
	private static String mergeTmpPath = "reduce-merge-final";
	private String[] outputPath;
	private ReadFromDataBus reader;*/
	public ReduceContext(String jobId, int partitionId, String outputPath){
		this.jobId = jobId;
		this.partitionId = partitionId;
		this.outputPath = outputPath;
		taskInput = new TaskInput(this.jobId, DATABUS.JOB_VALUE_BYTE);
		try {
			this.out = new FileOutputStream(this.outputPath, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void init() {
		fetchDataThread = taskInput.update(String.valueOf(this.partitionId));
	}
	
	public boolean nextKey()  {
		this.currentKey = fetchDataThread.getPrepareKey();
		if (this.currentKey != null) {
			return true;
		}
		else {
			return false;
		}
	}

	public String getCurrentKey() {
		return this.currentKey;
	}
	
	 public Iterable<Object> getValues() {
		 ByteArrayInputStream bais ;
		 ObjectInputStream ois;
		 Iterable <byte[]> iter =  taskInput.getkeyByte(String.valueOf(this.partitionId), this.currentKey);
		 List<Object> values = new ArrayList<Object>();
		 for (byte[] tmp : iter) {
			 try {
				 bais = new ByteArrayInputStream(tmp);
				 ois = new ObjectInputStream(bais);
				 values.add(ois.readObject());
				 ois.close();
				 bais.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 return values;
	 }
	 
	public void output(String key, Object value) {
//		outPut.receive(key, value);
		KVList.Builder builder = KVList.newBuilder();
		builder.setKey(key);
		builder.addVlist((ByteString) value);
		try {
			builder.build().writeDelimitedTo(out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void flush() {
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	

}
