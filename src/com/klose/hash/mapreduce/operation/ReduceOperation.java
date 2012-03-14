package com.klose.hash.mapreduce.operation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;


import com.klose.hash.mapreduce.Reducer;
import com.klose.hash.mapredue.reduce.ReduceContext;
import com.transformer.compiler.JobProperties;
import com.transformer.compiler.Operation;

public class ReduceOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(ReduceOperation.class);
	@Override
	public void operate(JobProperties properties, String[] inputPath, String[] outputPath) {
		String taskId = properties.getProperty("taskID");
		
		String jobId = properties.getProperty("jobID");
		LOG.info("JobID:" + jobId);
		LOG.info("taskID:" + taskId);
		int partitionId = Integer.parseInt(taskId.substring(taskId.lastIndexOf('_') + 1));
		
		ReduceContext context;
		try {
			context = new ReduceContext(jobId, partitionId,properties.getProperty("tmpDir") + "/" + outputPath[0]);
			Class<? extends Reducer>  reduceClass = (Class<? extends Reducer>) Class.forName(properties.getProperty("reducer.class"));
			//Class<? extends Reducer>  reduceClass = MRConfig.getReduceClass();
			Constructor<Reducer> meth = (Constructor<Reducer>) reduceClass.getConstructor(new Class[0]);
			meth.setAccessible(true);
			meth.newInstance().run(context);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
