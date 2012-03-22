package com.klose.hash.mapreduce.operation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import com.klose.hash.mapreduce.MRConfigDefine;
import com.klose.hash.mapreduce.Mapper;
import com.klose.hash.mapreduce.map.MapContext;
import com.transformer.compiler.JobProperties;
import com.transformer.compiler.Operation;

public class MapOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(MapOperation.class);
	@Override
	public void operate(JobProperties properties, String[] inputPath, String[] outputPath) {
		// TODO Auto-generated method stub
		int reducerNum = Integer.parseInt(properties.getProperty(MRConfigDefine.REDUCER_NUM));
		LOG.debug("mapper needs partition to " + reducerNum + "  partitions.");
		String taskId = properties.getProperty("taskID");
		String jobId = taskId.substring(0, taskId.lastIndexOf("-"));
		MapContext context;
		try {
			context = new MapContext(inputPath[0], jobId, reducerNum, properties.getProperty(MRConfigDefine.COMBINER_CLASS));
			Class<? extends Mapper>  mapClass = (Class<? extends Mapper>) Class.forName(properties.getProperty("mapper.class"));
			Constructor<Mapper> meth = (Constructor<Mapper>) mapClass.getConstructor(new Class[0]);
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}

