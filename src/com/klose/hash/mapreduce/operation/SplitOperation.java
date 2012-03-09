package com.klose.hash.mapreduce.operation;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;


import com.klose.hash.mapreduce.DataSplit;
import com.klose.hash.mapreduce.FileSplitIndex;
import com.klose.hash.mapreduce.MRConfig;
import com.transformer.compiler.JobProperties;
import com.transformer.compiler.Operation;

public class SplitOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(SplitOperation.class);
	private static Configuration hdfsConf = new Configuration();
	private static FileSystem fs;
	static {
		try {
			fs = FileSystem.get(hdfsConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("Cannot open HDFS");
		}
	}
	@Override
	public void operate(JobProperties properties, String[] inputPath, String[] outputPath) {
		// TODO Auto-generated method stub
		if (inputPath.length != 1) {
			LOG.error("the number of input path : " + inputPath.length);
			return;
		}
		if (Integer.parseInt(properties.getProperty("map.task.num")) != outputPath.length) {
			LOG.error("the numer of output path:" + outputPath.length + " doesnot equal to map task num!");
			return;
		}
		try {
			MRConfig conf = new MRConfig("split");
			conf.setSplitFileSize(Integer.parseInt(properties.getProperty("mapper.file.spilt.size")));
			final DataSplit split = new DataSplit(new Path(inputPath[0]));
			List<FileSplitIndex> list = split.getSplits(conf);
			if (list.size() != outputPath.length) {
				LOG.error("The number of map task conflicts with the number of output path.");
			}
			
			for(int i = 0; i < outputPath.length; i++) {
				BinosURL url = new BinosURL(new Text(outputPath[i]));
				OutputStream out = BinosDataClient.getOutputStream(url);
				list.get(i).write(out);
				out.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
