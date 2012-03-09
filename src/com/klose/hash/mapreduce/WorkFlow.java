package com.klose.hash.mapreduce;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;


import com.klose.hash.mapreduce.operation.MapOperation;
import com.klose.hash.mapreduce.operation.ReduceOperation;
import com.klose.hash.mapreduce.operation.SplitOperation;
import com.transformer.compiler.Channel;
import com.transformer.compiler.ChannelManager;
import com.transformer.compiler.JobCompiler;
import com.transformer.compiler.JobStruct;
import com.transformer.compiler.Operation;
import com.transformer.compiler.ParallelLevel;
import com.transformer.compiler.PhaseStruct;
import com.transformer.compiler.TaskStruct;
import com.transformer.compiler.TransmitType;
import com.transformer.compiler.Tunnel;

public class WorkFlow {
	private static final Log LOG = LogFactory.getLog(WorkFlow.class);
	JobStruct job = new JobStruct();
	private static final String PhaseStruct = null;
	private ArrayList<Class <? extends Operation>>   works = new ArrayList();
	private Class <? extends Mapper> mapCls = null;
	private Class <? extends Reducer> reduceCls = null;
	private MRConfig config;
	private int mapNumber = 4; 
	private int reduceNumber = 2;
	private int parallelNumber = 1;   // the split parallel number
	String[] inputPath;
	String[] outputPath;

	
	public WorkFlow(MRConfig conf){
		works.add(0,(Class<? extends Operation>) SplitOperation.class);
		works.add(1,(Class<? extends Operation>) MapOperation.class);
		works.add(2,(Class<? extends Operation>) ReduceOperation.class);
		config = conf;
		setMapClass(config.getMapClass());
		setReduceClass(config.getReduceClass());
		inputPath = config.getInputFileName();
		outputPath = config.getOutputFileName();
	}
	
	public void setMapClass(Class<? extends Mapper> cls) {		
		mapCls = cls;
		config.setMapClass(cls);
	}
	public void setReduceClass(Class<? extends Reducer> cls) {		
		reduceCls = cls;
		config.setReduceClass(cls);
	}
	
	public void constructWorkFlow(){
		if (mapCls == null || reduceCls == null) {
			LOG.error("please set mapCls and reduceCls.");
		}
		constructWork();
		constructFlow(job.getPhaseStruct());
	}
	
	public void constructWork(){
		//String pathPrefix = JobConfiguration.getPathHDFSPrefix();		
		ParallelLevel pal = new ParallelLevel(ParallelLevel.assignFirstLevel());
		PhaseStruct phase;
		TaskStruct task;

		int i;
		for(i = 0 ; i < works.size() ; i ++ ){
			if(i < works.size() - 1){
				phase  = new PhaseStruct(pal);
			}else{
				phase  = new PhaseStruct(pal.assignEndLevel());
			}	
			
			job.addPhaseStruct(phase);
			task = new TaskStruct();
			task.setOperationClass(works.get(i));
			if(i == 0 ){
				phase.addTask(task, parallelNumber, inputPath);
			}else{
				if( i < works.size() - 1){
					mapNumber = config.getMapTaskNum(new Path(inputPath[0]));
					phase.addTask(task, mapNumber);
				}else{
					reduceNumber = config.getReduceTaskNum();
					phase.addTask(task, reduceNumber,outputPath);
				}
			}		
			pal = pal.nextLevel();
		}
	}
	

//	/**here need to declare one thing : the construct work method is used for all flow conditions ,there 
//	 * can not be phase and phase  in the same level ,but there can only be unlimited number of phase.
//	 * But in the constructFlow function ,this is only used for mapreduce condition*/
	/**
	 * i = 0 means split phase
	 * i = 1 means map phase
	 * i = 2 means reduce phase
	 * and there are only 3 phase in the mapReduce model
	 * */
	public void constructFlow(ArrayList<PhaseStruct> phases){
		
		if(phases.size() != 3){
			System.out.println("in the mapreduce model ,there should be three phases , please check it");
			System.exit(2);
		}
		
		PhaseStruct phase0 = phases.get(0); //split
		PhaseStruct phase1 = phases.get(1); //map
		PhaseStruct phase2 = phases.get(2); //reduce
	
		
		ChannelManager channelManager = new ChannelManager();
		//split to map
		Channel[] channel0_1 = new Channel[phase1.getParallelNum()];
		for(int i = 0; i < phase1.getParallelNum(); i++)
		{	
			channel0_1[i] = new Channel(phase0.getTaskStruct()[0],i,phase1.getTaskStruct()[i],0);			
		}
		Tunnel tunnel0_1 = new Tunnel(phase0, phase1, channel0_1, TransmitType.HDFS);
		
		channelManager.addTunnel(tunnel0_1);
		
		//map to reduce
		
		ArrayList<Channel> tmp = new ArrayList<Channel>();
		for(int i = 0 ;i<phase2.getParallelNum();i++)
		{	
			Channel[] channel1_2 = new Channel[phase1.getParallelNum()];
			for(int j = 0 ;j<phase1.getParallelNum();j++){
				channel1_2[j] = new Channel(phase1.getTaskStruct()[j],i,phase2.getTaskStruct()[i],j);
				tmp.add(channel1_2[j]);
			}
		}
		//Tunnel tunnel1_2 = new Tunnel(phase1, phase2, tmp.toArray(new Channel[0]), TransmitType.HTTP);
		Tunnel tunnel1_2 = new Tunnel(phase1, phase2, tmp.toArray(new Channel[0]), TransmitType.DIST_MEMORY);
		channelManager.addTunnel(tunnel1_2);
		analisisCompile(channelManager);
	}
	
	public void analisisCompile(ChannelManager channelManager){
		Map<String, TaskStruct> map = channelManager.parseDep();
		JobCompiler compiler = new JobCompiler(map, job, config.parseMRConfig());
		compiler.compile();
	}
	
	public int getParallelNumber() {
		return parallelNumber;
	}

	public void setParallelNumber(int parallelNumber) {
		this.parallelNumber = parallelNumber;
	}
}
