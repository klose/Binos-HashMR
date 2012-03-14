package com.klose.hash.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import com.klose.hash.mapredue.reduce.ReduceContext;


public abstract class Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * The <code>Context</code> passed on to the {@link Reducer}
	 * implementations.
	 */
	public abstract class Context {
	}

	/**
	 * Called once at the start of the task.
	 */
	protected void setup(ReduceContext context) throws IOException,
			InterruptedException {
		context.init();
	}

	public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, ReduceContext context); //////////////
	
	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	@SuppressWarnings("unchecked")
	public void run(ReduceContext context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKey()) {
			reduce((KEYIN)context.getCurrentKey(), context.getValues(), context);
		}
		context.flush();
		cleanup(context);
	}
	
	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(ReduceContext context) throws IOException,
			InterruptedException {
		System.out.println(" reducer getKey used total " + context.timeUsed + "ms");
		// NOTHING
	}
}