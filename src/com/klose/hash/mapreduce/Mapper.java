package com.klose.hash.mapreduce;
import java.io.IOException;

import com.klose.hash.mapreduce.map.MapContext;

/**
 * Mapper : the base workflow of mapper operation.
 * @author jiangbing
 *
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class Mapper<KEY, VALUE> {
	public Mapper() {}
	  /**
	   * Called once at the beginning of the task.
	   */
	  protected void setup(MapContext context
	                       ) throws IOException, InterruptedException {
	    // NOTHING
	  }
	public abstract void map(String line, MapContext<KEY,VALUE> context);
	
	
	  /**
	   * Expert users can override this method for more complete control over the
	   * execution of the Mapper.
	   * @param context
	   * @throws IOException
	   */
	  public void run(MapContext<KEY,VALUE> context) throws IOException, InterruptedException {
	    setup(context);
	    
	    while (context.hasNextLine()) {
	      map(context.getNextLine(), context);
	    }
	    cleanup(context);
	  }
	  
	  /**
	   * Called once at the end of the task.
	   */
	  protected void cleanup(MapContext<KEY,VALUE> context
	                         ) throws IOException, InterruptedException {
	    // NOTHING
		  System.out.println("output used total " + context.timeUsed + "ms");
	  }
	  
	
}
