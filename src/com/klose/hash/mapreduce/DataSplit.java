package com.klose.hash.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;



import com.transformer.compiler.JobConfiguration;

/**
 * split the large HDFS file into small HDFS file. 
 * NOTICE: only support to split a large file into some smaller FileSplit.
 * @author jiangbing
 */
public class DataSplit {
	public static final Log LOG = LogFactory.getLog(DataSplit.class.getName());
	private static final double SPLIT_SLOP = 1.0;   // 10% slop
	public static Configuration conf;
	public static FileSystem fs;
	
	public Path path;
	static {
		//check the hdfs runs well.
		conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public DataSplit(Path path) throws IOException {
		if (!fs.exists(path)) {
			LOG.error(path + " doesnot exist.", new FileNotFoundException(path.toString()));
		}
		if (!fs.isFile(path)) {
			LOG.warn("DataSplit can only split a larger file into smaller ones."
						+ path + " is not a regular file.");
		}
		this.path = path;
	}
	public List<FileSplitIndex> getSplits(MRConfig conf) throws IOException {
		long splitSize = conf.getSplitFileSize();
		FileStatus status = fs.getFileStatus(this.path);
		long length = status.getLen();
		List<FileSplitIndex> splits = new ArrayList<FileSplitIndex> ();
		if (length != 0) {
			BlockLocation[] blkLocations = fs.getFileBlockLocations (status, 0, length);
			long fileBlockSize = status.getBlockSize();
			if (splitSize != fileBlockSize) {
				LOG.warn(status.getPath() + " split size:" + fileBlockSize
						+ " and the MR config split size : " + splitSize  + ", conflicts.");
			}
			long bytesRemaining = length;
			while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
				int blkIndex = getBlockIndex(blkLocations, length
						- bytesRemaining);
				splits.add(makeSplitIndex(path, length - bytesRemaining, splitSize,
						blkLocations[blkIndex].getHosts()));
				bytesRemaining -= splitSize;
			}

			if (bytesRemaining != 0) {
				splits.add(makeSplitIndex(path, length - bytesRemaining,
						bytesRemaining,
						blkLocations[blkLocations.length - 1].getHosts()));
			}
		}
		else {
			splits.add(makeSplitIndex(path, 0, length, new String[0]));
		}
		return splits;
	}

	protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
		for (int i = 0; i < blkLocations.length; i++) {
			// is the offset inside this block?
			if ((blkLocations[i].getOffset() <= offset)
					&& (offset < blkLocations[i].getOffset()
							+ blkLocations[i].getLength())) {
				return i;
			}
		}
		BlockLocation last = blkLocations[blkLocations.length - 1];
		long fileLength = last.getOffset() + last.getLength() - 1;
		throw new IllegalArgumentException("Offset " + offset
				+ " is outside of file (0.." + fileLength + ")");
	}
	/**
	   * A factory that makes the split for this class. It can be overridden
	   * by sub-classes to make sub-types
	   */
	  protected FileSplitIndex makeSplitIndex(Path file, long start, long length, 
	                                String[] hosts) {
	    return new FileSplitIndex(file, start, length, hosts);
	  }

}
