package com.klose.hash.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

import org.apache.hadoop.util.LineReader;


/**
 * FileLineReader: read the file split index, and control the function of reading
 * file between start to (start + length), and provide a line for used. 
 * @author jiangbing
 *
 */
public class HdfsFileLineReader {
	private static final Log LOG = LogFactory.getLog(HdfsFileLineReader.class);
	public static final String MAX_LINE_LENGTH = 
	    "mapreduce.input.linerecordreader.line.maxlength";
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;
	private static  FileSystem fs;
	private static  Configuration conf;
	static {
		try {
			conf = new Configuration();
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void initialize(FileSplitIndex genericSplit)
			throws IOException {
		
		FileSplitIndex split = (FileSplitIndex) genericSplit;
		
		this.maxLineLength = conf.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		// open the file and seek to the start of the split
		final FileSystem fs = file.getFileSystem(conf);
		fileIn = fs.open(file);
		fileIn.seek(start);
		in = new LineReader(fileIn, conf);
		filePosition = fileIn;
		// If this is not the first split, we always throw away first record
		// because we always (except the last split) read one extra line in
		// next() method.
		if (start != 0) {
			start += in.readLine(new Text(), 0, maxBytesToConsume(start));
		}
		this.pos = start;
	}
	
	 private int maxBytesToConsume(long pos) {
		    return (int) Math.min(Integer.MAX_VALUE, end - pos);
	 }

	private long getFilePosition() throws IOException {
		long retVal;
		if (null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		while (getFilePosition() <= end) {
			
			newSize = in.readLine(value, maxLineLength,
					Math.max(maxBytesToConsume(pos), maxLineLength));
			//System.out.println("end:" + end + "pos:" +pos);
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	public LongWritable getCurrentKey() {
		return key;
	}

	public Text getCurrentValue() {
		return value;
	}

}
