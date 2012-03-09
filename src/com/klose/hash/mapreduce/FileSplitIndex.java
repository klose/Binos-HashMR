package com.klose.hash.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public class FileSplitIndex {
	  private Path file;
	  private long start;
	  private long length;
	  private String[] hosts;
	  public FileSplitIndex() {}

	  /** Constructs a split index with host information
	   *
	   * @param file the file name
	   * @param start the position of the first byte in the file to process
	   * @param length the number of bytes in the file to process
	   * @param hosts the list of hosts containing the block, possibly null
	   */
	  public FileSplitIndex(Path file, long start, long length, String[] hosts) {
	    this.file = file;
	    this.start = start;
	    this.length = length;
	    this.hosts = hosts;
	  }
	  /** The file containing this split's data. */
	  public Path getPath() { return file; }
	  
	  /** The position of the first byte in the file to process. */
	  public long getStart() { return start; }
	  
	  /** The number of bytes in the file to process. */
	  public long getLength() { return length; }

	  @Override
	  public String toString() {
		  StringBuffer buf = new StringBuffer();
		  buf.append("file : " + file + "\n");
		  buf.append("start-offset : " + start + "\n");
		  buf.append("length : " + length + "\n");
		  buf.append("locations : " + "\n");
		  for (String loc: hosts) {
			  buf.append("  " + loc + "\n");
		  }
		  return buf.toString();
	  }
	  public void write(OutputStream out) throws IOException{
			WritableUtils.writeString((DataOutput) out,file.toString());
			WritableUtils.writeVLong((DataOutput) out, start);
			WritableUtils.writeVLong((DataOutput) out, length);
			WritableUtils.writeVInt((DataOutput) out, hosts.length);
		      for (int i = 0; i < hosts.length; i++) {
		        Text.writeString((DataOutput) out, hosts[i]);
		      }
	  }

	public void readFields(InputStream in) throws IOException {
		file = new Path(WritableUtils.readString((DataInput) in));
		start = WritableUtils.readVLong((DataInput) in);
		length = WritableUtils.readVLong((DataInput) in);
		hosts = new String[WritableUtils.readVInt((DataInput) in)];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = new String(Text.readString((DataInput) in));
		}
	}
}
