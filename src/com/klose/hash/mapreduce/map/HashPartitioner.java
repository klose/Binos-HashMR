package com.klose.hash.mapreduce.map;

public class HashPartitioner<K, V> {
	public  int getPartition(K key, V value, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

	public  int getPartition(K key, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	
}
