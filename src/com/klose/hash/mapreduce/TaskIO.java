package com.klose.hash.mapreduce;

import java.util.ArrayList;
import java.util.List;

public class TaskIO {
    public TaskIO(String jobId){
    	
    }
    public boolean putkey(int partId,String key,List<Object> value) {
    	return false;
    }
    public List<Object> getkey(int partId,String key) {
    	return new ArrayList<Object>();
    }
    public boolean update(int partId) {
    	return true;
    }
    public ArrayList<String> getkeyList(int partId){
    	return null;
    }
   
};