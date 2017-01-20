package com.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBInputWritable implements Writable, DBWritable {
	   private int empId;
	   private String name;
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setInt(1, empId);
		statement.setString(2, name);
		
	}
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		 empId = resultSet.getInt(1);
	     name = resultSet.getString(2);
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	public int getEmpId() {
		return empId;
	}
	public void setEmpId(int empId) {
		this.empId = empId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	
	   
	
}
