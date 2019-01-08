package com.bilin.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyDFSTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "hdfs://localhost:9000");

			FileSystem hdfs = FileSystem.get(conf);
			Path path = new Path("in/test3.txt");
			FSDataOutputStream outputStream = hdfs.create(path);

			byte[] buffer = " 你好Hello".getBytes();
			outputStream.write(buffer, 0, buffer.length);
			outputStream.flush();
			outputStream.close();

			System.out.println("Create OK");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
