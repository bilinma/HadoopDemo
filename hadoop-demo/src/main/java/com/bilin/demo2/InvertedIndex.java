package com.bilin.demo2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class MyType implements WritableComparable<MyType> {
		public MyType() {
		}

		private String word;

		public String Getword() {
			return word;
		}

		public void Setword(String value) {
			word = value;
		}

		private String filePath;

		public String GetfilePath() {
			return filePath;
		}

		public void SetfilePath(String value) {
			filePath = value;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(word);
			out.writeUTF(filePath);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			word = in.readUTF();
			filePath = in.readUTF();
		}

		@Override
		public int compareTo(MyType arg0) {
			if (word != arg0.word)
				return word.compareTo(arg0.word);
			return filePath.compareTo(arg0.filePath);
		}
	}

	public static class InvertedIndexMapper extends Mapper<Object, Text, MyType, Text> {

		public void map(Object key, Text value, Context context) throws InterruptedException, IOException {
			FileSplit split = (FileSplit) context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
			System.out.println("+++++++++++++"+split.getPath().toUri().getPath());

			while (itr.hasMoreTokens()) {
				MyType keyInfo = new MyType();
				keyInfo.Setword(itr.nextToken());
				keyInfo.SetfilePath(split.getPath().toUri().getPath().replace("/user/in/", ""));
				context.write(keyInfo, new Text("1"));
			}
		}
	}

	public static class InvertedIndexCombiner extends Reducer<MyType, Text, MyType, Text> {

		public void reduce(MyType key, Iterable<Text> values, Context context)
				throws InterruptedException, IOException {
			int sum = 0;
			for (Text value : values) {
				sum += Integer.parseInt(value.toString());
			}
			context.write(key, new Text(key.GetfilePath() + ":" + sum));
		}
	}

	public static class InvertedIndexReducer extends Reducer<MyType, Text, Text, Text> {

		public void reduce(MyType key, Iterable<Text> values, Context context)
				throws InterruptedException, IOException {
			Text result = new Text();

			String fileList = new String();
			for (Text value : values) {
				fileList += value.toString() + ";";
			}
			result.set(fileList);

			context.write(new Text(key.Getword()), result);
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		System.out.println("url:" + conf.get("fs.default.name"));

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(MyType.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path path = new Path("out");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(path))
			hdfs.delete(path, true);

		FileInputFormat.addInputPath(job, new Path("in"));
		FileOutputFormat.setOutputPath(job, new Path("out"));
		job.waitForCompletion(true);
	}

}