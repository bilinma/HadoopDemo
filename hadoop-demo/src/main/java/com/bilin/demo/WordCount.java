package com.bilin.demo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	/*
	 * 通过扩展Mapper实现内部类TokenizerMapper
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/*
		 * 重载map方法(non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);// 写入处理的中间结果<key,value>
			}
		}
	}

	/*
	 * 通过扩展Reducer实现内部类IntSumReducer
	 */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		/*
		 * 重载reduce方法(non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
		 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get(); // 计数
			}
			result.set(sum);
			context.write(key, result); // 写回结果
			
			System.out.println(key+" "+result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); // 启用默认配置
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");// 定义一个job
		job.setJarByClass(WordCount.class);// 设定执行类
		job.setMapperClass(TokenizerMapper.class);// 设定Mapper实现类
		job.setCombinerClass(IntSumReducer.class);// 设定Combiner实现类
		job.setReducerClass(IntSumReducer.class);// 设定Reducer实现类
		job.setOutputKeyClass(Text.class);// 设定OutputKey实现类,Text.class是默认实现
		job.setOutputValueClass(IntWritable.class);// 设定OutputValue实现类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));// 设定job输入文件夹
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 设定job输出文件夹
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
