//OBJECTIVES
//Find the number of movies having rating more than 3.9
//1,The Nightmare Before Christmas,1993,3.9,4568
//MAPPING


package ratingMorethan;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingMoreThan {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

					try {
						String[] str = value.toString().split(",");
						
							context.write(new Text(str[1]), new Text("," +str[3]));
					
					}

					catch (Exception e) {
						System.out.println(e.getMessage());
					}
				}
			}
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			
			for (Text t : values) {
				String parts[] = t.toString().split(",");
			if(!parts[0].contains(""))
			{
				float rating=Float.parseFloat(parts[0]); 
			
				if(rating>3.9)
			  {
				  context.write(key, new Text(parts[0])); 
			  }
			}
			
			
		}
	}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find the number of movies having rating more than 3.9");
		job.setJarByClass(RatingMoreThan.class);
		job.setMapperClass(MapClass.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


