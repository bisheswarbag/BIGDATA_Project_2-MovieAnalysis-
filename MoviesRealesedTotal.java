
//Count the total number of movies in the list 

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
//import org.apache.hadoop.metrics.spi.NullContext;

public class MoviesRealesedTotal {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				
					context.write(new Text(str[2]), new Text(","+str[1]));
				
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text,NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			int count=0;
			for (@SuppressWarnings("unused") Text t : values) {
				count++;
				@SuppressWarnings("unused")
				String parts[] = values.toString().split(",");
			}
			
			String counting="No. of Movies in "+count	;
				  context.write(null, new Text(counting)); 
			  
			}
			
			
		}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MoviesRealesedTotal.class);
		job.setJobName("Count the total number of movies in the list");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

 