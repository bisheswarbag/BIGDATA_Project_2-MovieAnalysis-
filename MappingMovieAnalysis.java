


//OBJECTIVES
//Find the number of movies released between 1945 and 1959
//1,The Nightmare Before Christmas,1993,3.9,4568
//MAPPING


package mappingMovieAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MappingMovieAnalysis {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				long year=Long.parseLong(str[2]);
				if (( year<=1959)&&(year>=1945))
					{
					
					context.write(new Text(str[1]), new Text(str[2]));
				}
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find the number of movies released between 1945 and 1959");
		job.setJarByClass(MappingMovieAnalysis.class);
		job.setMapperClass(MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


