

//Count the total number of movies in the list more than 1.5 hrs



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Moviesmorethanhours {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				
					context.write(new Text(str[1]), new Text(","+str[4]));
				
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	public static class ReduceClass extends Reducer<Text, Text,Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
			//1.5 hours = 110 minutes
			//1 sec=60 mints
			//60 mints= (60*60)sec
		     
			 
			for ( Text t : values)
			{
				String parts[] = t.toString().split(",");
			    float time=Float.parseFloat(parts[1]);
			    float mints=time/60;
			    
			    if(mints>110)
			    {
			    	context.write(key,new Text ("\n"+ " time in sec  "+parts[1] +"   time in mints   "+mints  ));
			    }
				
			}  
			}
		}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Moviesmorethanhours.class);
		job.setJobName("Find the number of movies with duration more than 1.5 hours");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

