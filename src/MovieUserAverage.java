import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MovieUserAverage {
    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input : userId, movieId, rating
            //outputKey: 1
            //outputValue : movieId
            String line = value.toString().trim();
            String outputValue = line.split(",")[1]; // movieId
            context.write(new IntWritable(1), new Text(outputValue));
        }
    }
    public static class UserAverageMapper extends Mapper<LongWritable, Text,IntWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input : user \t average score
            //output key : 1
            //output value : userId-averageRating
            String line = value.toString().trim();
            String user = line.split("\t")[0];  // userId
            String averageRating = line.split("\t")[1]; // average rating
            context.write(new IntWritable(1), new Text(user + "-" + averageRating));
        }
    }
    public static class MovieUserAverageReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> movies = new HashSet<String>();
            List<String> user_avarages = new ArrayList<String>();
            while (values.iterator().hasNext()) {
                String line = values.iterator().next().toString();
                if (line.contains("-")) {
                    user_avarages.add(line);
                } else {
                    movies.add(line);
                }
            }
            for (String movie : movies) {
                for (String user_average : user_avarages) {
                    context.write(new Text(movie), new Text(user_average)); // movieId \t userId-averageRating
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MovieUserAverage.class);
        job.setReducerClass(MovieUserAverageReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserAverageMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
