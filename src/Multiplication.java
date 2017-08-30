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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Multiplication {
    public static class CooccurenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] movieB_relation = value.toString().trim().split(",");
            String outputKey = movieB_relation[0];
            String outputValue = movieB_relation[1];
            context.write(new Text(outputKey), new Text(outputValue)); //movieB  \t movieA=1/2
        }
    }
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] user_rating = value.toString().trim().split(",");
            String movie = user_rating[1];
            String outputValue = user_rating[0] + ":" + user_rating[2]; // user1:3.9
            context.write(new Text(movie), new Text(outputValue));
        }
    }
    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input : movieId \t userId-averageRating
            //outputKey : movieId
            //outputValue: userId-averageRating
            String line = value.toString().trim();
            String movie = line.split("\t")[0];
            String userId_averageRating = line.split("\t")[1];
            context.write(new Text(movie), new Text(userId_averageRating));
        }
    }
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // inputKey : movieId
            //inputValue : movieA=1/2, userA:3.9, userA-3.5
            Map<String, Double> user_ratings = new HashMap<String, Double>();
            Map<String, Double> movie_relations = new HashMap<String, Double>();
            Map<String, Double> user_averages = new HashMap<String, Double>();
            while (values.iterator().hasNext()) {
                String line = values.iterator().next().toString();
                if (line.contains("=")) {
                    String movie = line.split("=")[0];
                    double relation = Double.parseDouble(line.split("=")[1]);
                    movie_relations.put(movie, relation);
                } else if (line.contains(":")){
                    String user = line.split(":")[0];
                    double rating = Double.parseDouble(line.split(":")[1]);
                    user_ratings.put(user, rating);
                } else {
                    String user = line.split("-")[0];
                    double average_rating = Double.parseDouble(line.split(":")[1]);
                    user_averages.put(user, average_rating);
                }
            }
            for (Map.Entry<String, Double> movie_relation: movie_relations.entrySet()) {
                String movie = movie_relation.getKey();
                double relation = movie_relation.getValue();
                for (Map.Entry<String, Double> user_average : user_averages.entrySet()) {
                    String user = user_average.getKey();
                    double rating = 0;
                    if (user_ratings.containsKey(user)) {
                        rating = user_ratings.get(user);
                    } else {
                        rating = user_averages.get(user);
                    }
                    context.write(new Text(movie + ":" + user), new Text(String.valueOf(relation * rating))); // movie:user \t score representing the user's rating has
                                                                                                    // how much impact on this movie
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);
        job.setReducerClass(MultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AverageMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.waitForCompletion(true);

    }
}
