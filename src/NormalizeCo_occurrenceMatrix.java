import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NormalizeCo_occurrenceMatrix {
    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] movie_relation = value.toString().trim().split(",");
            String movieA = movie_relation[0].split(":")[0];
            String movieB = movie_relation[0].split(":")[1];
            context.write(new Text(movieA), new Text(movieB + "=" + movie_relation[1]));
        }
    }
    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key = movieA
            //value = <movieB = relation1, movieC = relation2...
            //sum(relation) = denominator
            //outputKey = movieB
            //outputValue = movieA = relation1/denominator
            int denominator = 0;
            Map<String, Integer> movie_relation_map = new HashMap<String, Integer>();
            while (values.iterator().hasNext()) {
                String movie_relation = values.iterator().next().toString();
                String movie = movie_relation.split("=")[0];
                int value = Integer.parseInt(movie_relation.split("=")[1]);
                denominator += value;
                movie_relation_map.put(movie, value);
            }

            for (Map.Entry<String, Integer> relation : movie_relation_map.entrySet()) {
                String movie = relation.getKey();
                int value = relation.getValue();
                context.write(new Text(movie), new Text(key + "=" + (double)value / denominator));
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(NormalizeCo_occurrenceMatrix.class);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
