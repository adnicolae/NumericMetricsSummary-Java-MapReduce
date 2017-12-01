package summaries;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ToolRunner;

// creates a job process that is submitted to the hadoop environment
public class Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //
        Job job = Job.getInstance(getConf());
        job.setJobName("average");
        // the jar where the main class is present in
        job.setJarByClass(Main.class);

        // set the data types of the output
        job.setMapOutputValueClass(NumPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);

        Path inputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/NumericMetricsSummary/data/input" +
                        "/census.txt");
        Path outputFilePath = new Path
                ("/home/andrei/hadoop-install/HadoopProjects/NumericMetricsSummary/data/output" +
                        "-with-combiner");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }
}
