package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LetterCount2 {

    /**
     * LetterCountMapper is the Mapper class for counting occurrences of each letter.
     */
    public static class LetterCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * The map method processes each line of input. It converts the line to lowercase,
         * iterates through each character, checks if it is a letter, and emits the count for each letter.
         *
         * @param key The input key, representing the offset of the line in the file.
         * @param value The input value, representing the line of text.
         * @param context The context object for interacting with the Hadoop framework.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();  // Convert the text line to lowercase.

            for (char c : line.toCharArray()) {  // Iterate through each character inside the text line.
                if (Character.isLetter(c)) {  // Check if the character is a letter; ignore special characters and numbers.
                    Text letter = new Text(String.valueOf(c));  // Create a Text object for the letter
                    context.write(letter, new IntWritable(1));  // Emit the letter and a count of 1
                }
            }
        }
    }

     /*
        The outputs of the mapper are like:
        < a , 1 >
        < b , 1 >
        < c , 1 >
        < b , 1 >
        < c , 1 >
        ...
        < z , 1 >
         */

    /**
     * LetterCountCombiner is the Combiner class for aggregating counts locally on the mapper side.
     */
    public static class LetterCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * The reduce method processes each key and a list of associated values (counts).
         * It sums the counts for each letter and emits the intermediate count.
         *
         * @param key The input key, representing the letter.
         * @param values An iterable over the values associated with the letter.
         * @param context The context object for interacting with the Hadoop framework.
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;  // Initialize the sum of counts
            // Sum the counts for the current letter
            for (IntWritable value : values) {
                sum += value.get();
            }
            // Emit the letter and its intermediate count
            context.write(key, new IntWritable(sum));
        }
    }

     /*
        The outputs of the combiner are like:
        < a , 2 >
        < b , 4 >
        < c , 7 >
        ...
        < z , 1 >
     */

    /**
     * LetterCountReducer is the Reducer class for aggregating the final counts of each letter.
     */
    public static class LetterCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * The reduce method processes each key and a list of associated values (counts).
         * It sums the counts for each letter and emits the final count.
         *
         * @param key The input key, representing the letter.
         * @param values An iterable over the values associated with the letter.
         * @param context The context object for interacting with the Hadoop framework.
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;  // Initialize the sum of counts
            // Sum the counts for the current letter
            for (IntWritable value : values) {
                sum += value.get();
            }
            // Emit the letter and its total count
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // Step 1: Initialize Configuration and parse command-line arguments
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Step 2: Check and validate command-line arguments
        if (otherArgs.length != 3) {
            System.err.println("Usage: LetterCount2 <input path> <output path> <num of reducers>");
            System.exit(1);
        }

        final String inputPath = otherArgs[0];
        final String outputPath = otherArgs[1];
        final String numOfReducers = otherArgs[2];

        // Step 3: Print out the provided command-line arguments
        System.out.println("args[0]: <input path>=" + inputPath);
        System.out.println("args[1]: <output path>=" + outputPath);
        System.out.println("args[2]: <num of reducers>=" + numOfReducers);


        // Step 4: Create a new instance of Job with a given configuration and job name
        Job job = Job.getInstance(conf, "LetterCount2");

        // Step 5: Specify the jar file that contains the mapper, reducer, and job configurations
        job.setJarByClass(LetterCount2.class);

        // Step 6: Set mapper, combiner, and reducer classes for the job
        job.setMapperClass(LetterCountMapper.class); // Set the mapper class.
        job.setCombinerClass(LetterCountCombiner.class); // Set the combiner class.
        job.setReducerClass(LetterCountReducer.class); // Set the reducer class.

        // Step 7: Define mapper's output key-value classes
        job.setMapOutputKeyClass(Text.class); // Set the mapper output key class.
        job.setMapOutputValueClass(IntWritable.class); // Set the mapper output value class.

        // Step 8: Define reducer's output key-value classes
        job.setOutputKeyClass(Text.class); // Set the reducer output key class.
        job.setOutputValueClass(IntWritable.class); // Set the reducer output value class.

        // Step 9: Set the number of reducer tasks (degree of parallelism)
        job.setNumReduceTasks(Integer.parseInt(numOfReducers));

        // Step 10: Define input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(inputPath)); // Set the input path.
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); // Set the output path.

        // Step 11: Set input and output formats
        job.setInputFormatClass(TextInputFormat.class); // Set input format class.
        job.setOutputFormatClass(TextOutputFormat.class); // Set output format class.

        // Step 12: Submit the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1); // Exit upon job completion; 0 means success.
    }
}
