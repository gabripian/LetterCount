package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class LetterCount {

    /**
     * LetterCountMapper is the Mapper class for counting occurrences of each letter.
     * It uses a stateful In-Mapper Combining to aggregate counts within the Mapper itself,
     * reducing the amount of intermediate data sent to the Reducer.
     */
    public static class LetterCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<Text, IntWritable> letterCounts;  // A map to store the count of each letter.
        // letterCounts is the state of the mapper, so it is private per each mapper.
        // This state is preserved until the cleanup() method.

        /**
         * The setup method is called once at the beginning of the map task.
         * It initializes the letterCounts map.
         */
        @Override
        protected void setup(Context context) /*throws IOException, InterruptedException*/ {
            letterCounts = new HashMap<>();  // Initialize the map
        }

        /**
         * The map method processes each line of input. It converts the line to lowercase,
         * iterates through each character, checks if it is a letter, and updates the count in the map.
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
                    // Then I have to update the map object.
                    if (letterCounts.containsKey(letter)) {
                        // If the letter has been seen before, we substitute the existing entry with the incremented one.
                        letterCounts.put(letter, new IntWritable(letterCounts.get(letter).get() + 1));
                        // As you can see, we did a local aggregation of the results.
                    } else {
                        // Otherwise, it's the first time that I see this letter, then we insert a new entry.
                        letterCounts.put(letter, new IntWritable(1));
                    }
                }
            }
        }

        /*
        The outputs of the In-Mapper combining are like:
        < a , 2 >
        < b , 4 >
        < c , 7 >
        ...
        < z , 1 >
         */

        /**
         * The cleanup method is called once at the end of the map task.
         * It emits the aggregated letter counts stored in the map.
         *
         * @param context The context object for interacting with the Hadoop framework.
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit each letter and its count.
            for (Map.Entry<Text, IntWritable> entry : letterCounts.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
                // Local results are emitted after the cleanup() method.
            }
        }
    }

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

        // < a , [2 , 4 , 6 , 8 , 2 ] >
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
            System.err.println("Usage: LetterCount <input path> <output path> <num of reducers>");
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
        Job job = Job.getInstance(conf, "LetterCount");

        // Step 5: Specify the jar file that contains the mapper, reducer, and job configurations
        job.setJarByClass(LetterCount.class);

        // Step 6: Set mapper and reducer classes for the job
        job.setMapperClass(LetterCountMapper.class); // Set the mapper class.
        job.setReducerClass(LetterCountReducer.class); // Set the reducer class.

        // Step 7: Define mapper's output key-value classes
        //job.setMapOutputKeyClass(Text.class); // Set the mapper output key class.
        //job.setMapOutputValueClass(IntWritable.class); // Set the mapper output value class.

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

/*
CONTEXT

The context object provides access to the job's configuration data.
This allows the mapper and reducer to read configuration settings, such as file paths,
custom parameters, and other configuration properties.
In the map phase, the context object is used to emit intermediate key-value pairs.
In the reduce phase, it is used to emit final key-value pairs.
Methods: write(KEYOUT key, VALUEOUT value)

The context object allows tasks to report progress back to the Hadoop framework.
This is essential for long-running tasks to prevent the job tracker from assuming that the task is stuck.
Methods: progress()

Tasks can send status updates back to the framework using the context object. This helps in tracking the progress of the job.
Methods: setStatus(String status)

The context object can be used to log messages. This is useful for debugging and monitoring the task's execution.
Methods: getCounter(Enum<?> counterName) and incrementCounter(Enum<?> counterName, long amount)
* */
