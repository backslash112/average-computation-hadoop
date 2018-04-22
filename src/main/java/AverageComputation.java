import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputation {

    public static class Map extends Mapper<LongWritable, Text, Text, IntPair> {

        private java.util.Map<String, Integer> sizeSum = null;
        private java.util.Map<String, Integer> recordCount = null;

        @Override
        protected void setup(Context context) {
            this.sizeSum = new HashMap<String, Integer>();
            this.recordCount = new HashMap<String, Integer>();
        }

        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] result = line.split(" ");
            String address = result[0];
            String sizeStr = result[result.length - 1];
            int size = sizeStr.equals("-") ? 0 : Integer.parseInt(sizeStr);
            Integer originSum = this.sizeSum.containsKey(address) ? this.sizeSum.get(address) : 0;
            Integer originCount = this.recordCount.containsKey(address) ? this.recordCount.get(address) : 0;
            this.sizeSum.put(address, originSum + size);
            this.recordCount.put(address, originCount + 1);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<String, Integer> entry: this.sizeSum.entrySet()) {
                String address = entry.getKey();
                Integer sum = entry.getValue();
                Integer count = this.recordCount.get(address);
                context.write(new Text(entry.getKey()), new IntPair(sum, count));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntPair, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntPair> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int size = 0;
            for (IntPair pair : values) {
                sum += pair.getFirst();
                size += pair.getSecond();
            }
            context.write(key, new IntWritable(sum / size));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "average-computation-hadoop");
        job.setJarByClass(AverageComputation.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}