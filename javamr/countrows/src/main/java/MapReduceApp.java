import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceApp {

    public static class MyMapper
            extends Mapper<Object, Text, IntWritable, NullWritable>{

        private Text word = new Text();

        public int counter;
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException,
                InterruptedException {
            counter = 0;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {
                if (value.toString().equals("")){
                    return;
                }
                counter++;
            } catch (Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }

        protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException,
                InterruptedException {
            context.write(new IntWritable(counter), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduceApp.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
