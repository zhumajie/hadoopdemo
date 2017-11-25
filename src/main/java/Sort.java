import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Sort extends MapReduce {

    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{

        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
            if (StringUtils.isEmpty(value.toString())) {
                return;
            }
            data.set(Integer.parseInt(value.toString()));
            context.write(data, new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

        private static IntWritable nm = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            context.write(nm, key);
            nm.set(nm.get() + 1);
        }
    }

    public static void main(String[] args) throws Exception{
/*        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "sort");
        job.setJarByClass(Sort.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);*/

        start("sort", Sort.class, Map.class, Reduce.class, IntWritable.class, IntWritable.class, args);
    }
}
