import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Dedup extends MapReduce
{

    public static class Map extends Mapper<Object, Text, Text, Text>{

        private static Text one = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            super.map(key, value, context);
/*            one = value;
            context.write(one, new Text(""));*/
            context.write(value, new Text(""));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            context.write(key, new Text(""));
        }

    }

    public static void main(String[] args) throws Exception{
     /*   Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "dedup");
        job.setJarByClass(Dedup.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        start("dedup", Dedup.class, Map.class, Reduce.class, Text.class, Text.class, args);
    }
}
