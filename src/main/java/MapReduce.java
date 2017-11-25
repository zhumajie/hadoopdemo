import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Objects;

public class MapReduce {

    public static void start(String jobName, Class jobClass, Class map, Class reduce, Class key, Class value, String[] args) {
        try {
            if (Objects.isNull(args) || args.length < 2) {
                System.exit(2);
            }

            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(jobClass);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(map);
            job.setReducerClass(reduce);

            job.setOutputKeyClass(key);
            job.setOutputValueClass(value);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            FileSystem hdfs = FileSystem.get(configuration);
            if (hdfs.exists(new Path(args[1])))
                hdfs.delete(new Path(args[1]), true);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
