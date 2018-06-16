import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();

        // 输入路径
        String pathIn = "/input";

		String output = "/output";
		
        // 输出路径
        String pathOut = "/output";

        // 用来交换的
        String pathTmp = "";

        // the ip of master
        FileSystem.setDefaultUri(conf, new URI("hdfs://192.168.142.129:9000"));

        // pretreat the data
        Job job = new Job(conf, "MapReduce pretreatment");
        job.setJarByClass(PageRank.class);
        
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
        
        // Only if the output type of map and reduce class is same, we can do so
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

        // swap the input file and output file
        pathIn = pathOut;
		pathOut = output + 0;

        job.waitForCompletion(true);
		FileSystem.get(job.getConfiguration()).delete(new Path(pathOut), true);

        // 进行 pagerank
        for (int i = 0; i < 100; i++) {
            job = new Job(conf, "MapReduce pagerank");
            job.setJarByClass(PageRank.class);
            
            // firstmapper used to 
            job.setMapperClass(SecondMapper.class);
            job.setReducerClass(SecondReducer.class);
            
            // Only if the output type of map and reduce class is same, we can do so
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(pathIn));
            FileOutputFormat.setOutputPath(job, new Path(pathOut));
    
            // swap the input file and output file
            pathIn = pathOut;
            pathOut = output + (i + 1);
            
            job.waitForCompletion(true);
        }
    }
}