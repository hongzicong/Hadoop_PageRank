import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<Object, Text, IntWritable, Text> {
    
    private IntWritable id;
    private String pr;
    private int count;
    private float average_pr;
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer str = new StringTokenizer(value.toString());
        // if not empty get the page if
        if (str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        } else {
            return;
        }

        // get the page of pr
        pr = str.nextToken();
        
        // get the count of output
        count = str.countTokens();

        // each page average pr
        average_pr = Float.parseFloat(pr) / count;
        
        // get tht output page id
        while (str.hasMoreTokens()) {
            String nextId = str.nextToken();
            
            // nextId @ average_pr
            Text tmpText = new Text("@" + average_pr);
            context.write(new IntWritable(Integer.parseInt(nextId)), tmpText);

            // id # nextId
            tmpText = new Text("#" + nextId);
            context.write(id, tmpText);
        }
    }
}