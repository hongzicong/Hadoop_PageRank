import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayList<String> idlist = new ArrayList<String>();
        
        //整理数据
        for(Text id : values) {
            String idStr = id.toString();
            //把所有链接的节点存放于idlist中
            if (idStr.substring(0,1).equals("&")) {
                idlist.add(idStr.substring(1));
            }
        }

        int pr = 1;

        String result = String.valueOf(pr) + " ";
        
        for(int i = 0; i< idlist.size();i++){
            result += idlist.get(i) + " ";
        }

        context.write(key, new Text(result));
    }

}