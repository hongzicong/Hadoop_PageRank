import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        IntWritable id;
        IntWritable nextId;

        // 用来将每一行的数据拆分开
        StringTokenizer str = new StringTokenizer(value.toString());

        // 如果不为空，则读取该页面的页号
        if(str.hasMoreTokens()) {
            id = new IntWritable(Integer.parseInt(str.nextToken()));
        }else{
            return;
        }

        // 该页面所指向的页面
        nextId = new IntWriteable(Integer.parseInt(str.nextToken()));

        context.write(id, new Text("&" + nextId));

    }

}