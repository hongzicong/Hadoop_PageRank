import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public static class SecondReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) {
        
        // 定义一个存储网页链接ID的队列
        ArrayList<String> idList = new ArrayList<String>();
        
        // 将所有的链接ID以String格式保存
        String output = "  ";

        // 定义一个保存网页PR值的变量
        double pr = 0;

        for (Text id : values) {
            String idStr = id.toString();
            // nextId @ average_pr
            if (idStr.substring(0, 1).equals("@")) {
                pr += Float.parseFloat(idStr.substring(1));
            }
            // id # nextId
            else if (idStr.substring(0, 1).equals("#")) {
                String nextId = idStr.substring(1);
                idList.add(nextId);
            }
        }

        // 解决终止点问题和陷阱问题
        pr = pr * 0.85f + 0.15f;

        // 得到所有链接ID的String形式
        for (int i = 0; i < idList.size(); i++) {
            output = output + idList.get(i) + "  ";
        }
        String result = pr + output;
        context.write(key, new Text(result));
    }
}