import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ibrahim on 12/7/16.
 */
public class WordReducer extends MapReduceBase implements Reducer <Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter r) throws IOException {
        int count = 0;

        while (values.hasNext()){
            IntWritable i = values.next();
            count += i.get();
        }

        output.collect(key, new IntWritable(count));
    }
}
