package summaries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, NumPair, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, final Iterable<NumPair> values, final Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        Integer count = 0;

        for (NumPair value: values){
            sum += value.getHoursPerWeek().get();
            count += value.getCountInSet().get();
        }

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
    }
}
