package summaries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// line number, one line within input file, key = text = marital status, value = doublewritable = no. of hours worked per week
public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");

        try {
            String maritalStatus = data[5];
            Double hrs = Double.parseDouble(data[12]);

            context.write(new Text(maritalStatus), new DoubleWritable(hrs));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
