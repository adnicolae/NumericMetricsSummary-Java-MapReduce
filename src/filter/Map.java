package filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Input: LongWritable, Text <=> line number, a single line
 * Result: value output of the map phase, records that match the criteria: NullWritable, Text <=> NULL, a single line
 */
public class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(" ");

        if (data[2].equalsIgnoreCase("Laptops")) {
            context.write(NullWritable.get(), value);
        }
    }
}
