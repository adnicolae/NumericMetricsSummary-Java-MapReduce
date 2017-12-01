package summaries;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumPair implements WritableComparable<NumPair> {
    private DoubleWritable hoursPerWeek;
    private IntWritable countInSet;

    public NumPair() {
        set(new DoubleWritable(), new IntWritable());
    }

    public NumPair(Double hours, Integer count){
        set(new DoubleWritable(hours), new IntWritable(count));
    }

    public NumPair(DoubleWritable hours, IntWritable count) {
        set(hours, count);
    }

    private void set(DoubleWritable hours, IntWritable count) {
        this.hoursPerWeek = hours;
        this.countInSet = count;
    }

    public DoubleWritable getHoursPerWeek() {
        return hoursPerWeek;
    }

    public IntWritable getCountInSet() {
        return countInSet;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        hoursPerWeek.write(out);
        countInSet.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hoursPerWeek.readFields(in);
        countInSet.readFields(in);
    }

    @Override
    public int compareTo(NumPair numPair) {
        int cmp = hoursPerWeek.compareTo(numPair.hoursPerWeek);

        if (cmp != 0){
            return cmp;
        }

        return countInSet.compareTo(numPair.countInSet);
    }

    @Override
    public int hashCode() {
        return hoursPerWeek.hashCode()*163 + countInSet.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NumPair){
            NumPair numPair = (NumPair) o;
            return hoursPerWeek.equals(numPair.hoursPerWeek) && countInSet.equals(numPair
                    .countInSet);
        }

        return false;
    }
}
