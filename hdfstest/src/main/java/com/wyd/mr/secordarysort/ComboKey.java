package com.wyd.mr.secordarysort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComboKey implements WritableComparable {

    private Integer year;

    private Integer temp;

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getTemp() {
        return temp;
    }

    public void setTemp(Integer temp) {
        this.temp = temp;
    }

    public int compareTo(Object o) {
//        ComboKey that = (ComboKey)o;
//        if(this.year - that.year == 0){
//            return this.temp - that.temp;
//        } else {
//            return this.year - that.year;
//        }
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.temp = dataInput.readInt();
    }
}
