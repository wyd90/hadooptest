package com.wyd.mr.secordarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {
    public KeyComparator() {
        super(ComboKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey o1 = (ComboKey) a;
        ComboKey o2 = (ComboKey) b;
        if(o1.getYear() - o2.getYear() == 0){
            return o1.getTemp() - o2.getTemp();
        } else {
            return o1.getYear() - o2.getYear();
        }
    }
}
