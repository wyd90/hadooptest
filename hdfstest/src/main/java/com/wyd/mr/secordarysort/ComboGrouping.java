package com.wyd.mr.secordarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComboGrouping extends WritableComparator {

    public ComboGrouping() {
        super(ComboKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey o1 = (ComboKey)a;
        ComboKey o2 = (ComboKey)b;
        return o1.getYear().compareTo(o2.getYear());
    }
}
