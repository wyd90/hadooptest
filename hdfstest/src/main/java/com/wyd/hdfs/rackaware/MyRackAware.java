package com.wyd.hdfs.rackaware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyRackAware implements DNSToSwitchMapping {
    public List<String> resolve(List<String> list) {

        List<String> names = new ArrayList<String>();

        try {
            FileWriter fw = new FileWriter("/root/rackaware.txt",true);

            for(String str : list){
                System.out.println(str);
                fw.write(str);
                if("192.168.56.101".equals(str)){
                    names.add("/rack1/node1");
                } else if("192.168.56.102".equals(str)) {
                    names.add("/rack1/node2");
                } else if("192.168.56.103".equals(str)){
                    names.add("/rack2/node3");
                } else if("192.168.56.104".equals(str)){
                    names.add("/rack2/node4");
                } else if("192.168.56.105".equals(str)){
                    names.add("/rack3/node5");
                } else if("node1".equals(str)){
                    names.add("/rack1/node1");
                }else if("node2".equals(str)) {
                    names.add("/rack1/node2");
                } else if("node3".equals(str)){
                    names.add("/rack2/node3");
                } else if("node4".equals(str)){
                    names.add("/rack2/node4");
                } else if("node5".equals(str)){
                    names.add("/rack3/node5");
                }

            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return names;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}
