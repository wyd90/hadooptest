package com.wyd.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class DebugUtil {

    public static String getHostname(){
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获得当前程序进程id
     * @return
     */
    public static int getPID(){
        String info = ManagementFactory.getRuntimeMXBean().getName();
        return Integer.parseInt(info.substring(0,info.indexOf("@")));
    }

    /**
     * 返回当前线程id
     * @return
     */
    public static String getTID(){
        return Thread.currentThread().getName();
    }

    public static String getObjInfo(Object o){
        return o.getClass().getSimpleName();
    }
}
