package com.csfrez.flink.cdc.thread;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yangzhi
 * @date 2021/11/20
 * @email csfrez@163.com
 */
public class StringThreadLocal {

    private static ThreadLocal<String> threadLocal = new ThreadLocal<>();

    /**
     * 设置值
     * @param string
     */
    public static void set(String string){
        threadLocal.set(string);
    }


    /**
     * 获取值
     * @return
     */
    public static String get(){
        return threadLocal.get();
    }

    /**
     * 清空值
     */
    public static void remove(){
        threadLocal.remove();
    }
}
