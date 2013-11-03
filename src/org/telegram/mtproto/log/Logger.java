package org.telegram.mtproto.log;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 3:54
 */
public class Logger {

    public static void w(String tag, String message) {
        System.out.println(tag + ":" + message);
    }

    public static void d(String tag, String message) {
        System.out.println(tag + ":" + message);
    }

    public static void t(String tag, Throwable t) {
        t.printStackTrace();
    }
}
