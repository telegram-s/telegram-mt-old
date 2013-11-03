package org.telegram.mtproto.time;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 02.11.13
 * Time: 21:35
 */
public class TimeOverlord {
    private static TimeOverlord instance;

    public static synchronized TimeOverlord getInstance() {
        if (instance == null) {
            instance = new TimeOverlord();
        }
        return instance;
    }

    public long createWeakMessageId() {
        return (getLocalTime() / 1000) << 32;
    }

    public long getLocalTime() {
        return System.currentTimeMillis();
    }

    public void onMethodExecuted(long sentId, long responseId) {

    }
}
