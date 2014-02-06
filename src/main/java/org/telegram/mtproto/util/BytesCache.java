package org.telegram.mtproto.util;

import org.telegram.mtproto.log.Logger;

import java.util.HashSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by ex3ndr on 04.02.14.
 */
public class BytesCache {
    private static BytesCache instance = new BytesCache("GlobalByteCache");

    public static BytesCache getInstance() {
        return instance;
    }

    private HashSet<byte[]> byteBuffer = new HashSet<byte[]>();

    private final String TAG;

    public BytesCache(String logTag) {
        TAG = logTag;
    }

    public synchronized void put(byte[] data) {
        byteBuffer.add(data);
        // Logger.d(TAG, "Adding to cache with size " + data.length + ", cache size " + byteBuffer.size());
    }

    public synchronized byte[] allocate(int minSize) {
        byte[] res = null;
        for (byte[] cached : byteBuffer) {
            if (cached.length < minSize) {
                continue;
            }
            if (res == null) {
                res = cached;
            } else if (res.length > cached.length) {
                res = cached;
            }
        }

        if (res != null) {
            byteBuffer.remove(res);
            // Logger.d(TAG, "Return cached " + res.length + " with required >=" + minSize);
            return res;
        }

        // Logger.d(TAG, "Allocating new " + minSize);

        return new byte[minSize];
    }
}
