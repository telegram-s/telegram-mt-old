package org.telegram.mtproto.tl;

import org.telegram.tl.TLContext;
import org.telegram.tl.TLObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.telegram.tl.StreamingUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 21:42
 */
public class MTRpcError extends TLObject {

    public static final int CLASS_ID = 0x2144ca19;

    private int errorCode;

    private String message;

    public MTRpcError(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public MTRpcError() {

    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void serializeBody(OutputStream stream) throws IOException {
        writeInt(errorCode, stream);
        writeTLString(message, stream);
    }

    @Override
    public void deserializeBody(InputStream stream, TLContext context) throws IOException {
        errorCode = readInt(stream);
        message = readTLString(stream);
    }
}
