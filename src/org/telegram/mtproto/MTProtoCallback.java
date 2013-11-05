package org.telegram.mtproto;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 04.11.13
 * Time: 22:11
 */
public interface MTProtoCallback {
    public void onSessionCreated(MTProto proto);

    public void onApiMessage(byte[] message);

    public void onRpcResult(int callId, byte[] response);

    public void onRpcError(int callId, int errorCode, String message);
}
