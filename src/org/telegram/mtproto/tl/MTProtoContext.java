package org.telegram.mtproto.tl;

import org.telegram.tl.TLContext;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:22
 */
public class MTProtoContext extends TLContext {
    @Override
    protected void init() {
        registerClass(MTPing.CLASS_ID, MTPing.class);
        registerClass(MTPong.CLASS_ID, MTPong.class);
        registerClass(MTMsgsAck.CLASS_ID, MTMsgsAck.class);
        registerClass(MTNewSessionCreated.CLASS_ID, MTNewSessionCreated.class);
        registerClass(MTBadMessageNotification.CLASS_ID, MTBadMessageNotification.class);
        registerClass(MTBadServerSalt.CLASS_ID, MTBadServerSalt.class);
        registerClass(MTMessagesContainer.CLASS_ID, MTMessagesContainer.class);
        registerClass(MTRpcError.CLASS_ID, MTRpcError.class);
        registerClass(MTRpcResult.CLASS_ID, MTRpcResult.class);
    }
}
