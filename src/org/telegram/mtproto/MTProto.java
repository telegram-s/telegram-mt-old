package org.telegram.mtproto;

import org.telegram.mtproto.pq.PqAuth;
import org.telegram.mtproto.tl.MTProtoContext;
import org.telegram.mtproto.transport.TcpContext;

import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:14
 */
public class MTProto {
    private MTProtoContext protoContext;

    private boolean isSelfReparing = true;

    private HashSet<TcpContext> contexts = new HashSet<TcpContext>();

    private int desiredConnectionCount;

    public MTProto(PqAuth auth) {
        protoContext = new MTProtoContext();
        desiredConnectionCount = 2;
    }
}
