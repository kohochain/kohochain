package io.fortest.core.net.messagehandler;

import io.fortest.core.exception.P2pException;
import io.fortest.core.net.message.khcMessage;
import io.fortest.core.net.peer.PeerConnection;

public interface khcMsgHandler {

  void processMessage(PeerConnection peer, khcMessage msg) throws P2pException;

}
