package io.fortest.core.net.message;

import io.fortest.common.overlay.message.Message;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.protos.Protocol;

public class TransactionMessage extends khcMessage {

  private TransactionCapsule transactionCapsule;

  public TransactionMessage(byte[] data) throws Exception {
    super(data);
    this.transactionCapsule = new TransactionCapsule(getCodedInputStream(data));
    this.type = MessageTypes.kht.asByte();
    if (Message.isFilter()) {
      compareBytes(data, transactionCapsule.getInstance().toByteArray());
      transactionCapsule
          .validContractProto(transactionCapsule.getInstance().getRawData().getContract(0));
    }
  }

  public TransactionMessage(Protocol.Transaction kht) {
    this.transactionCapsule = new TransactionCapsule(kht);
    this.type = MessageTypes.kht.asByte();
    this.data = kht.toByteArray();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString())
        .append("messageId: ").append(super.getMessageId()).toString();
  }

  @Override
  public Sha256Hash getMessageId() {
    return this.transactionCapsule.getTransactionId();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  public TransactionCapsule getTransactionCapsule() {
    return this.transactionCapsule;
  }
}
