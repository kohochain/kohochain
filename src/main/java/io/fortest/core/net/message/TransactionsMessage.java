package io.fortest.core.net.message;

import java.util.List;

import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.protos.Protocol;

public class TransactionsMessage extends khcMessage {

  private Protocol.Transactions transactions;

  public TransactionsMessage(List<Protocol.Transaction> khts) {
    Protocol.Transactions.Builder builder = Protocol.Transactions.newBuilder();
    khts.forEach(kht -> builder.addTransactions(kht));
    this.transactions = builder.build();
    this.type = MessageTypes.khtS.asByte();
    this.data = this.transactions.toByteArray();
  }

  public TransactionsMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.khtS.asByte();
    this.transactions = Protocol.Transactions.parseFrom(getCodedInputStream(data));
    if (isFilter()) {
      compareBytes(data, transactions.toByteArray());
      TransactionCapsule.validContractProto(transactions.getTransactionsList());
    }
  }

  public Protocol.Transactions getTransactions() {
    return transactions;
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString()).append("kht size: ")
        .append(this.transactions.getTransactionsList().size()).toString();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

}
