package io.fortest.core;

import com.google.protobuf.ByteString;
import java.util.List;

import io.fortest.api.GrpcAPI;
import io.fortest.common.utils.ByteArray;
import io.fortest.protos.Protocol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.fortest.core.db.api.StoreAPI;

@Slf4j
@Component
public class WalletSolidity {

  @Autowired
  private StoreAPI storeAPI;

  public GrpcAPI.TransactionList getTransactionsFromThis(ByteString thisAddress, long offset, long limit) {
    List<Protocol.Transaction> transactionsFromThis = storeAPI
        .getTransactionsFromThis(ByteArray.toHexString(thisAddress.toByteArray()), offset, limit);
    GrpcAPI.TransactionList transactionList = GrpcAPI.TransactionList.newBuilder()
        .addAllTransaction(transactionsFromThis).build();
    return transactionList;
  }

  public GrpcAPI.TransactionList getTransactionsToThis(ByteString toAddress, long offset, long limit) {
    List<Protocol.Transaction> transactionsToThis = storeAPI
        .getTransactionsToThis(ByteArray.toHexString(toAddress.toByteArray()), offset, limit);
    GrpcAPI.TransactionList transactionList = GrpcAPI.TransactionList.newBuilder()
        .addAllTransaction(transactionsToThis).build();
    return transactionList;
  }
}
