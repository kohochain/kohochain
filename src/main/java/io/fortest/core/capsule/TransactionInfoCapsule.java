package io.fortest.core.capsule;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.fortest.core.config.args.Args;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import io.fortest.common.runtime.vm.LogInfo;
import io.fortest.common.runtime.vm.program.InternalTransaction;
import io.fortest.common.runtime.vm.program.ProgramResult;
import io.fortest.core.db.TransactionTrace;
import io.fortest.core.exception.BadItemException;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.TransactionInfo;
import io.fortest.protos.Protocol.TransactionInfo.Log;
import io.fortest.protos.Protocol.TransactionInfo.code;

@Slf4j(topic = "capsule")
public class TransactionInfoCapsule implements ProtoCapsule<TransactionInfo> {

  private TransactionInfo transactionInfo;

  /**
   * constructor TransactionCapsule.
   */
  public TransactionInfoCapsule(TransactionInfo khtRet) {
    this.transactionInfo = khtRet;
  }

  public TransactionInfoCapsule(byte[] data) throws BadItemException {
    try {
      this.transactionInfo = TransactionInfo.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new BadItemException("TransactionInfoCapsule proto data parse exception");
    }
  }

  public TransactionInfoCapsule() {
    this.transactionInfo = TransactionInfo.newBuilder().build();
  }

  public long getFee() {
    return transactionInfo.getFee();
  }

  public void setId(byte[] id) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .setId(ByteString.copyFrom(id)).build();
  }

  public byte[] getId() {
    return transactionInfo.getId().toByteArray();
  }


  public void setUnfreezeAmount(long amount) {
    this.transactionInfo = this.transactionInfo.toBuilder().setUnfreezeAmount(amount).build();
  }

  public long getUnfreezeAmount() {
    return transactionInfo.getUnfreezeAmount();
  }

  public void setWithdrawAmount(long amount) {
    this.transactionInfo = this.transactionInfo.toBuilder().setWithdrawAmount(amount).build();
  }

  public long getWithdrawAmount() {
    return transactionInfo.getWithdrawAmount();
  }

  public void setFee(long fee) {
    this.transactionInfo = this.transactionInfo.toBuilder().setFee(fee).build();
  }

  public void setResult(code result) {
    this.transactionInfo = this.transactionInfo.toBuilder().setResult(result).build();
  }

  public void setResMessage(String message) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .setResMessage(ByteString.copyFromUtf8(message)).build();
  }

  public void addFee(long fee) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .setFee(this.transactionInfo.getFee() + fee).build();
  }

  public long getBlockNumber() {
    return transactionInfo.getBlockNumber();
  }

  public void setBlockNumber(long num) {
    this.transactionInfo = this.transactionInfo.toBuilder().setBlockNumber(num)
        .build();
  }

  public long getBlockTimeStamp() {
    return transactionInfo.getBlockTimeStamp();
  }

  public void setBlockTimeStamp(long time) {
    this.transactionInfo = this.transactionInfo.toBuilder().setBlockTimeStamp(time)
        .build();
  }

  public void setContractResult(byte[] ret) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .addContractResult(ByteString.copyFrom(ret))
        .build();
  }

  public void setContractAddress(byte[] contractAddress) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .setContractAddress(ByteString.copyFrom(contractAddress))
        .build();
  }

  public void setReceipt(ReceiptCapsule receipt) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .setReceipt(receipt.getReceipt())
        .build();
  }


  public void addAllLog(List<Log> logs) {
    this.transactionInfo = this.transactionInfo.toBuilder()
        .addAllLog(logs)
        .build();
  }

  @Override
  public byte[] getData() {
    return this.transactionInfo.toByteArray();
  }

  @Override
  public TransactionInfo getInstance() {
    return this.transactionInfo;
  }

  public static TransactionInfoCapsule buildInstance(TransactionCapsule khtCap, BlockCapsule block,
      TransactionTrace trace) {

    TransactionInfo.Builder builder = TransactionInfo.newBuilder();
    ReceiptCapsule traceReceipt = trace.getReceipt();
    builder.setResult(code.SUCESS);
    if (StringUtils.isNoneEmpty(trace.getRuntimeError()) || Objects
        .nonNull(trace.getRuntimeResult().getException())) {
      builder.setResult(code.FAILED);
      builder.setResMessage(ByteString.copyFromUtf8(trace.getRuntimeError()));
    }
    builder.setId(ByteString.copyFrom(khtCap.getTransactionId().getBytes()));
    ProgramResult programResult = trace.getRuntimeResult();
    long fee =
        programResult.getRet().getFee() + traceReceipt.getEnergyFee()
            + traceReceipt.getNetFee() + traceReceipt.getMultiSignFee();
    ByteString contractResult = ByteString.copyFrom(programResult.getHReturn());
    ByteString ContractAddress = ByteString.copyFrom(programResult.getContractAddress());

    builder.setFee(fee);
    builder.addContractResult(contractResult);
    builder.setContractAddress(ContractAddress);
    builder.setUnfreezeAmount(programResult.getRet().getUnfreezeAmount());
    builder.setAssetIssueID(programResult.getRet().getAssetIssueID());
    builder.setExchangeId(programResult.getRet().getExchangeId());
    builder.setWithdrawAmount(programResult.getRet().getWithdrawAmount());
    builder.setExchangeReceivedAmount(programResult.getRet().getExchangeReceivedAmount());
    builder.setExchangeInjectAnotherAmount(programResult.getRet().getExchangeInjectAnotherAmount());
    builder.setExchangeWithdrawAnotherAmount(
        programResult.getRet().getExchangeWithdrawAnotherAmount());

    List<Log> logList = new ArrayList<>();
    programResult.getLogInfoList().forEach(
        logInfo -> {
          logList.add(LogInfo.buildLog(logInfo));
        }
    );
    builder.addAllLog(logList);

    if (Objects.nonNull(block)) {
      builder.setBlockNumber(block.getInstance().getBlockHeader().getRawData().getNumber());
      builder.setBlockTimeStamp(block.getInstance().getBlockHeader().getRawData().getTimestamp());
    }

    builder.setReceipt(traceReceipt.getReceipt());

    if (Args.getInstance().isSaveInternalTx() && null != programResult.getInternalTransactions()) {
      for (InternalTransaction internalTransaction : programResult
          .getInternalTransactions()) {
        Protocol.InternalTransaction.Builder internalkhtBuilder = Protocol.InternalTransaction
            .newBuilder();
        // set hash
        internalkhtBuilder.setHash(ByteString.copyFrom(internalTransaction.getHash()));
        // set caller
        internalkhtBuilder.setCallerAddress(ByteString.copyFrom(internalTransaction.getSender()));
        // set TransferTo
        internalkhtBuilder
            .setTransferToAddress(ByteString.copyFrom(internalTransaction.getTransferToAddress()));
        //TODO: "for loop" below in future for multiple token case, we only have one for now.
        Protocol.InternalTransaction.CallValueInfo.Builder callValueInfoBuilder =
            Protocol.InternalTransaction.CallValueInfo.newBuilder();
        // kht will not be set token name
        callValueInfoBuilder.setCallValue(internalTransaction.getValue());
        // Just one transferBuilder for now.
        internalkhtBuilder.addCallValueInfo(callValueInfoBuilder);
        internalTransaction.getTokenInfo().forEach((tokenId, amount) -> {
          Protocol.InternalTransaction.CallValueInfo.Builder tokenInfoBuilder =
              Protocol.InternalTransaction.CallValueInfo.newBuilder();
          tokenInfoBuilder.setTokenId(tokenId);
          tokenInfoBuilder.setCallValue(amount);
          internalkhtBuilder.addCallValueInfo(tokenInfoBuilder);
        });
        // Token for loop end here
        internalkhtBuilder.setNote(ByteString.copyFrom(internalTransaction.getNote().getBytes()));
        internalkhtBuilder.setRejected(internalTransaction.isRejected());
        builder.addInternalTransactions(internalkhtBuilder);
      }
    }

    return new TransactionInfoCapsule(builder.build());
  }
}