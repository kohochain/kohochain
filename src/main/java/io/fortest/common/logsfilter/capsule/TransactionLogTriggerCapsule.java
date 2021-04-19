package io.fortest.common.logsfilter.capsule;

import static io.fortest.protos.Protocol.Transaction.Contract.ContractType.TransferAssetContract;
import static io.fortest.protos.Protocol.Transaction.Contract.ContractType.TransferContract;

import com.google.protobuf.Any;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.protobuf.ByteString;
import io.fortest.common.logsfilter.EventPluginLoader;
import io.fortest.common.logsfilter.trigger.InternalTransactionPojo;
import io.fortest.common.logsfilter.trigger.TransactionLogTrigger;
import io.fortest.common.runtime.vm.program.InternalTransaction;
import io.fortest.common.runtime.vm.program.ProgramResult;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import io.fortest.core.Wallet;
import io.fortest.core.db.TransactionTrace;
import io.fortest.protos.Contract.TransferAssetContract;
import io.fortest.protos.Contract.TransferContract;
import io.fortest.protos.Protocol;

@Slf4j
public class TransactionLogTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  TransactionLogTrigger transactionLogTrigger;

  public void setLatestSolidifiedBlockNumber(long latestSolidifiedBlockNumber) {
    transactionLogTrigger.setLatestSolidifiedBlockNumber(latestSolidifiedBlockNumber);
  }

  public TransactionLogTriggerCapsule(TransactionCapsule khtCasule, BlockCapsule blockCapsule) {
    transactionLogTrigger = new TransactionLogTrigger();
    if (Objects.nonNull(blockCapsule)) {
      transactionLogTrigger.setBlockHash(blockCapsule.getBlockId().toString());
    }
    transactionLogTrigger.setTransactionId(khtCasule.getTransactionId().toString());
    transactionLogTrigger.setTimeStamp(blockCapsule.getTimeStamp());
    transactionLogTrigger.setBlockNumber(khtCasule.getBlockNum());

    TransactionTrace khtTrace = khtCasule.getkhtTrace();

    //result
    if (Objects.nonNull(khtCasule.getContractRet())) {
      transactionLogTrigger.setResult(khtCasule.getContractRet().toString());
    }

    if (Objects.nonNull(khtCasule.getInstance().getRawData())) {
      // feelimit
      transactionLogTrigger.setFeeLimit(khtCasule.getInstance().getRawData().getFeeLimit());

      Protocol.Transaction.Contract contract = khtCasule.getInstance().getRawData().getContract(0);
      Any contractParameter = null;
      // contract type
      if (Objects.nonNull(contract)) {
        Protocol.Transaction.Contract.ContractType contractType = contract.getType();
        if (Objects.nonNull(contractType)) {
          transactionLogTrigger.setContractType(contractType.toString());
        }

        contractParameter = contract.getParameter();

        transactionLogTrigger.setContractCallValue(TransactionCapsule.getCallValue(contract));
      }

      if (Objects.nonNull(contractParameter) && Objects.nonNull(contract)) {
        try {
          if (contract.getType() == TransferContract) {
            TransferContract contractTransfer = contractParameter.unpack(TransferContract.class);

            if (Objects.nonNull(contractTransfer)) {
              transactionLogTrigger.setAssetName("fortest");

              if (Objects.nonNull(contractTransfer.getOwnerAddress())) {
                transactionLogTrigger.setFromAddress(
                    Wallet.encode58Check(contractTransfer.getOwnerAddress().toByteArray()));
              }

              if (Objects.nonNull(contractTransfer.getToAddress())) {
                transactionLogTrigger.setToAddress(
                    Wallet.encode58Check(contractTransfer.getToAddress().toByteArray()));
              }

              transactionLogTrigger.setAssetAmount(contractTransfer.getAmount());
            }

          } else if (contract.getType() == TransferAssetContract) {
            TransferAssetContract contractTransfer = contractParameter
                .unpack(TransferAssetContract.class);

            if (Objects.nonNull(contractTransfer)) {
              if (Objects.nonNull(contractTransfer.getAssetName())) {
                transactionLogTrigger.setAssetName(contractTransfer.getAssetName().toStringUtf8());
              }

              if (Objects.nonNull(contractTransfer.getOwnerAddress())) {
                transactionLogTrigger.setFromAddress(
                    Wallet.encode58Check(contractTransfer.getOwnerAddress().toByteArray()));
              }

              if (Objects.nonNull(contractTransfer.getToAddress())) {
                transactionLogTrigger.setToAddress(
                    Wallet.encode58Check(contractTransfer.getToAddress().toByteArray()));
              }
              transactionLogTrigger.setAssetAmount(contractTransfer.getAmount());
            }
          }
        } catch (Exception e) {
          logger.error("failed to load transferAssetContract, error'{}'", e);
        }
      }
    }

    // receipt
    if (Objects.nonNull(khtTrace) && Objects.nonNull(khtTrace.getReceipt())) {
      transactionLogTrigger.setEnergyFee(khtTrace.getReceipt().getEnergyFee());
      transactionLogTrigger.setOriginEnergyUsage(khtTrace.getReceipt().getOriginEnergyUsage());
      transactionLogTrigger.setEnergyUsageTotal(khtTrace.getReceipt().getEnergyUsageTotal());
      transactionLogTrigger.setNetUsage(khtTrace.getReceipt().getNetUsage());
      transactionLogTrigger.setNetFee(khtTrace.getReceipt().getNetFee());
      transactionLogTrigger.setEnergyUsage(khtTrace.getReceipt().getEnergyUsage());
    }

    // program result
    if (Objects.nonNull(khtTrace) && Objects.nonNull(khtTrace.getRuntime()) &&  Objects.nonNull(khtTrace.getRuntime().getResult())) {
      ProgramResult programResult = khtTrace.getRuntime().getResult();
      ByteString contractResult = ByteString.copyFrom(programResult.getHReturn());
      ByteString contractAddress = ByteString.copyFrom(programResult.getContractAddress());

      if (Objects.nonNull(contractResult) && contractResult.size() > 0) {
        transactionLogTrigger.setContractResult(Hex.toHexString(contractResult.toByteArray()));
      }

      if (Objects.nonNull(contractAddress) && contractAddress.size() > 0) {
        transactionLogTrigger
            .setContractAddress(Wallet.encode58Check((contractAddress.toByteArray())));
      }

      // internal transaction
      transactionLogTrigger.setInternalTrananctionList(
          getInternalTransactionList(programResult.getInternalTransactions()));
    }
  }

  private List<InternalTransactionPojo> getInternalTransactionList(
      List<InternalTransaction> internalTransactionList) {
    List<InternalTransactionPojo> pojoList = new ArrayList<>();

    internalTransactionList.forEach(internalTransaction -> {
      InternalTransactionPojo item = new InternalTransactionPojo();

      item.setHash(Hex.toHexString(internalTransaction.getHash()));
      item.setCallValue(internalTransaction.getValue());
      item.setTokenInfo(internalTransaction.getTokenInfo());
      item.setCaller_address(Hex.toHexString(internalTransaction.getSender()));
      item.setTransferTo_address(Hex.toHexString(internalTransaction.getTransferToAddress()));
      item.setData(Hex.toHexString(internalTransaction.getData()));
      item.setRejected(internalTransaction.isRejected());
      item.setNote(internalTransaction.getNote());

      pojoList.add(item);
    });

    return pojoList;
  }

  @Override
  public void processTrigger() {
    EventPluginLoader.getInstance().postTransactionTrigger(transactionLogTrigger);
  }
}
