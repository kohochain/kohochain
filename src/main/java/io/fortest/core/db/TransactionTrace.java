package io.fortest.core.db;

import java.util.Objects;

import io.fortest.common.runtime.Runtime;
import io.fortest.common.runtime.RuntimeImpl;
import io.fortest.common.runtime.config.VMConfig;
import io.fortest.common.runtime.vm.program.InternalTransaction;
import io.fortest.common.runtime.vm.program.Program;
import io.fortest.common.runtime.vm.program.ProgramResult;
import io.fortest.common.runtime.vm.program.invoke.ProgramInvokeFactoryImpl;
import io.fortest.common.storage.DepositImpl;
import io.fortest.common.utils.Sha256Hash;
import io.fortest.core.Constant;
import io.fortest.core.Wallet;
import io.fortest.core.config.args.Args;
import io.fortest.protos.Contract;
import io.fortest.protos.Protocol;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.spongycastle.util.encoders.Hex;
import org.springframework.util.StringUtils;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.ReceiptCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.exception.BalanceInsufficientException;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.ReceiptCheckErrException;
import io.fortest.core.exception.VMIllegalException;

@Slf4j(topic = "TransactionTrace")
public class TransactionTrace {

  private TransactionCapsule kht;

  private ReceiptCapsule receipt;

  private Manager dbManager;

  private Runtime runtime;

  private EnergyProcessor energyProcessor;

  private InternalTransaction.khtType khtType;

  private long txStartTimeInMs;

  public TransactionCapsule getkht() {
    return kht;
  }

  public enum TimeResultType {
    NORMAL,
    LONG_RUNNING,
    OUT_OF_TIME
  }

  @Getter
  @Setter
  private TimeResultType timeResultType = TimeResultType.NORMAL;

  public TransactionTrace(TransactionCapsule kht, Manager dbManager) {
    this.kht = kht;
    Protocol.Transaction.Contract.ContractType contractType = this.kht.getInstance().getRawData()
        .getContract(0).getType();
    switch (contractType.getNumber()) {
      case Protocol.Transaction.Contract.ContractType.TriggerSmartContract_VALUE:
        khtType = InternalTransaction.khtType.kht_CONTRACT_CALL_TYPE;
        break;
      case Protocol.Transaction.Contract.ContractType.CreateSmartContract_VALUE:
        khtType = InternalTransaction.khtType.kht_CONTRACT_CREATION_TYPE;
        break;
      default:
        khtType = InternalTransaction.khtType.kht_PRECOMPILED_TYPE;
    }

    this.dbManager = dbManager;
    this.receipt = new ReceiptCapsule(Sha256Hash.ZERO_HASH);

    this.energyProcessor = new EnergyProcessor(this.dbManager);
  }

  private boolean needVM() {
    return this.khtType == InternalTransaction.khtType.kht_CONTRACT_CALL_TYPE || this.khtType == InternalTransaction.khtType.kht_CONTRACT_CREATION_TYPE;
  }

  public void init(BlockCapsule blockCap) {
    init(blockCap, false);
  }

  //pre transaction check
  public void init(BlockCapsule blockCap, boolean eventPluginLoaded) {
    txStartTimeInMs = System.currentTimeMillis();
    DepositImpl deposit = DepositImpl.createRoot(dbManager);
    runtime = new RuntimeImpl(this, blockCap, deposit, new ProgramInvokeFactoryImpl());
    runtime.setEnableEventLinstener(eventPluginLoaded);
  }

  public void checkIsConstant() throws ContractValidateException, VMIllegalException {
    if (VMConfig.allowTvmConstantinople()) {
      return;
    }

    Contract.TriggerSmartContract triggerContractFromTransaction = ContractCapsule
        .getTriggerContractFromTransaction(this.getkht().getInstance());
    if (InternalTransaction.khtType.kht_CONTRACT_CALL_TYPE == this.khtType) {
      DepositImpl deposit = DepositImpl.createRoot(dbManager);
      ContractCapsule contract = deposit
          .getContract(triggerContractFromTransaction.getContractAddress().toByteArray());
      if (contract == null) {
        logger.info("contract: {} is not in contract store", Wallet
            .encode58Check(triggerContractFromTransaction.getContractAddress().toByteArray()));
        throw new ContractValidateException("contract: " + Wallet
            .encode58Check(triggerContractFromTransaction.getContractAddress().toByteArray())
            + " is not in contract store");
      }
      Protocol.SmartContract.ABI abi = contract.getInstance().getAbi();
      if (Wallet.isConstant(abi, triggerContractFromTransaction)) {
        throw new VMIllegalException("cannot call constant method");
      }
    }
  }

  //set bill
  public void setBill(long energyUsage) {
    if (energyUsage < 0) {
      energyUsage = 0L;
    }
    receipt.setEnergyUsageTotal(energyUsage);
  }

  //set net bill
  public void setNetBill(long netUsage, long netFee) {
    receipt.setNetUsage(netUsage);
    receipt.setNetFee(netFee);
  }

  public void addNetBill(long netFee) {
    receipt.addNetFee(netFee);
  }

  public void exec()
      throws ContractExeException, ContractValidateException, VMIllegalException {
    /*  VM execute  */
    runtime.execute();
    runtime.go();

    if (InternalTransaction.khtType.kht_PRECOMPILED_TYPE != runtime.getkhtType()) {
      if (Protocol.Transaction.Result.contractResult.OUT_OF_TIME
          .equals(receipt.getResult())) {
        setTimeResultType(TimeResultType.OUT_OF_TIME);
      } else if (System.currentTimeMillis() - txStartTimeInMs
          > Args.getInstance().getLongRunningTime()) {
        setTimeResultType(TimeResultType.LONG_RUNNING);
      }
    }
  }

  public void finalization() throws ContractExeException {
    try {
      pay();
    } catch (BalanceInsufficientException e) {
      throw new ContractExeException(e.getMessage());
    }
    runtime.finalization();
  }

  /**
   * pay actually bill(include ENERGY and storage).
   */
  public void pay() throws BalanceInsufficientException {
    byte[] originAccount;
    byte[] callerAccount;
    long percent = 0;
    long originEnergyLimit = 0;
    switch (khtType) {
      case kht_CONTRACT_CREATION_TYPE:
        callerAccount = TransactionCapsule.getOwner(kht.getInstance().getRawData().getContract(0));
        originAccount = callerAccount;
        break;
      case kht_CONTRACT_CALL_TYPE:
        Contract.TriggerSmartContract callContract = ContractCapsule
            .getTriggerContractFromTransaction(kht.getInstance());
        ContractCapsule contractCapsule =
            dbManager.getContractStore().get(callContract.getContractAddress().toByteArray());

        callerAccount = callContract.getOwnerAddress().toByteArray();
        originAccount = contractCapsule.getOriginAddress();
        percent = Math
            .max(Constant.ONE_HUNDRED - contractCapsule.getConsumeUserResourcePercent(), 0);
        percent = Math.min(percent, Constant.ONE_HUNDRED);
        originEnergyLimit = contractCapsule.getOriginEnergyLimit();
        break;
      default:
        return;
    }

    // originAccount Percent = 30%
    AccountCapsule origin = dbManager.getAccountStore().get(originAccount);
    AccountCapsule caller = dbManager.getAccountStore().get(callerAccount);
    receipt.payEnergyBill(
        dbManager,
        origin,
        caller,
        percent, originEnergyLimit,
        energyProcessor,
        dbManager.getWitnessController().getHeadSlot());
  }

  public boolean checkNeedRetry() {
    if (!needVM()) {
      return false;
    }
    return kht.getContractRet() != Protocol.Transaction.Result.contractResult.OUT_OF_TIME && receipt.getResult()
        == Protocol.Transaction.Result.contractResult.OUT_OF_TIME;
  }

  public void check() throws ReceiptCheckErrException {
    if (!needVM()) {
      return;
    }
    if (Objects.isNull(kht.getContractRet())) {
      throw new ReceiptCheckErrException("null resultCode");
    }
    if (!kht.getContractRet().equals(receipt.getResult())) {
      logger.info(
          "this tx id: {}, the resultCode in received block: {}, the resultCode in self: {}",
          Hex.toHexString(kht.getTransactionId().getBytes()), kht.getContractRet(),
          receipt.getResult());
      throw new ReceiptCheckErrException("Different resultCode");
    }
  }

  public ReceiptCapsule getReceipt() {
    return receipt;
  }

  public void setResult() {
    if (!needVM()) {
      return;
    }
    RuntimeException exception = runtime.getResult().getException();
    if (Objects.isNull(exception) && StringUtils
        .isEmpty(runtime.getRuntimeError()) && !runtime.getResult().isRevert()) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.SUCCESS);
      return;
    }
    if (runtime.getResult().isRevert()) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.REVERT);
      return;
    }
    if (exception instanceof Program.IllegalOperationException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.ILLEGAL_OPERATION);
      return;
    }
    if (exception instanceof Program.OutOfEnergyException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.OUT_OF_ENERGY);
      return;
    }
    if (exception instanceof Program.BadJumpDestinationException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.BAD_JUMP_DESTINATION);
      return;
    }
    if (exception instanceof Program.OutOfTimeException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.OUT_OF_TIME);
      return;
    }
    if (exception instanceof Program.OutOfMemoryException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.OUT_OF_MEMORY);
      return;
    }
    if (exception instanceof Program.PrecompiledContractException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.PRECOMPILED_CONTRACT);
      return;
    }
    if (exception instanceof Program.StackTooSmallException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.STACK_TOO_SMALL);
      return;
    }
    if (exception instanceof Program.StackTooLargeException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.STACK_TOO_LARGE);
      return;
    }
    if (exception instanceof Program.JVMStackOverFlowException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.JVM_STACK_OVER_FLOW);
      return;
    }
    if (exception instanceof Program.TransferException) {
      receipt.setResult(Protocol.Transaction.Result.contractResult.TRANSFER_FAILED);
      return;
    }

    logger.info("uncaught exception", exception);
    receipt.setResult(Protocol.Transaction.Result.contractResult.UNKNOWN);
  }

  public String getRuntimeError() {
    return runtime.getRuntimeError();
  }

  public ProgramResult getRuntimeResult() {
    return runtime.getResult();
  }

  public Runtime getRuntime() {
    return runtime;
  }
}
