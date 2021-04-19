package io.fortest.common.runtime;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.commons.lang3.ArrayUtils.getLength;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static io.fortest.common.runtime.utils.MUtil.convertTokhcAddress;
import static io.fortest.common.runtime.utils.MUtil.transfer;
import static io.fortest.common.runtime.utils.MUtil.transferToken;

import com.google.protobuf.ByteString;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.fortest.core.actuator.Actuator;
import io.fortest.core.actuator.ActuatorFactory;
import io.fortest.core.capsule.AccountCapsule;
import io.fortest.core.capsule.BlockCapsule;
import io.fortest.core.capsule.ContractCapsule;
import io.fortest.core.capsule.TransactionCapsule;
import io.fortest.core.config.args.Args;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.VMIllegalException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.spongycastle.util.encoders.Hex;
import io.fortest.common.logsfilter.EventPluginLoader;
import io.fortest.common.logsfilter.trigger.ContractTrigger;
import io.fortest.common.runtime.config.VMConfig;
import io.fortest.common.runtime.vm.DataWord;
import io.fortest.common.runtime.vm.EnergyCost;
import io.fortest.common.runtime.vm.LogInfoTriggerParser;
import io.fortest.common.runtime.vm.VM;
import io.fortest.common.runtime.vm.VMConstant;
import io.fortest.common.runtime.vm.VMUtils;
import io.fortest.common.runtime.vm.program.InternalTransaction;
import io.fortest.common.runtime.vm.program.InternalTransaction.ExecutorType;
import io.fortest.common.runtime.vm.program.InternalTransaction.khtType;
import io.fortest.common.runtime.vm.program.Program;
import io.fortest.common.runtime.vm.program.Program.JVMStackOverFlowException;
import io.fortest.common.runtime.vm.program.Program.OutOfTimeException;
import io.fortest.common.runtime.vm.program.Program.TransferException;
import io.fortest.common.runtime.vm.program.ProgramPrecompile;
import io.fortest.common.runtime.vm.program.ProgramResult;
import io.fortest.common.runtime.vm.program.invoke.ProgramInvoke;
import io.fortest.common.runtime.vm.program.invoke.ProgramInvokeFactory;
import io.fortest.common.storage.Deposit;
import io.fortest.common.storage.DepositImpl;
import io.fortest.core.Constant;
import io.fortest.core.Wallet;
import io.fortest.core.db.EnergyProcessor;
import io.fortest.core.db.TransactionTrace;
import io.fortest.protos.Contract;
import io.fortest.protos.Contract.CreateSmartContract;
import io.fortest.protos.Contract.TriggerSmartContract;
import io.fortest.protos.Protocol;
import io.fortest.protos.Protocol.Block;
import io.fortest.protos.Protocol.SmartContract;
import io.fortest.protos.Protocol.Transaction;
import io.fortest.protos.Protocol.Transaction.Contract.ContractType;
import io.fortest.protos.Protocol.Transaction.Result.contractResult;

@Slf4j(topic = "VM")
public class RuntimeImpl implements Runtime {

  private VMConfig config = VMConfig.getInstance();

  private Transaction kht;
  private BlockCapsule blockCap;
  private Deposit deposit;
  private ProgramInvokeFactory programInvokeFactory;
  private String runtimeError;

  private EnergyProcessor energyProcessor;
  private ProgramResult result = new ProgramResult();

  private VM vm;
  private Program program;
  private InternalTransaction rootInternalTransaction;

  @Getter
  @Setter
  private InternalTransaction.khtType khtType;
  private ExecutorType executorType;

  //tx trace
  private TransactionTrace trace;

  @Getter
  @Setter
  private boolean isStaticCall = false;

  @Setter
  private boolean enableEventLinstener;

  private LogInfoTriggerParser logInfoTriggerParser;

  /**
   * For blockCap's kht run
   */
  public RuntimeImpl(TransactionTrace trace, BlockCapsule block, Deposit deposit,
      ProgramInvokeFactory programInvokeFactory) {
    this.trace = trace;
    this.kht = trace.getkht().getInstance();

    if (Objects.nonNull(block)) {
      this.blockCap = block;
      this.executorType = ExecutorType.ET_NORMAL_TYPE;
    } else {
      this.blockCap = new BlockCapsule(Block.newBuilder().build());
      this.executorType = ExecutorType.ET_PRE_TYPE;
    }
    this.deposit = deposit;
    this.programInvokeFactory = programInvokeFactory;
    this.energyProcessor = new EnergyProcessor(deposit.getDbManager());

    ContractType contractType = this.kht.getRawData().getContract(0).getType();
    switch (contractType.getNumber()) {
      case ContractType.TriggerSmartContract_VALUE:
        khtType = khtType.kht_CONTRACT_CALL_TYPE;
        break;
      case ContractType.CreateSmartContract_VALUE:
        khtType = khtType.kht_CONTRACT_CREATION_TYPE;
        break;
      default:
        khtType = khtType.kht_PRECOMPILED_TYPE;
    }
  }


  /**
   * For constant kht with latest blockCap.
   */
  public RuntimeImpl(Transaction tx, BlockCapsule block, DepositImpl deposit,
      ProgramInvokeFactory programInvokeFactory, boolean isStaticCall) {
    this(tx, block, deposit, programInvokeFactory);
    this.isStaticCall = isStaticCall;
  }

  private RuntimeImpl(Transaction tx, BlockCapsule block, DepositImpl deposit,
      ProgramInvokeFactory programInvokeFactory) {
    this.kht = tx;
    this.deposit = deposit;
    this.programInvokeFactory = programInvokeFactory;
    this.executorType = ExecutorType.ET_PRE_TYPE;
    this.blockCap = block;
    this.energyProcessor = new EnergyProcessor(deposit.getDbManager());
    ContractType contractType = tx.getRawData().getContract(0).getType();
    switch (contractType.getNumber()) {
      case ContractType.TriggerSmartContract_VALUE:
        khtType = khtType.kht_CONTRACT_CALL_TYPE;
        break;
      case ContractType.CreateSmartContract_VALUE:
        khtType = khtType.kht_CONTRACT_CREATION_TYPE;
        break;
      default:
        khtType = khtType.kht_PRECOMPILED_TYPE;
    }
  }


  private void precompiled() throws ContractValidateException, ContractExeException {
    TransactionCapsule khtCap = new TransactionCapsule(kht);
    final List<Actuator> actuatorList = ActuatorFactory
        .createActuator(khtCap, deposit.getDbManager());

    for (Actuator act : actuatorList) {
      act.validate();
      act.execute(result.getRet());
    }
  }

  @Override
  public void execute()
      throws ContractValidateException, ContractExeException, VMIllegalException {
    switch (khtType) {
      case kht_PRECOMPILED_TYPE:
        precompiled();
        break;
      case kht_CONTRACT_CREATION_TYPE:
        create();
        break;
      case kht_CONTRACT_CALL_TYPE:
        call();
        break;
      default:
        throw new ContractValidateException("Unknown contract type");
    }
  }

  public long getAccountEnergyLimitWithFixRatio(AccountCapsule account, long feeLimit,
                                                long callValue) {

    long sunPerEnergy = Constant.SUN_PER_ENERGY;
    if (deposit.getDbManager().getDynamicPropertiesStore().getEnergyFee() > 0) {
      sunPerEnergy = deposit.getDbManager().getDynamicPropertiesStore().getEnergyFee();
    }

    long leftFrozenEnergy = energyProcessor.getAccountLeftEnergyFromFreeze(account);

    long energyFromBalance = max(account.getBalance() - callValue, 0) / sunPerEnergy;
    long availableEnergy = Math.addExact(leftFrozenEnergy, energyFromBalance);

    long energyFromFeeLimit = feeLimit / sunPerEnergy;
    return min(availableEnergy, energyFromFeeLimit);

  }

  private long getAccountEnergyLimitWithFloatRatio(AccountCapsule account, long feeLimit,
      long callValue) {

    long sunPerEnergy = Constant.SUN_PER_ENERGY;
    if (deposit.getDbManager().getDynamicPropertiesStore().getEnergyFee() > 0) {
      sunPerEnergy = deposit.getDbManager().getDynamicPropertiesStore().getEnergyFee();
    }
    // can change the calc way
    long leftEnergyFromFreeze = energyProcessor.getAccountLeftEnergyFromFreeze(account);
    callValue = max(callValue, 0);
    long energyFromBalance = Math
        .floorDiv(max(account.getBalance() - callValue, 0), sunPerEnergy);

    long energyFromFeeLimit;
    long totalBalanceForEnergyFreeze = account.getAllFrozenBalanceForEnergy();
    if (0 == totalBalanceForEnergyFreeze) {
      energyFromFeeLimit =
          feeLimit / sunPerEnergy;
    } else {
      long totalEnergyFromFreeze = energyProcessor
          .calculateGlobalEnergyLimit(account);
      long leftBalanceForEnergyFreeze = getEnergyFee(totalBalanceForEnergyFreeze,
          leftEnergyFromFreeze,
          totalEnergyFromFreeze);

      if (leftBalanceForEnergyFreeze >= feeLimit) {
        energyFromFeeLimit = BigInteger.valueOf(totalEnergyFromFreeze)
            .multiply(BigInteger.valueOf(feeLimit))
            .divide(BigInteger.valueOf(totalBalanceForEnergyFreeze)).longValueExact();
      } else {
        energyFromFeeLimit = Math
            .addExact(leftEnergyFromFreeze,
                (feeLimit - leftBalanceForEnergyFreeze) / sunPerEnergy);
      }
    }

    return min(Math.addExact(leftEnergyFromFreeze, energyFromBalance), energyFromFeeLimit);
  }

  private long getTotalEnergyLimitWithFloatRatio(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, long feeLimit, long callValue) {

    long callerEnergyLimit = getAccountEnergyLimitWithFloatRatio(caller, feeLimit, callValue);
    if (Arrays.equals(creator.getAddress().toByteArray(), caller.getAddress().toByteArray())) {
      return callerEnergyLimit;
    }

    // creatorEnergyFromFreeze
    long creatorEnergyLimit = energyProcessor.getAccountLeftEnergyFromFreeze(creator);

    ContractCapsule contractCapsule = this.deposit
        .getContract(contract.getContractAddress().toByteArray());
    long consumeUserResourcePercent = contractCapsule.getConsumeUserResourcePercent();

    if (creatorEnergyLimit * consumeUserResourcePercent
        > (Constant.ONE_HUNDRED - consumeUserResourcePercent) * callerEnergyLimit) {
      return Math.floorDiv(callerEnergyLimit * Constant.ONE_HUNDRED, consumeUserResourcePercent);
    } else {
      return Math.addExact(callerEnergyLimit, creatorEnergyLimit);
    }
  }

  public long getTotalEnergyLimitWithFixRatio(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, long feeLimit, long callValue)
      throws ContractValidateException {

    long callerEnergyLimit = getAccountEnergyLimitWithFixRatio(caller, feeLimit, callValue);
    if (Arrays.equals(creator.getAddress().toByteArray(), caller.getAddress().toByteArray())) {
      // when the creator calls his own contract, this logic will be used.
      // so, the creator must use a BIG feeLimit to call his own contract,
      // which will cost the feeLimit kht when the creator's frozen energy is 0.
      return callerEnergyLimit;
    }

    long creatorEnergyLimit = 0;
    ContractCapsule contractCapsule = this.deposit
        .getContract(contract.getContractAddress().toByteArray());
    long consumeUserResourcePercent = contractCapsule.getConsumeUserResourcePercent();

    long originEnergyLimit = contractCapsule.getOriginEnergyLimit();
    if (originEnergyLimit < 0) {
      throw new ContractValidateException("originEnergyLimit can't be < 0");
    }

    if (consumeUserResourcePercent <= 0) {
      creatorEnergyLimit = min(energyProcessor.getAccountLeftEnergyFromFreeze(creator),
          originEnergyLimit);
    } else {
      if (consumeUserResourcePercent < Constant.ONE_HUNDRED) {
        // creatorEnergyLimit =
        // min(callerEnergyLimit * (100 - percent) / percent, creatorLeftFrozenEnergy, originEnergyLimit)

        creatorEnergyLimit = min(
            BigInteger.valueOf(callerEnergyLimit)
                .multiply(BigInteger.valueOf(Constant.ONE_HUNDRED - consumeUserResourcePercent))
                .divide(BigInteger.valueOf(consumeUserResourcePercent)).longValueExact(),
            min(energyProcessor.getAccountLeftEnergyFromFreeze(creator), originEnergyLimit)
        );
      }
    }
    return Math.addExact(callerEnergyLimit, creatorEnergyLimit);
  }

  public long getTotalEnergyLimit(AccountCapsule creator, AccountCapsule caller,
      TriggerSmartContract contract, long feeLimit, long callValue)
      throws ContractValidateException {
    if (Objects.isNull(creator) && VMConfig.allowTvmConstantinople()) {
      return getAccountEnergyLimitWithFixRatio(caller, feeLimit, callValue);
    }
    //  according to version
    if (VMConfig.getEnergyLimitHardFork()) {
      return getTotalEnergyLimitWithFixRatio(creator, caller, contract, feeLimit, callValue);
    } else {
      return getTotalEnergyLimitWithFloatRatio(creator, caller, contract, feeLimit, callValue);
    }
  }

  private boolean isCheckTransaction() {
    return this.blockCap != null && !this.blockCap.getInstance().getBlockHeader()
        .getWitnessSignature().isEmpty();
  }

  private double getCpuLimitInUsRatio() {

    double cpuLimitRatio;

    if (ExecutorType.ET_NORMAL_TYPE == executorType) {
      // self witness generates block
      if (this.blockCap != null && blockCap.generatedByMyself &&
          this.blockCap.getInstance().getBlockHeader().getWitnessSignature().isEmpty()) {
        cpuLimitRatio = 1.0;
      } else {
        // self witness or other witness or fullnode verifies block
        if (kht.getRet(0).getContractRet() == contractResult.OUT_OF_TIME) {
          cpuLimitRatio = Args.getInstance().getMinTimeRatio();
        } else {
          cpuLimitRatio = Args.getInstance().getMaxTimeRatio();
        }
      }
    } else {
      // self witness or other witness or fullnode receives tx
      cpuLimitRatio = 1.0;
    }

    return cpuLimitRatio;
  }

  /*
   **/
  private void create()
      throws ContractValidateException, VMIllegalException {
    if (!deposit.getDbManager().getDynamicPropertiesStore().supportVM()) {
      throw new ContractValidateException("vm work is off, need to be opened by the committee");
    }

    CreateSmartContract contract = ContractCapsule.getSmartContractFromTransaction(kht);
    if (contract == null) {
      throw new ContractValidateException("Cannot get CreateSmartContract from transaction");
    }
    SmartContract newSmartContract = contract.getNewContract();
    if (!contract.getOwnerAddress().equals(newSmartContract.getOriginAddress())) {
      logger.info("OwnerAddress not equals OriginAddress");
      throw new VMIllegalException("OwnerAddress is not equals OriginAddress");
    }

    byte[] contractName = newSmartContract.getName().getBytes();

    if (contractName.length > VMConstant.CONTRACT_NAME_LENGTH) {
      throw new ContractValidateException("contractName's length cannot be greater than 32");
    }

    long percent = contract.getNewContract().getConsumeUserResourcePercent();
    if (percent < 0 || percent > Constant.ONE_HUNDRED) {
      throw new ContractValidateException("percent must be >= 0 and <= 100");
    }

    byte[] contractAddress = Wallet.generateContractAddress(kht);
    // insure the new contract address haven't exist
    if (deposit.getAccount(contractAddress) != null) {
      throw new ContractValidateException(
          "Trying to create a contract with existing contract address: " + Wallet
              .encode58Check(contractAddress));
    }

    newSmartContract = newSmartContract.toBuilder()
        .setContractAddress(ByteString.copyFrom(contractAddress)).build();
    long callValue = newSmartContract.getCallValue();
    long tokenValue = 0;
    long tokenId = 0;
    if (VMConfig.allowTvmTransferTrc10()) {
      tokenValue = contract.getCallTokenValue();
      tokenId = contract.getTokenId();
    }
    byte[] callerAddress = contract.getOwnerAddress().toByteArray();
    // create vm to constructor smart contract
    try {
      long feeLimit = kht.getRawData().getFeeLimit();
      if (feeLimit < 0 || feeLimit > VMConfig.MAX_FEE_LIMIT) {
        logger.info("invalid feeLimit {}", feeLimit);
        throw new ContractValidateException(
            "feeLimit must be >= 0 and <= " + VMConfig.MAX_FEE_LIMIT);
      }
      AccountCapsule creator = this.deposit
          .getAccount(newSmartContract.getOriginAddress().toByteArray());

      long energyLimit;
      // according to version

      if (VMConfig.getEnergyLimitHardFork()) {
        if (callValue < 0) {
          throw new ContractValidateException("callValue must >= 0");
        }
        if (tokenValue < 0) {
          throw new ContractValidateException("tokenValue must >= 0");
        }
        if (newSmartContract.getOriginEnergyLimit() <= 0) {
          throw new ContractValidateException("The originEnergyLimit must be > 0");
        }
        energyLimit = getAccountEnergyLimitWithFixRatio(creator, feeLimit, callValue);
      } else {
        energyLimit = getAccountEnergyLimitWithFloatRatio(creator, feeLimit, callValue);
      }

      checkTokenValueAndId(tokenValue, tokenId);

      byte[] ops = newSmartContract.getBytecode().toByteArray();
      rootInternalTransaction = new InternalTransaction(kht, khtType);

      long maxCpuTimeOfOneTx = deposit.getDbManager().getDynamicPropertiesStore()
          .getMaxCpuTimeOfOneTx() * Constant.ONE_THOUSAND;
      long thisTxCPULimitInUs = (long) (maxCpuTimeOfOneTx * getCpuLimitInUsRatio());
      long vmStartInUs = System.nanoTime() / Constant.ONE_THOUSAND;
      long vmShouldEndInUs = vmStartInUs + thisTxCPULimitInUs;
      ProgramInvoke programInvoke = programInvokeFactory
          .createProgramInvoke(khtType.kht_CONTRACT_CREATION_TYPE, executorType, kht,
              tokenValue, tokenId, blockCap.getInstance(), deposit, vmStartInUs,
              vmShouldEndInUs, energyLimit);
      this.vm = new VM(config);
      this.program = new Program(ops, programInvoke, rootInternalTransaction, config,
          this.blockCap);
      byte[] txId = new TransactionCapsule(kht).getTransactionId().getBytes();
      this.program.setRootTransactionId(txId);
      if (enableEventLinstener &&
          (EventPluginLoader.getInstance().isContractEventTriggerEnable()
              || EventPluginLoader.getInstance().isContractLogTriggerEnable())
          && isCheckTransaction()) {
        logInfoTriggerParser = new LogInfoTriggerParser(blockCap.getNum(), blockCap.getTimeStamp(),
            txId, callerAddress);

      }
    } catch (Exception e) {
      logger.info(e.getMessage());
      throw new ContractValidateException(e.getMessage());
    }
    program.getResult().setContractAddress(contractAddress);

    deposit.createAccount(contractAddress, newSmartContract.getName(),
        Protocol.AccountType.Contract);

    deposit.createContract(contractAddress, new ContractCapsule(newSmartContract));
    byte[] code = newSmartContract.getBytecode().toByteArray();
    if (!VMConfig.allowTvmConstantinople()) {
      deposit.saveCode(contractAddress, ProgramPrecompile.getCode(code));
    }
    // transfer from callerAddress to contractAddress according to callValue
    if (callValue > 0) {
      transfer(this.deposit, callerAddress, contractAddress, callValue);
    }

    long createSmartContractFee = this.deposit.getDbManager().getDynamicPropertiesStore().getCreateSmartContractFee();


    if (createSmartContractFee > 0) {
      transfer(this.deposit, callerAddress, this.deposit.getBlackHoleAddress() , createSmartContractFee);
    }

    if (VMConfig.allowTvmTransferTrc10()) {
      if (tokenValue > 0) {
        transferToken(this.deposit, callerAddress, contractAddress, String.valueOf(tokenId),
            tokenValue);
      }
    }

  }

  /**
   * **
   */

  private void call()
      throws ContractValidateException {

    if (!deposit.getDbManager().getDynamicPropertiesStore().supportVM()) {
      logger.info("vm work is off, need to be opened by the committee");
      throw new ContractValidateException("VM work is off, need to be opened by the committee");
    }

    Contract.TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(kht);
    if (contract == null) {
      return;
    }

    if (contract.getContractAddress() == null) {
      throw new ContractValidateException("Cannot get contract address from TriggerContract");
    }

    byte[] contractAddress = contract.getContractAddress().toByteArray();

    ContractCapsule deployedContract = this.deposit.getContract(contractAddress);
    if (null == deployedContract) {
      logger.info("No contract or not a smart contract");
      throw new ContractValidateException("No contract or not a smart contract");
    }

    long callValue = contract.getCallValue();
    long tokenValue = 0;
    long tokenId = 0;
    if (VMConfig.allowTvmTransferTrc10()) {
      tokenValue = contract.getCallTokenValue();
      tokenId = contract.getTokenId();
    }

    if (VMConfig.getEnergyLimitHardFork()) {
      if (callValue < 0) {
        throw new ContractValidateException("callValue must >= 0");
      }
      if (tokenValue < 0) {
        throw new ContractValidateException("tokenValue must >= 0");
      }
    }

    byte[] callerAddress = contract.getOwnerAddress().toByteArray();
    checkTokenValueAndId(tokenValue, tokenId);

    byte[] code = this.deposit.getCode(contractAddress);
    if (isNotEmpty(code)) {

      long feeLimit = kht.getRawData().getFeeLimit();
      if (feeLimit < 0 || feeLimit > VMConfig.MAX_FEE_LIMIT) {
        logger.info("invalid feeLimit {}", feeLimit);
        throw new ContractValidateException(
            "feeLimit must be >= 0 and <= " + VMConfig.MAX_FEE_LIMIT);
      }
      AccountCapsule caller = this.deposit.getAccount(callerAddress);
      long energyLimit;
      if (isStaticCall) {
        energyLimit = Constant.ENERGY_LIMIT_IN_CONSTANT_TX;
      } else {
        AccountCapsule creator = this.deposit
            .getAccount(deployedContract.getInstance().getOriginAddress().toByteArray());
        energyLimit = getTotalEnergyLimit(creator, caller, contract, feeLimit, callValue);
      }

      long maxCpuTimeOfOneTx = deposit.getDbManager().getDynamicPropertiesStore()
          .getMaxCpuTimeOfOneTx() * Constant.ONE_THOUSAND;
      long thisTxCPULimitInUs =
          (long) (maxCpuTimeOfOneTx * getCpuLimitInUsRatio());
      long vmStartInUs = System.nanoTime() / Constant.ONE_THOUSAND;
      long vmShouldEndInUs = vmStartInUs + thisTxCPULimitInUs;
      ProgramInvoke programInvoke = programInvokeFactory
          .createProgramInvoke(khtType.kht_CONTRACT_CALL_TYPE, executorType, kht,
              tokenValue, tokenId, blockCap.getInstance(), deposit, vmStartInUs,
              vmShouldEndInUs, energyLimit);
      if (isStaticCall) {
        programInvoke.setStaticCall();
      }
      this.vm = new VM(config);
      rootInternalTransaction = new InternalTransaction(kht, khtType);
      this.program = new Program(code, programInvoke, rootInternalTransaction, config,
          this.blockCap);
      byte[] txId = new TransactionCapsule(kht).getTransactionId().getBytes();
      this.program.setRootTransactionId(txId);

      if (enableEventLinstener &&
          (EventPluginLoader.getInstance().isContractEventTriggerEnable()
              || EventPluginLoader.getInstance().isContractLogTriggerEnable())
          && isCheckTransaction()) {
        logInfoTriggerParser = new LogInfoTriggerParser(blockCap.getNum(), blockCap.getTimeStamp(),
            txId, callerAddress);
      }
    }

    program.getResult().setContractAddress(contractAddress);
    //transfer from callerAddress to targetAddress according to callValue

    if (callValue > 0) {
      transfer(this.deposit, callerAddress, contractAddress, callValue);
    }

    if (VMConfig.allowTvmTransferTrc10()) {
      if (tokenValue > 0) {
        transferToken(this.deposit, callerAddress, contractAddress, String.valueOf(tokenId),
            tokenValue);
      }
    }

  }

  public void go() {
    try {
      if (vm != null) {
        TransactionCapsule khtCap = new TransactionCapsule(kht);
        if (null != blockCap && blockCap.generatedByMyself && null != khtCap.getContractRet()
            && contractResult.OUT_OF_TIME == khtCap.getContractRet()) {
          result = program.getResult();
          program.spendAllEnergy();

          OutOfTimeException e = Program.Exception.alreadyTimeOut();
          runtimeError = e.getMessage();
          result.setException(e);
          throw e;
        }

        vm.play(program);
        result = program.getResult();

        if (isStaticCall) {
          long callValue = TransactionCapsule.getCallValue(kht.getRawData().getContract(0));
          long callTokenValue = TransactionCapsule
              .getCallTokenValue(kht.getRawData().getContract(0));
          if (callValue > 0 || callTokenValue > 0) {
            runtimeError = "constant cannot set call value or call token value.";
            result.rejectInternalTransactions();
          }
          return;
        }

        if (khtType.kht_CONTRACT_CREATION_TYPE == khtType && !result.isRevert()) {
          byte[] code = program.getResult().getHReturn();
          long saveCodeEnergy = (long) getLength(code) * EnergyCost.getInstance().getCREATE_DATA();
          long afterSpend = program.getEnergyLimitLeft().longValue() - saveCodeEnergy;
          if (afterSpend < 0) {
            if (null == result.getException()) {
              result.setException(Program.Exception
                  .notEnoughSpendEnergy("save just created contract code",
                      saveCodeEnergy, program.getEnergyLimitLeft().longValue()));
            }
          } else {
            result.spendEnergy(saveCodeEnergy);
            if (VMConfig.allowTvmConstantinople()) {
              deposit.saveCode(program.getContractAddress().getNoLeadZeroesData(), code);
            }
          }
        }

        if (result.getException() != null || result.isRevert()) {
          result.getDeleteAccounts().clear();
          result.getLogInfoList().clear();
          result.resetFutureRefund();
          result.rejectInternalTransactions();

          if (result.getException() != null) {
            if (!(result.getException() instanceof TransferException)) {
              program.spendAllEnergy();
            }
            runtimeError = result.getException().getMessage();
            throw result.getException();
          } else {
            runtimeError = "REVERT opcode executed";
          }
        } else {
          deposit.commit();

          if (logInfoTriggerParser != null) {
            List<ContractTrigger> triggers = logInfoTriggerParser
                .parseLogInfos(program.getResult().getLogInfoList(), this.deposit);
            program.getResult().setTriggerList(triggers);
          }

        }
      } else {
        deposit.commit();
      }
    } catch (JVMStackOverFlowException e) {
      program.spendAllEnergy();
      result = program.getResult();
      result.setException(e);
      result.rejectInternalTransactions();
      runtimeError = result.getException().getMessage();
      logger.info("JVMStackOverFlowException: {}", result.getException().getMessage());
    } catch (OutOfTimeException e) {
      program.spendAllEnergy();
      result = program.getResult();
      result.setException(e);
      result.rejectInternalTransactions();
      runtimeError = result.getException().getMessage();
      logger.info("timeout: {}", result.getException().getMessage());
    } catch (Throwable e) {
      if (!(e instanceof TransferException)) {
        program.spendAllEnergy();
      }
      result = program.getResult();
      result.rejectInternalTransactions();
      if (Objects.isNull(result.getException())) {
        logger.error(e.getMessage(), e);
        result.setException(new RuntimeException("Unknown Throwable"));
      }
      if (StringUtils.isEmpty(runtimeError)) {
        runtimeError = result.getException().getMessage();
      }
      logger.info("runtime result is :{}", result.getException().getMessage());
    }
    if (!isStaticCall) {
      trace.setBill(result.getEnergyUsed());
    }
  }

  private static long getEnergyFee(long callerEnergyUsage, long callerEnergyFrozen,
      long callerEnergyTotal) {
    if (callerEnergyTotal <= 0) {
      return 0;
    }
    return BigInteger.valueOf(callerEnergyFrozen).multiply(BigInteger.valueOf(callerEnergyUsage))
        .divide(BigInteger.valueOf(callerEnergyTotal)).longValueExact();
  }

  public void finalization() {
    if (StringUtils.isEmpty(runtimeError)) {
      for (DataWord contract : result.getDeleteAccounts()) {
        deposit.deleteContract(convertTokhcAddress((contract.getLast20Bytes())));
      }
    }

    if (config.vmTrace() && program != null) {
      String traceContent = program.getTrace()
          .result(result.getHReturn())
          .error(result.getException())
          .toString();

      if (config.vmTraceCompressed()) {
        traceContent = VMUtils.zipAndEncode(traceContent);
      }

      String txHash = Hex.toHexString(rootInternalTransaction.getHash());
      VMUtils.saveProgramTraceFile(config, txHash, traceContent);
    }

  }

  public void checkTokenValueAndId(long tokenValue, long tokenId) throws ContractValidateException {
    if (VMConfig.allowTvmTransferTrc10()) {
      if (VMConfig.allowMultiSign()) { //allowMultiSigns
        // tokenid can only be 0
        // or (MIN_TOKEN_ID, Long.Max]
        if (tokenId <= VMConstant.MIN_TOKEN_ID && tokenId != 0) {
          throw new ContractValidateException("tokenId must > " + VMConstant.MIN_TOKEN_ID);
        }
        // tokenid can only be 0 when tokenvalue = 0,
        // or (MIN_TOKEN_ID, Long.Max]
        if (tokenValue > 0 && tokenId == 0) {
          throw new ContractValidateException("invalid arguments with tokenValue = " + tokenValue +
              ", tokenId = " + tokenId);
        }
      }
    }
  }

  public ProgramResult getResult() {
    return result;
  }

  public String getRuntimeError() {
    return runtimeError;
  }

}
