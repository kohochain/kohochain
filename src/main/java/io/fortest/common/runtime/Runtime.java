package io.fortest.common.runtime;

import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;
import io.fortest.core.exception.VMIllegalException;
import io.fortest.common.runtime.vm.program.InternalTransaction.khtType;
import io.fortest.common.runtime.vm.program.ProgramResult;


public interface Runtime {

  void execute() throws ContractValidateException, ContractExeException, VMIllegalException;

  void go();

  khtType getkhtType();

  void finalization();

  ProgramResult getResult();

  String getRuntimeError();

  void setEnableEventLinstener(boolean enableEventLinstener);
}
