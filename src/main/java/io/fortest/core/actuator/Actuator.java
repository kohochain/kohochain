package io.fortest.core.actuator;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fortest.core.capsule.TransactionResultCapsule;
import io.fortest.core.exception.ContractExeException;
import io.fortest.core.exception.ContractValidateException;

public interface Actuator {

  boolean execute(TransactionResultCapsule result) throws ContractExeException;

  boolean validate() throws ContractValidateException;

  ByteString getOwnerAddress() throws InvalidProtocolBufferException;

  long calcFee();

}
