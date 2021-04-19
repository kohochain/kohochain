package io.fortest.core.exception;

public class VMMemoryOverflowException extends ForTestException {

  public VMMemoryOverflowException() {
    super("VM memory overflow");
  }

  public VMMemoryOverflowException(String message) {
    super(message);
  }

}
