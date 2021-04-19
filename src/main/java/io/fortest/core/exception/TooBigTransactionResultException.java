package io.fortest.core.exception;

public class TooBigTransactionResultException extends ForTestException {

  public TooBigTransactionResultException() {
    super("too big transaction result");
  }

  public TooBigTransactionResultException(String message) {
    super(message);
  }
}
