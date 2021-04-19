package io.fortest.core.exception;

public class khcRuntimeException extends RuntimeException {

  public khcRuntimeException() {
    super();
  }

  public khcRuntimeException(String message) {
    super(message);
  }

  public khcRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public khcRuntimeException(Throwable cause) {
    super(cause);
  }

  protected khcRuntimeException(String message, Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }


}
