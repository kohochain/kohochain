package io.fortest.core.capsule;

public interface ProtoCapsule<T> {

  byte[] getData();

  T getInstance();
}
