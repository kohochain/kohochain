package io.fortest.core.db2.core;

public interface ISession extends AutoCloseable {

  void commit();

  void revoke();

  void merge();

  void destroy();

  void close();

}
