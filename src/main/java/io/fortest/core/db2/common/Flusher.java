package io.fortest.core.db2.common;

import java.util.Map;

import io.fortest.core.db.common.WrappedByteArray;

public interface Flusher {

  void flush(Map<WrappedByteArray, WrappedByteArray> batch);

  void close();

  void reset();
}
