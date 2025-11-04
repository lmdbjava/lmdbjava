package org.lmdbjava;

import java.util.function.BiConsumer;

public interface EntryConsumer<T> extends BiConsumer<T, T> {

  @Override
  void accept(T key, T val);
}
