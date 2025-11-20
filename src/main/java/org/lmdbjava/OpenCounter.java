package org.lmdbjava;

import java.util.concurrent.atomic.AtomicInteger;

public class OpenCounter {

  private static final int CLOSED_VALUE = 0;
  private static final int ONLY_ENV_OPEN = 1;

  private final AtomicInteger counter = new AtomicInteger(ONLY_ENV_OPEN);

  private OpenCounter() {}

  static OpenCounter open() {
    return new OpenCounter();
  }

  /**
   * @throws org.lmdbjava.Env.OpenItemsException When there are open items such as transactions,
   *     cursors, etc.
   * @return True if closeAction was called.
   */
  boolean close(final Runnable closeAction) {
    final int currCount =
        counter.getAndUpdate(
            count -> {
              if (count == CLOSED_VALUE || count == ONLY_ENV_OPEN) {
                // Already closed or we are about to close
                return CLOSED_VALUE;
              } else if (count > ONLY_ENV_OPEN) {
                throw new Env.OpenItemsException(count - ONLY_ENV_OPEN);
              } else {
                // Suggests a mismatch between increment and decrement calls
                throw new IllegalStateException("Unexpected count " + count);
              }
            });

    if (currCount > 0) {
      // Our thread made the count change, so proceed with the closeAction
      closeAction.run();
      return true;
    } else {
      // Already closed, no-op
      return false;
    }
  }

  boolean isClosed() {
    return counter.get() == CLOSED_VALUE;
  }

  void checkNotClosed() {
    if (counter.get() == CLOSED_VALUE) {
      throw new Env.AlreadyClosedException();
    }
  }

  void increment() {
    final int newCount =
        counter.updateAndGet(
            count -> {
              if (count >= ONLY_ENV_OPEN) {
                return count + 1;
              } else if (count == 0) {
                throw new Env.AlreadyClosedException();
              } else {
                // Suggests a mismatch between increment and decrement calls
                throw new IllegalStateException("Unexpected count " + count);
              }
            });
    System.out.println(Thread.currentThread().getName() + " - increment " + (newCount - 1) + " => " + newCount);
  }

  void decrement() {
    final int newCount =
        counter.updateAndGet(
            count -> {
              if (count > ONLY_ENV_OPEN) {
                return count - 1;
              } else if (count == 0) {
                throw new Env.AlreadyClosedException();
              } else {
                // Suggests a mismatch between increment and decrement calls
                throw new IllegalStateException("Unexpected count " + count);
              }
            });
    System.out.println(Thread.currentThread().getName() + " - decrement " + (newCount + 1) + " => " + newCount);
  }
}
