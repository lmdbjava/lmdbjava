package org.lmdbjava;


public class NoOpRefCounter implements RefCounter {

  @Override
  public RefCounterReleaser acquire() {
    return RefCounterReleaser.NO_OP_RELEASER;
  }

  @Override
  public void use(Runnable runnable) {
    runnable.run();
  }

  @Override
  public void close() {
    // no-op
  }

//  @Override
//  public void close(long duration, TimeUnit timeUnit) {
//    // no-op
//  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public EnvState getState() {
    return null;
  }

  @Override
  public void checkNotClosed() {
    // no-op
  }
}
