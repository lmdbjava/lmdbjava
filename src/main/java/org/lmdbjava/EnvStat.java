package org.lmdbjava;

/**
 * Environment statistics.
 */
public class EnvStat {

  public final long branchPages;
  public final int depth;
  public final long entries;
  public final long leafPages;
  public final long overflowPages;
  public final int pageSize;

  EnvStat(int pageSize, int depth, long branchPages, long leafPages,
          long overflowPages, long entries) {
    this.pageSize = pageSize;
    this.depth = depth;
    this.branchPages = branchPages;
    this.leafPages = leafPages;
    this.overflowPages = overflowPages;
    this.entries = entries;
  }

}
