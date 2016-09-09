package ch.ethz.globis.phtree;

import ch.ethz.globis.phtree.PhTree.PhIterator;
import ch.ethz.globis.phtree.PhTree.PhQuery;

/**
 * Range query.
 * 
 * @author Tilmann Zaeschke
 *
 * @param <T> The type parameter
 */
public class PhRangeQuery<T> implements PhIterator<T> {

  private final long[] min;
  private final long[] max;
  private final PhQuery<T> q;
  private final int dims;
  private final PhDistance dist;
  private final PhFilterDistance filter;

  public PhRangeQuery(PhQuery<T> iter, PhTree<T> tree, 
      PhDistance dist, PhFilterDistance filter) {
    this.dims = tree.getDim();
    this.q = iter;
    this.dist = dist;
    this.filter = filter;
    this.min = new long[dims];
    this.max = new long[dims];
  }

  public PhRangeQuery<T> reset(double range, long... center) {
    filter.set(center, dist, range);
    dist.toMBB(range, center, min, max);
    q.reset(min, max);
    return this;
  }

  @Override
  public long[] nextKey() {
    return q.nextKey();
  }

  @Override
  public T nextValue() {
    return q.nextValue();
  }

  @Override
  public PhEntry<T> nextEntry() {
    return q.nextEntry();
  }

  @Override
  public boolean hasNext() {
    return q.hasNext();
  }

  @Override
  public T next() {
    return q.next();
  }

  @Override
  public void remove() {
    q.remove();
  }

  @Override
  public PhEntry<T> nextEntryReuse() {
    return q.nextEntryReuse();
  }

}
