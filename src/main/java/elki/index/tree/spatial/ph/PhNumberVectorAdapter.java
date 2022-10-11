package elki.index.tree.spatial.ph;

import ch.ethz.globis.phtree.pre.PreProcessorPointF;
import elki.data.NumberVector;

final class PhNumberVectorAdapter implements NumberVector {
  
  private final PreProcessorPointF pre;
  private final double[] min;
  //private double[] max;
  
  public PhNumberVectorAdapter(int dimension, PreProcessorPointF pre) {
    this.pre = pre;
    min = new double[dimension];
  }
  
  @Override
  public double getMin(int dimension) {
    return min[dimension];
  }

  @Override
  public double getMax(int dimension) {
    //return max[dimension];
    throw new UnsupportedOperationException();
  }

  @Override
  public int getDimensionality() {
    return min.length;
  }

  @Override
  public short shortValue(int dimension) {
    return (short) getMin(dimension);
  }

  @Override
  public long longValue(int dimension) {
    return (long) getMin(dimension);
  }

  @Override
  public int intValue(int dimension) {
    return (int) getMin(dimension);
  }

  @Override
  public Number getValue(int dimension) {
    return getMin(dimension);
  }

  @Override
  public double[] toArray() {
    return min.clone();
  }

  @Override
  public float floatValue(int dimension) {
    return (float) getMin(dimension);
  }

  @Override
  public double doubleValue(int dimension) {
    return getMin(dimension);
  }

  @Override
  public byte byteValue(int dimension) {
    return (byte) getMin(dimension);
  }

  public NumberVector wrap(long[] v) {
    pre.post(v, min);
    return this;
  }
}