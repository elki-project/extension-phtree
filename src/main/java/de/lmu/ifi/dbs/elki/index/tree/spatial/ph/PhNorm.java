package de.lmu.ifi.dbs.elki.index.tree.spatial.ph;

import ch.ethz.globis.phtree.PhDistance;
import ch.ethz.globis.phtree.pre.PreProcessorPointF;
import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.distance.distancefunction.Norm;

/**
 * PhDistance implementation that wraps around an ELKI distance.
 * 
 * @author Tilmann Zaeschke
 *
 * @param <O>
 */
final class PhNorm<O extends NumberVector> implements PhDistance {

  private final Norm<NumberVector> norm;
  private final PreProcessorPointF pre;
  private final PhNumberVectorAdapter o1;
  private final PhNumberVectorAdapter o2;
  
  private long distanceCalcCount = 0;
  
  PhNorm(Norm<O> norm, int dimensions, PreProcessorPointF pre) {
    this.norm = (Norm<NumberVector>) norm;
    this.pre = pre;
    this.o1 = new PhNumberVectorAdapter(dimensions, pre);
    this.o2 = new PhNumberVectorAdapter(dimensions, pre);
  }
  
  @Override
  public double dist(long[] v1, long[] v2) {
    distanceCalcCount++;
    return norm.distance((NumberVector)o1.wrap(v1), (NumberVector)o2.wrap(v2));
  }
  
  public long getAndResetDistanceCounter() {
    long x = distanceCalcCount;
    distanceCalcCount = 0;
    return x;
  }
  

  @Override
  public void toMBB(double distance, long[] center, long[] outMin,
      long[] outMax) {
    double[] c = new double[center.length];
    double[] min = new double[outMin.length];
    double[] max = new double[outMax.length];
    pre.post(center, c);
    //TODO this only really works for eucledean distance...
    for (int i = 0; i < center.length; i++) {
      min[i] = c[i] - distance;
      max[i] = c[i] + distance;
    }
    pre.pre(min, outMin);
    pre.pre(max, outMax);
  }

}