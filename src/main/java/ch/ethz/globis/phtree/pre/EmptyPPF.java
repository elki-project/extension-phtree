package ch.ethz.globis.phtree.pre;

import ch.ethz.globis.phtree.util.BitTools;

public class EmptyPPF implements PreProcessorPointF {

  @Override
  public void pre(double[] raw, long[] pre) {
    for (int d=0; d<raw.length; d++) {
      pre[d] = BitTools.toSortableLong(raw[d]);
    }
  }

  @Override
  public void post(long[] pre, double[] post) {
    for (int d=0; d<pre.length; d++) {
      post[d] = BitTools.toDouble(pre[d]);
    }
  }

}
