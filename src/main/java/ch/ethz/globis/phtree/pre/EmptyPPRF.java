package ch.ethz.globis.phtree.pre;

import ch.ethz.globis.phtree.util.BitTools;

public class EmptyPPRF implements PreProcessorRangeF {

  @Override
  public void pre(double[] raw1, double[] raw2, long[] pre) {
    final int pDIM = raw1.length;
    for (int d=0; d<pDIM; d++) {
      pre[d] = BitTools.toSortableLong(raw1[d]);
      pre[d+pDIM] = BitTools.toSortableLong(raw2[d]);
    }
  }

  @Override
  public void post(long[] pre, double[] post1, double[] post2) {
    final int pDIM = post1.length;
    for (int d=0; d<pDIM; d++) {
      post1[d] = BitTools.toDouble(pre[d]);
      post2[d] = BitTools.toDouble(pre[d+pDIM]);
    }
  }

}
