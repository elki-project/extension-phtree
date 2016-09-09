package ch.ethz.globis.phtree.util;

/*
This file is part of ELKI:
Environment for Developing KDD-Applications Supported by Index-Structures

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.Arrays;

/**
 * Quality stats related to data characteristics and tree quality.
 */
public final class PhTreeStats {
  private final int bitWidth;
  public int nNodes;
  public int nAHC; //AHC nodes (formerly Nodes with AHC-postfix representation)
  public int nNtNodes; //NtNodes (formerly Nodes with sub-HC representation)
  public int nNT; //nodes with NT representation
  public int nTotalChildren;
  public long size;  //calculated size in bytes
  public int q_totalDepth;
  public int[] q_nPostFixN;  //filled with  x[currentDepth] = nPost;
  public int[] infixHist = new int[64];  //prefix len
  public int[] nodeDepthHist = new int[64];  //prefix len
  public int[] nodeSizeLogHist = new int[32];  //log (nEntries)

  public PhTreeStats() {
    this(64);
  }

  public PhTreeStats(int bitWidth) {
    this.bitWidth = bitWidth;
    this.q_nPostFixN = new int[bitWidth];
  }

  @Override
  public String toString() {
    StringBuilderLn r = new StringBuilderLn();
    r.appendLn("  nNodes = " + nNodes);
    r.appendLn("  avgNodeDepth = " + (double)q_totalDepth/(double)nNodes); 
    //            "  noPostChildren=" + q_nPostFix1 + "\n" +
    r.appendLn("  AHC=" + nAHC + "  NI=" + nNT + "  nNtNodes=" + nNtNodes);
    double apl = getAvgPostlen(r);
    r.appendLn("  avgPostLen = " + apl + " (" + (bitWidth-apl) + ")");

    return r.toString();
  }

  public String toStringHist() {
    StringBuilderLn r = new StringBuilderLn();
    r.appendLn("  infixLen      = " + Arrays.toString(infixHist));
    r.appendLn("  nodeSizeLog   = " + Arrays.toString(nodeSizeLogHist));
    r.appendLn("  nodeDepthHist = " + Arrays.toString(nodeDepthHist));
    r.appendLn("  depthHist     = " + Arrays.toString(q_nPostFixN));
    return r.toString();
  }

  /**
   * 
   * @param r String builder
   * @return average postLen, including the HC/LHC bit.
   */
  public double getAvgPostlen(StringBuilderLn r) {
    long total = 0;
    int nEntry = 0;
    for (int i = 0; i < bitWidth; i++) {
      if (r!=null) {
        //r.appendLn("  depth= " + i + "  n= " + q_nPostFixN[i]);
      }
      total += (bitWidth-i)*(long)q_nPostFixN[i];
      nEntry += q_nPostFixN[i];
    }
    return total/(double)nEntry;
  }

  public int getNodeCount() {
    return nNodes;
  }

  public int getAhcCount() {
    return nAHC;
  }

  public int getNtInternalNodeCount() {
    return nNtNodes;
  }

  public int getNtCount() {
    return nNT;
  }

  public long getCalculatedMemSize() {
    return size;
  }

  public int getBitDepth() {
    return bitWidth;
  }

}