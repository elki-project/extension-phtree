package ch.ethz.globis.phtree.pre;

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

/**
 * Preprocessor for range data (rectangles). 
 *
 */
public interface PreProcessorRange {

  /**
   * 
   * @param raw1 raw data (input) of lower left corner
   * @param raw2 raw data (input) of upper right corner
   * @param pre pre-processed data (output, must be non-null and same size as input array)
   */
  public void pre(long[] raw1, long[] raw2, long[] pre);


  /**
   * @param pre pre-processed data (input)
   * @param post1 post-processed data (output, must be non-null and same size as input array)
   *              of lower left corner
   * @param post2 post-processed data (output, must be non-null and same size as input array)
   *              of upper right corner
   */
  public void post(long[] pre, long[] post1, long[] post2);


  /**
   * This preprocessors turns a k-dimensional rectangle into a point with 2k dimensions.
   */
  public class Simple implements PreProcessorRange {

    public Simple() {
      //
    }

    @Override
    public void pre(long[] raw1, long[] raw2, long[] pre) {
      final int pDIM = raw1.length;
      for (int d = 0; d < pDIM; d++) {
        pre[d] = raw1[d];
        pre[d+pDIM] = raw2[d];
      }
    }

    @Override
    public void post(long[] pre, long[] post1, long[] post2) {
      final int pDIM = post1.length;
      for (int d = 0; d < pDIM; d++) {
        post1[d] = pre[d];
        post2[d] = pre[d+pDIM];
      }
    }
  }

}
