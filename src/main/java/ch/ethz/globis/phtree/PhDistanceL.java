package ch.ethz.globis.phtree;

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
 * Calculate the euclidean distance for integer values.
 * 
 * @see PhDistance
 * 
 * @author ztilmann
 */
public class PhDistanceL implements PhDistance {

  public static final PhDistanceL THIS = new PhDistanceL();
  
  /**
   * Calculate the distance for integer values.
   * 
   * @see PhDistance#dist(long[], long[])
   */
  @Override
  public double dist(long[] v1, long[] v2) {
    double d = 0;
    //How do we best handle this?
    //Substraction can easily overflow, especially with floating point values that have been 
    //converted to 'long'.
    //1) cast to (double). This will lose some precision for large values, but gives a good
    //   'estimate' and is otherwise quite fault tolerant
    //2) Use Math.addExact(). This will fall early, and will often not work for converted
    //   'double' values. However, we can thus enforce using PhDistanceF instead. This
    //   would be absolutely precise and unlikely to overflow.
    //The dl*dl can be done as 'double', which is always safe.
    for (int i = 0; i < v1.length; i++) {
      //TODO replace with Math.exact in Java 8
      //long dl = Math.subtractExact(v1[i], v2[i]);
      //d += Math.multiplyExact(dl, dl);
      double dl = (double)v1[i] - (double)v2[i];
      d += dl*dl;
    }
    return Math.sqrt(d);
  }
  
  @Override
  public void toMBB(double distance, long[] center, long[] outMin,
      long[] outMax) {
    for (int i = 0; i < center.length; i++) {
			//casting to 'long' always rounds down (floor)
      outMin[i] = (long) (center[i] - distance);
			//casting to 'long' after adding 1.0 always rounds up (ceiling)
			outMax[i] = (long) (center[i] + distance + 1);
    }
  }
}