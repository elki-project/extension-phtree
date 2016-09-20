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

/**
 * Type of mapper that does not use the value of the PHEntry, only the key.
 */
public abstract class PhMapperK<T, R> implements PhMapper<T, R> {

//    static <T> PhMapperK<T, long[]> LONG_ARRAY() {
//        return e -> (e.getKey());
//    }
//
//    static <T> PhMapperK<T, double[]> DOUBLE_ARRAY() {
//        return e -> (toDouble(e.getKey()));
//    }

  private static final long serialVersionUID = 1L;

  public static double[] toDouble(long[] point) {
    double[] d = new double[point.length];
    for (int i = 0; i < d.length; i++) {
      d[i] = BitTools.toDouble(point[i]);
    }
    return d;
  }

}