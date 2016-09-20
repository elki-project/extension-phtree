package ch.ethz.globis.phtree.v11;

import ch.ethz.globis.phtree.PhTreeHelper;

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
 * Reference pooling and management for Node instances.
 * 
 * @author ztilmann
 */
public class NodePool {

  private static final Node[] POOL = 
      new Node[PhTreeHelper.MAX_OBJECT_POOL_SIZE];
  private static int poolSize;
  /** Nodes currently used outside the pool. */
  private static int activeNodes = 0;

  private NodePool() {
    // empty
  }

  static synchronized Node getNode() {
    activeNodes++;
    if (poolSize == 0) {
      return Node.createEmpty();
    }
    return POOL[--poolSize];
  }

  static synchronized void offer(Node node) {
    activeNodes--;
    if (poolSize < POOL.length) {
      POOL[poolSize++] = node;
    }
  }

  public static int getActiveNodes() {
    return activeNodes;
  }
}
