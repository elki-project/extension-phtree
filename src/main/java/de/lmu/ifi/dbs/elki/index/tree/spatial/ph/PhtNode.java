package de.lmu.ifi.dbs.elki.index.tree.spatial.ph;
/*
 This file is part of ELKI:
 Environment for Developing KDD-Applications Supported by Index-Structures

 Copyright (C) 2016
 Ludwig-Maximilians-Universität München
 Lehr- und Forschungseinheit für Datenbanksysteme
 ELKI Development Team

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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import de.lmu.ifi.dbs.elki.index.tree.AbstractNode;
import de.lmu.ifi.dbs.elki.index.tree.IndexTreePath;
import de.lmu.ifi.dbs.elki.index.tree.Node;
import de.lmu.ifi.dbs.elki.persistent.AbstractExternalizablePage;
import de.lmu.ifi.dbs.elki.persistent.Page;

public class PhtNode extends AbstractExternalizablePage implements Page {

  private ch.ethz.globis.phtree.v12.Node node;
  /**
   * The number of entries in this node.
   */
  private int numEntries;
 
//  @Override
//  public Enumeration<IndexTreePath<PhtEntry>> children(final IndexTreePath<PhtEntry> parentPath) {
//    return new Enumeration<IndexTreePath<PhtEntry>>() {
//      int count = 0;
//
//      @Override
//      public boolean hasMoreElements() {
//        return count < numEntries;
//      }
//
//      @Override
//      public IndexTreePath<PhtEntry> nextElement() {
//        synchronized(PhtNode.this) {
//          if(count < numEntries) {
//            return new IndexTreePath<>(parentPath, entries[count], count++);
//          }
//        }
//        throw new NoSuchElementException();
//      }
//    };
//  }

//  @Override
//  public int getNumEntries() {
//    return node.getEntryCount();
//  }
//
//  @Override
//  public boolean isLeaf() {
//    return false;
//  }
//
//  @Override
//  public PhtEntry getEntry(int index) {
//    // TODO Auto-generated method stub
//    throw new UnsupportedOperationException();
//    //return null;
//  }
//
//  @Override
//  public int addLeafEntry(PhtEntry entry) {
//    // TODO Auto-generated method stub
//    throw new UnsupportedOperationException();
////   return 0;
//  }
//
//  @Override
//  public int addDirectoryEntry(PhtEntry entry) {
//    // TODO Auto-generated method stub
//    throw new UnsupportedOperationException();
////    return 0;
//  }
  
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    node = ch.ethz.globis.phtree.v12.Node.createEmpty();
    node.readExternal(in);
  }
  
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    node.writeExternal(out);
  }
  
}
