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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Interface for persistence providers. Persistence providers can be used by
 * PhTrees (such as v12) to serialize the tree or to measure I/O access.
 */
public abstract class PersistenceProvider {
	
	/**
	 * The empty implementation of a persistence provide, it does not provide persistence.
	 */
	public static final PersistenceProvider NONE = new PersistenceProviderNone();
	
	/**
	 * The empty implementation of a persistence provide, it does not provide persistence.
	 */
	public static class PersistenceProviderNone extends PersistenceProvider {
		private PhTree<?> tree;
		@Override
		public Object registerNode(Externalizable o) {
			return o;
		}
		
		@Override
		public Object loadNode(Object o) {
			return o;
		}

		@Override
		public void updateNode(Externalizable o) {
			//
		}

		@Override
		public String getDescription() {
			return "NONE";
		}

		@Override
		public int statsGetPageReads() {
			return -1;
		}

		@Override
		public int statsGetPageWrites() {
			return -1;
		}

		@Override
		public void statsReset() {
			//
		}
	
		@Override
		public void writeTree(PhTree<?> tree, int dims) {
			this.tree = tree;
		}

		@Override
		public void updateTree(PhTree<?> tree, int dims, int nEntries, Object rootId) {
			this.tree = tree;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> PhTree<T> loadTree() {
			return (PhTree<T>) tree;
		}

		@Override
		public void flush() {
			// nothing 
		}
	}
	
	public abstract Object loadNode(Object o);
	
	/**
	 * Register a new node.
	 * @param o the new node
	 * @return A node identifier
	 */
	public abstract Object registerNode(Externalizable o);
	public abstract void updateNode(Externalizable o);
	
	public abstract String getDescription();
	public abstract int statsGetPageReads();
	public abstract int statsGetPageWrites();
	public abstract void statsReset();

	public abstract void writeTree(PhTree<?> tree, int dims);

	public abstract void updateTree(PhTree<?> tree, int dims, int nEntries, Object rootId);

	public abstract <T> PhTree<T> loadTree();
	
	public abstract void flush();

	public static void write(Object[] values, ObjectOutput out) throws IOException {
		out.writeShort(values.length);
		for (int i = 0; i < values.length; i++) {
			Object v = values[i];
			int vi = v != null ? (int)v : -1;
			out.writeInt(vi);
		}
	}

	public static Object[] read(ObjectInput in) throws IOException {
		int size = in.readShort();
		Object[] ret = new Object[size];
		for (int i = 0; i < size; i++) {
			int vi = in.readInt();
			ret[i] = vi == -1 ? null : Integer.valueOf(vi);
		}
		return ret;
	}
}