package ch.ethz.globis.pht64kd;

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
import java.util.Iterator;

/**
 * NodeTrees are a way to represent Nodes that are to big to be represented as AHC or LHC nodes.
 * 
 * A NodeTree splits a k-dimensional node into a hierarchy of smaller nodes by splitting,
 * for example, the 16-dim key into 2 8-dim keys.
 * 
 * Unlike the normal PH-Tree, NodeTrees do not support infixes.
 * 
 * @author ztilmann
 *
 */
public interface MaxKTreeI {

	public static interface PhIterator64<T> extends Iterator<T> {

		public long nextKey();

		public long[] nextKdKey();

		public T nextValue();

		public NtEntry<T> nextEntry();

		/**
		 * Special 'next' method that avoids creating new objects internally by reusing Entry objects.
		 * Advantage: Should completely avoid any GC effort.
		 * Disadvantage: Returned PhEntries are not stable and are only valid until the
		 * next call to next(). After that they may change state. Modifying returned entries may
		 * invalidate the backing tree.
		 * @return The next entry
		 */
		NtEntry<T> nextEntryReuse();

		public void reset(MaxKTreeI tree, long min, long max);
		public void reset(MaxKTreeI tree);
	}

	public static class NtEntry<T> {
		private long key;
		private long[] kdKey;
		private T value;
		public NtEntry(long key, long[] kdKey, T value) {
			this.key = key;
			this.kdKey = kdKey;
			this.value = value;
		}
		
		public NtEntry(NtEntry<T> e) {
			this.key = e.getKey();
			this.kdKey = Arrays.copyOf(e.getKdKey(), e.getKdKey().length);
			this.value = e.getValue();
		}
		
		public long getKey() {
			return key;
		}
		
		public long[] getKdKey() {
			return kdKey;
		}
		
		public T getValue() {
			return value;
		}

		protected void set(long key, long[] kdKey, T value) {
			this.key = key;
			this.kdKey = kdKey;
			this.value = value;
		}
		
		public void setValue(T value) {
			this.value = value;
		}
		
		@Override
		public String toString() {
			return Long.toString(key);
		}
		
		public long key() {
			return getKey();
		}
		public T value() {
			return getValue();
		}

		public void setKey(long key) {
			this.key = key;
		}
	}
	
	

	public int size();

	public int getKeyBitWidth();
	
	public Object getRoot();
	
}
