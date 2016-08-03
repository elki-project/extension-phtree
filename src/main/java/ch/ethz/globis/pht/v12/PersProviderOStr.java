package ch.ethz.globis.pht.v12;

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
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import ch.ethz.globis.pht.PersistenceProvider;

public class PersProviderOStr implements PersistenceProvider {
	
	private int nPageRead;
	private int nPageWrite;
	
	@Override
	public Object resolveObject(Object o) {
		nPageRead++;
		return ((NodeID)o).resolve();
	}

	@Override
	public Object storeObject(Externalizable o) {
		nPageWrite++;
		return new NodeID(o);
	}

	@Override
	public String getDescription() {
		return "SIMPLE";
	}

	@Override
	public int statsGetPageReads() {
		return nPageRead;
	}

	@Override
	public int statsGetPageWrites() {
		return nPageWrite;
	}

	@Override
	public void statsReset() {
		nPageRead = 0;
		nPageWrite = 0;
	}

	private static class NodeID implements Externalizable {
		private Externalizable node;
		private long id;
		private static long idCount = 0;
		public NodeID() {
			//for externalizable only
		}
		public NodeID(Externalizable node) {
			this.node = node;
			this.id = ++idCount;
		}
		Object resolve() {
			return node;
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			//TODO assign id
			out.writeLong(id);
			node.writeExternal(out);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			//TODO lookup by id
			long readID = in.readLong();
//			if (readID != id) {
//				throw new IllegalArgumentException();
//			}
			node = Node.createEmpty();
			node.readExternal(in);
		}
	}
	
	private static class StatOutputStream extends OutputStream {

		private final OutputStream out;
		private int nBytes = 0;
		
		protected StatOutputStream(OutputStream out) {
			this.out = out;
		}

		@Override
		public void write(int b) throws IOException {
			nBytes++;
			out.write(b);
		}
		
	}
}
