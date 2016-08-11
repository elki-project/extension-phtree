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
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.IdentityHashMap;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhTree;

public class PersProviderOStr extends PersistenceProvider {
	
	private int idCnt = 0;
	private static final boolean LOG = true;
	
	private int dims = -1;
	private int nEntries = -1;
	private Object rootId = null;
	
	private IdentityHashMap<Externalizable, Integer> idMap = new IdentityHashMap<>();
	
	//pageId -> page
	//private IdentityHashMap<Integer, byte[]> database = new IdentityHashMap<>();
	private IdentityHashMap<Integer, Externalizable> database = new IdentityHashMap<>();
	
	private int nPageRead;
	private int nPageWrite;
	private ObjectOutputStream out;
	private ObjectInputStream in;
	
//	public PersProviderOStr(ObjectInputStream in, ObjectOutputStream out) {
//		// TODO Auto-generated constructor stub
//	}
	
	public PersProviderOStr() {
//		ByteArrayOutputStream baos = new ByteArrayOutputStream();
//		ObjectOutputStream out = new ObjectOutputStream(baos);
//		//out.writeObject(tree);
//		PhTree12.writeExternal(out, (PhTree12) tree);
//		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
//
//		System.out.println("Bytes written: " + baos.size() +  "   nodes="+tree.getStats().nNodes);
//		
//		ObjectInputStream in = new ObjectInputStream(bais);
	}
	
	@Override
	public Object loadNode(Object o) {
		nPageRead++;
//		byte[] objMap.get((NodeID)o);
//		ObjectIn
		//return ((NodeID)o).resolve();
//		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
//		bais.
		Object ret = database.get((Integer)o);
		log("get id=", o, "o=", ret);
		return ret;
	}

	@Override
	public Object registerNode(Externalizable o) {
		nPageWrite++;
		Object id = idMap.get(o);
		if (id != null) {
			throw new IllegalArgumentException();
		}
		id = ++idCnt;
		idMap.put(o, (Integer)id);
		database.put((Integer)id, o);
		log("alloc id=", id, "o=", o);
		return id;
	}
	
	@Override
	public void updateNode(Externalizable o) {
		nPageWrite++;
		Object id = idMap.get(o);
		if (id != null) {
			throw new IllegalArgumentException();
		}
		database.put((Integer)id, o);
		log("update id=", id, "o=", o);
	}

	@Override
	public String getDescription() {
		return "OBJ-STR";
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

	@Override
	public String toString() {
		return "nPageRead=" + nPageRead + 
				"  nPageWrite=" + nPageWrite;
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
	
	private void log(Object ...strings) {
		if (LOG) {
			StringBuilder sb = new StringBuilder();
			for (Object s: strings) {
				sb.append(s);
				sb.append(" ");
			}
			System.out.println(sb.toString());
		}
	}

	@Override
	public void writeTree(PhTree<?> tree, int dims) {
		this.dims = dims;
		this.nEntries = 0;
	}

	@Override
	public void updateTree(PhTree<?> tree, int dims, int nEntries, Object rootId) {
		this.dims = dims;
		this.nEntries = nEntries;
		this.rootId = rootId;
	}

	@Override
	public <T> PhTree<T> loadTree() {
		return new PhTree12<>(dims, nEntries, rootId, this);
	}

	@Override
	public void flush() {
		// 
	}
}
