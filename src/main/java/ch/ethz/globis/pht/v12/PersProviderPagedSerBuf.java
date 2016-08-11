/*
 * Copyright 2011-2016 ETH Zurich. All Rights Reserved.
 *
 * This software is the proprietary information of ETH Zurich.
 * Use is subject to license terms.
 */
package ch.ethz.globis.pht.v12;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.v12.nt.NtNode;
import ch.ethz.globis.pht.v12.nt.NtNodePool;

/**
 * Persistence provider that stores nodes in a map of byte[].
 */
public class PersProviderPagedSerBuf extends PersistenceProvider {
	
	private int dims;
	private int nEntries = -1;
	private Object rootId = -1;
	private int pageIdCnt = 0;
	private int bucketIdCount = 0;
	
	private static final byte NODE_NULL = -1;
	private static final byte NODE_PH = 2;
	private static final byte NODE_NT = 3;
	
	private static final boolean LOG = false;
	
	public static final int PAGE_SIZE = 4096;
	
	private IdentityHashMap<Externalizable, Integer> idMap = new IdentityHashMap<>();
	
	//pageId -> page
	private HashMap<Integer, byte[]> database = new HashMap<>();
	
	//this is a buffer to avoid writing data before it is required.
	private HashMap<Integer, Externalizable> bufferById = new HashMap<>();
	private IdentityHashMap<Externalizable, Integer> bufferByObj = new IdentityHashMap<>();
	
	private HashMap<Integer, Bucket> bucketByObjId = new HashMap<>();
	private HashMap<Integer, Bucket> bucketByBucketId = new HashMap<>();
	private int prevBucket;
	private Integer prevNodeId = null;

	private int nNodeRead;
	private int nNodeNew;
	private int nNodeUpdate;
	private long nBytesRead;
	private long nBytesWritten;
	private long maxNodeSize = 0;
	
	public PersProviderPagedSerBuf() {
		// 
	}
	
	@Override
	public Object loadNode(Object o) {
		if (o == null) {
			return null;
		}
		nNodeRead++;
		Integer id = (Integer) o;
		Object ret = bufferById.get(id);
		if (ret != null) {
			log("get-b id=", id, "o=", ret);
			prevNodeId = id;
			return ret;
		}
		
		byte[] buf = database.get(id);
		if (buf == null) {
//			for (Map.Entry<Integer, byte[]> e: database.entrySet()) {
//				System.out.println("e: " + e.getKey() + " -> " + e.getValue());
//			}
			throw new IllegalArgumentException("id=" + id);
		}
		nBytesRead += buf.length;
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		Externalizable node;
		try {
			ObjectInputStream in = new ObjectInputStream(bais);
			byte nodeType = in.readByte();
			switch (nodeType) {
			case NODE_PH: 
				node = NodePool.getNode();
				node.readExternal(in);
				break;
			case NODE_NT:
				node = NtNodePool.getNode();
				node.readExternal(in);
				break;
			case NODE_NULL: node = null;
			default:
				throw new IllegalStateException("nt=" + nodeType);
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		log("get id=", id, "o=", node);
		idMap.put(node, id);
		//TODO this creates a (small) memory leak, because Nodes are never removed from the list
		//     however, they are reused, due to the node-pool
		prevNodeId = id;
		return node;
	}

	@Override
	public Object registerNode(Externalizable o) {
		nNodeNew++;
		Integer id = idMap.get(o);
		if (id != null) {
			throw new IllegalArgumentException();
		}
		id = ++pageIdCnt;
		idMap.put(o, (Integer)id);
		
		Object prev = bufferByObj.put(o, id);
		if (prev != null) {
			throw new IllegalArgumentException();
		}
		bufferById.put(id, o);
		log("alloc id=", id, "o=", o);
		
		assignBucket(id, o);
		
		return id;
	}
	
	@Override
	public void updateNode(Externalizable o) {
		nNodeUpdate++;
		Integer id = idMap.get(o);
		if (id == null) {
			id = bufferByObj.get(o);
			if (id == null) {
				throw new IllegalArgumentException();
			}
		}
		
		bufferByObj.put(o, id);
		bufferById.put(id, o);

		log("update id=", id, "o=", o);
		
		reassignBucket(id, o);
	}

	private void writeNode(Externalizable o, Object id) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(PAGE_SIZE);
		try {
			ObjectOutputStream out = new ObjectOutputStream(baos);
			if (o instanceof Node) {
				out.writeByte(NODE_PH);
			} else if (o instanceof NtNode) {
				out.writeByte(NODE_NT);
			} else {
				out.writeByte(NODE_NULL);
			}
			o.writeExternal(out);
			out.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		byte[] buf = baos.toByteArray();
		database.put((Integer)id, buf);
		log("write id=", id, "o=", o);
		nBytesWritten += buf.length;
		maxNodeSize = buf.length > maxNodeSize ? buf.length : maxNodeSize;
	}
	
	@Override
	public String getDescription() {
		return "OBJ-STR";
	}

	@Override
	public int statsGetPageReads() {
		return nNodeRead;
	}

	@Override
	public int statsGetPageWrites() {
		return nNodeNew + nNodeUpdate;
	}

	@Override
	public void statsReset() {
		nNodeRead = 0;
		nNodeNew = 0;
		nNodeUpdate = 0;
		nBytesRead = 0;
		nBytesWritten = 0;
		maxNodeSize = 0;
	}

	@Override
	public String toString() {
		int nBytesInStorage = 0;
		for (byte[] ba: database.values()) {
			nBytesInStorage += ba.length;
		}
		return "nNodeRead=" + nNodeRead + 
				"  nNodeNew=" + nNodeNew +
				"  nNodeUpdate=" + nNodeUpdate + 
				"  pages=" + database.size() + 
				"  bytesRead=" + nBytesRead +
				"  bytesWritten=" + nBytesWritten +
				"  bytesStored=" + nBytesInStorage +
				"  maxNodeSize=" + maxNodeSize +
				"  avgNodeSize=" + (nBytesInStorage/database.size());
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
		log("flush size=", bufferById.size(), "/", bufferByObj.size());
		for (Map.Entry<Integer, Externalizable> e: bufferById.entrySet()) {
			writeNode(e.getValue(), e.getKey());
		}
		bufferById.clear();
		bufferByObj.clear();
	}
	
	private void assignBucket(Integer nodeId, Externalizable o) {
		if (prevNodeId == null) {
			//no previous read -> must be root
			Bucket b = new Bucket(++bucketIdCount);
			b.addNode(nodeId, o);
			
		}
		
	}

	private void reassignBucket(Integer id, Externalizable o) {
		// TODO Auto-generated method stub
		
	}

	private static class Bucket {
		private final int id;
		private final int totalSize = 0;
		public Bucket(int id) {
			this.id = id;
		}
		public boolean addNode(Integer nodeId, Externalizable o) {
			//if (small enough)// TODO Auto-generated method stub
			return true;
			//else
			//return false
			
			//Priorities: for new nodes
			// - first try to store in bucket of parent node, then try siblings (node with same 
			//   parent). 
			//   -> 'parent' has some flaws: 
			//      1) New intermediate nodes could mean that a a previously immediate parent
			//         mutates to a grand-parent (still in same node)
			//		   -> prefer subnodes with infixLen==0?????
			//      2) -> Later, a direct child could be rejected because of grandchildren that 
			//         already reside in the node. Should we trigger a reorg in this case?
			//      3) Resizing could mean that any node (parent/child/grandchild) could trigger
			//         a split. We need to reanalize the nodes to find out which are direct children
			//         -> Again, store infixlen/postlen in the bucket?
			//         -> Prioritization on postLen could still favour certain grandchildren over
			//            certain direct children. However, would that be really bad? By tendency,
			//            this could result in a more compact structure, with clusters of points 
			//            ending up io the same node
			//
			//   -> bulk loading would be an advantage. 
			
		}
	}
}
