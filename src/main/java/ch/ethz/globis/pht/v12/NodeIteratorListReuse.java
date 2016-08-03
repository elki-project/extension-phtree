package ch.ethz.globis.pht.v12;

import java.util.List;

import ch.ethz.globis.pht.PersistenceProvider;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhTreeHelper;
import ch.ethz.globis.pht.v12.PhTree12.NodeEntry;
import ch.ethz.globis.pht.v12.nt.NodeTreeV12.NtEntry12;
import ch.ethz.globis.pht.v12.nt.NtIteratorMask;

/**
 * A NodeIterator that returns a list instead of an Iterator AND reuses the NodeIterator.
 * 
 * This seems to be slower than the simple NodeIteratorList. Why? 
 * The reason could be that the simple NIL creates many objects, but they end up in the stack
 * memory because they can not escape.
 * The NILReuse creates much less objects, but they end up in an array for reuse, which means
 * they may not be on the stack.... 
 * 
 * @author ztilmann
 *
 * @param <T>
 * @param <R>
 */
public class NodeIteratorListReuse<T, R> {
	
	private class PhIteratorStack {
		@SuppressWarnings("unchecked")
		private final NodeIterator[] stack = new NodeIteratorListReuse.NodeIterator[64];
		private int size = 0;


		NodeIterator prepare() {
			NodeIterator ni = stack[size++];
			if (ni == null)  {
				ni = new NodeIterator();
				stack[size-1] = ni;
			}
			return ni;
		}

		NodeIterator pop() {
			return stack[--size];
		}
	}

	private final int dims;
	private final PhResultList<T,R> results;
	private int maxResults;
	private final long[] valTemplate;
	private long[] rangeMin;
	private long[] rangeMax;

	private final PhIteratorStack pool;
	private final PersistenceProvider pp;
	
	private final class NodeIterator {
	
		private Node node;
		private NtIteratorMask<Object> niIterator;
		private int nMaxEntry;
		private int nEntryFound = 0;
		private long maskLower;
		private long maskUpper;
		private boolean useHcIncrementer;

		/**
		 * 
		 * @param node
		 * @param dims
		 * @param valTemplate A null indicates that no values are to be extracted.
		 * @param lower The minimum HC-Pos that a value should have.
		 * @param upper
		 * @param minValue The minimum value that any found value should have. 
		 * 				   If the found value is lower, the search continues.
		 * @param maxValue
		 */
		void reinitAndRun(Node node, long lower, long upper) {
			this.node = node;
			boolean isNI = node.isNT();
			this.niIterator = null;
			nMaxEntry = node.getEntryCount();
			this.nEntryFound = 0;
			this.maskLower = lower;
			this.maskUpper = upper;

			useHcIncrementer = false;
			if (isNI && niIterator == null) {
				//TODO use non-mask iterator if node is fully included in query rectangle 
				niIterator = node.ntIteratorWithMask(dims, maskLower, maskUpper, pp);
			}

			if (dims > 6 && nMaxEntry > 10) {
				initHCI(isNI);
			}

			//For sub-HC, the standard iteration is extremely efficient ( node[pos]!=0 ?), so we
			//should resort to HC-incrementer only rarely.
			//Use it only if it is at least 25% full

			getAll();
		}

		private void initHCI(boolean isNI) {
			//LHC, NI, ...
			long maxHcAddr = ~((-1L)<<dims);
			int nSetFilterBits = Long.bitCount(maskLower | ((~maskUpper) & maxHcAddr));
			//nPossibleMatch = (2^k-x)
			long nPossibleMatch = 1L << (dims - nSetFilterBits);
			if (isNI) {
				int nChild = node.ntGetSize();
				int logNChild = Long.SIZE - Long.numberOfLeadingZeros(nChild);
				//TODO div by logNChild i.o. (double)?
				//the following will overflow for k=60
				//DIM < 60 as safeguard against overflow of (nPossibleMatch*logNChild)
				useHcIncrementer = PhTree12.HCI_ENABLED && dims < 50 
						&& (nChild > nPossibleMatch*(double)logNChild*2);
				if (!useHcIncrementer) {
					niIterator.reset(node.ind(), maskLower, maskUpper);
				}
			} else if (PhTree12.HCI_ENABLED){
				if (node.isAHC()) {
					//nPossibleMatch < 2^k? --> nPossibleMatch can be only 1.0,0.5,0.25,... of 2^k 
					useHcIncrementer = nPossibleMatch < maxHcAddr;
				} else {
					int logNPost = Long.SIZE - Long.numberOfLeadingZeros(nMaxEntry) + 1;
					useHcIncrementer = (nMaxEntry > nPossibleMatch*(double)logNPost); 
				}
			}
		}
		
		private void checkAndAddResult(PhEntry<T> e) {
			results.phOffer(e);
		}

		private void checkAndRunSubnode(byte subCode, Object sub) {
			if (results.phIsPrefixValid(valTemplate, subCode+1)) {
				run((Node) pp.resolveObject(sub));
			}
		}

		@SuppressWarnings("unchecked")
		private void readValue(int pin, long pos) {
			NodeEntry<T> resultBuffer = (NodeEntry<T>) results.phGetTempEntry();
			long[] key = resultBuffer.getKey();
			Object o = node.checkAndGetEntryPIN(pin, pos, valTemplate, key, rangeMin, rangeMax);
 			if (o == null) {
				results.phReturnTemp(resultBuffer);
				return;
			}
 			byte subCode = node.getSubCode(pin);
 			if (Node.isSubNode(subCode)) {
				results.phReturnTemp(resultBuffer);
				checkAndRunSubnode( subCode, o );
				return;
			}
			resultBuffer.setValue((T) o);
			checkAndAddResult(resultBuffer);
		}

		private void readValue(long pos, Object value, NodeEntry<T> result) {
			if (!node.checkAndGetEntryNt(pos, value, result, valTemplate, rangeMin, rangeMax)) {
				results.phReturnTemp(result);
				return;
			}
			
			checkAndAddResult(result);
		}

		private void getAllHCI() {
			//Ideally we would switch between b-serch-HCI and incr-search depending on the expected
			//distance to the next value.
			long currentPos = maskLower;
			do {
				int pin = node.getPosition(currentPos, dims);
				if (pin >= 0) {
					readValue(pin, currentPos);
				}
				
				currentPos = PhTree12.inc(currentPos, maskLower, maskUpper);
				if (currentPos <= maskLower) {
					break;
				}
			} while (results.size() < maxResults);
		}


		private void getAll() {
			if (node.isNT()) {
				niAllNext();
				return;
			}

			if (useHcIncrementer) {
				getAllHCI();
			} else if (node.isAHC()) {
				getAllAHC();
			} else {
				getAllLHC();
			}
		}

		private void getAllAHC() {
			//Position of the current entry
			long currentPos = maskLower; 
			while (results.size() < maxResults) {
				//check HC-pos
				if (checkHcPos(currentPos)) {
					//check post-fix
					readValue((int) currentPos, currentPos);
				}

				currentPos++;  //pos w/o bit-offset
				if (currentPos > maskUpper) {
					break;
				}
			}
		}

		private void getAllLHC() {
			//Position of the current entry
			int currentOffsetKey = node.getBitPosIndex();
			//length of post-fix WITH key
			int postEntryLenLHC = Node.IK_WIDTH(dims) + dims*node.getPostLen();
			while (results.size() < maxResults) {
				if (++nEntryFound > nMaxEntry) {
					break;
				}
				long currentPos = Bits.readArray(node.ba, currentOffsetKey, Node.IK_WIDTH(dims));
				currentOffsetKey += postEntryLenLHC;
				//check HC-pos
				if (!checkHcPos(currentPos)) {
					if (currentPos > maskUpper) {
						break;
					}
				} else {
					//check post-fix
					readValue(nEntryFound - 1, currentPos);
				}
			}
		}


		private void niAllNext() {
			//iterator?
			if (useHcIncrementer) {
				niAllNextHCI();
			} else {
				niAllNextIterator();
			}
		}
		
		private void niAllNextIterator() {
			//ITERATOR is used for DIM>6 or if results are dense 
			while (niIterator.hasNext() && results.size() < maxResults) {
				NtEntry12<Object> e = niIterator.nextEntryReuse();
				byte subCode = e.getKdSubCode();
				if (Node.isSubNode(subCode)) {
					PhTreeHelper.applyHcPos(e.key(), node.getPostLen(), valTemplate);
					if (node.checkAndApplyInfixNt(subCode, e.getKdKey(),
							valTemplate, rangeMin, rangeMax)) {
						checkAndRunSubnode(subCode, e.value());
					}
				} else {
					NodeEntry<T> resultBuffer = (NodeEntry<T>) results.phGetTempEntry();
					System.arraycopy(e.getKdKey(), 0, resultBuffer.getKey(), 0, dims);
					resultBuffer.setSubCode(e.getKdSubCode());
					readValue(e.key(), e.value(), resultBuffer);
				}
			}
			return;
		}

		private void niAllNextHCI() {
			//HCI is used for DIM <=6 or if results are sparse
			//repeat until we found a value inside the given range
			long currentPos = maskLower; 
			while (results.size() < maxResults) {
				NodeEntry<T> resultBuffer = (NodeEntry<T>) results.phGetTempEntry();
				node.ntGetEntry(currentPos, resultBuffer, pp);
				byte subCode = resultBuffer.getSubCode();
				if (Node.isSubNode(subCode)) {
					PhTreeHelper.applyHcPos(currentPos, node.getPostLen(), valTemplate);
					if (node.checkAndApplyInfixNt(subCode, resultBuffer.getKey(), 
							valTemplate, rangeMin, rangeMax)) {
						Object o = resultBuffer.getValue();
						results.phReturnTemp(resultBuffer);
						checkAndRunSubnode(subCode, o);
					}
				} else if (!Node.isSubEmpty(subCode)) { 
					//read and check post-fix
					readValue(currentPos, resultBuffer.getValue(), resultBuffer);
				} else {
					results.phReturnTemp(resultBuffer);
				}

				currentPos = PhTree12.inc(currentPos, maskLower, maskUpper);
				if (currentPos <= maskLower) {
					break;
				}
			}
		}


		private boolean checkHcPos(long pos) {
			return ((pos | maskLower) & maskUpper) == pos;
		}
	}
	
	NodeIteratorListReuse(int dims, PhResultList<T, R> results, PersistenceProvider pp) {
		this.pp = pp;
		this.dims = dims;
		this.valTemplate = new long[dims];
		this.results = results;
		this.pool = new PhIteratorStack();
	}

	List<R> resetAndRun(Node node, long[] rangeMin, long[] rangeMax, int maxResults) {
		results.clear();
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
		this.maxResults = maxResults;
		run(node);
		return results;
	}
	
	void run(Node node) {
		//create limits for the local node. there is a lower and an upper limit. Each limit
		//consists of a series of DIM bit, one for each dimension.
		//For the lower limit, a '1' indicates that the 'lower' half of this dimension does 
		//not need to be queried.
		//For the upper limit, a '0' indicates that the 'higher' half does not need to be 
		//queried.
		//
		//              ||  lowerLimit=0 || lowerLimit=1 || upperLimit = 0 || upperLimit = 1
		// =============||===================================================================
		// query lower  ||     YES             NO
		// ============ || ==================================================================
		// query higher ||                                     NO               YES
		//
		long maskHcBit = 1L << node.getPostLen();
		long maskVT = (-1L) << node.getPostLen();
		long lowerLimit = 0;
		long upperLimit = 0;
		if (maskHcBit >= 0) { //i.e. postLen < 63
			for (int i = 0; i < valTemplate.length; i++) {
				lowerLimit <<= 1;
				upperLimit <<= 1;
				long nodeBisection = (valTemplate[i] | maskHcBit) & maskVT; 
				if (rangeMin[i] >= nodeBisection) {
					//==> set to 1 if lower value should not be queried 
					lowerLimit |= 1L;
				}
				if (rangeMax[i] >= nodeBisection) {
					//Leave 0 if higher value should not be queried.
					upperLimit |= 1L;
				}
			}
		} else {
			//special treatment for signed longs
			//The problem (difference) here is that a '1' at the leading bit does indicate a
			//LOWER value, opposed to indicating a HIGHER value as in the remaining 63 bits.
			//The hypercube assumes that a leading '0' indicates a lower value.
			//Solution: We leave HC as it is.

			for (int i = 0; i < valTemplate.length; i++) {
				lowerLimit <<= 1;
				upperLimit <<= 1;
				if (rangeMin[i] < 0) {
					//If minimum is positive, we don't need the search negative values 
					//==> set upperLimit to 0, prevent searching values starting with '1'.
					upperLimit |= 1L;
				}
				if (rangeMax[i] < 0) {
					//Leave 0 if higher value should not be queried
					//If maximum is negative, we do not need to search positive values 
					//(starting with '0').
					//--> lowerLimit = '1'
					lowerLimit |= 1L;
				}
			}
		}
		NodeIterator nIt = pool.prepare();
		nIt.reinitAndRun(node, lowerLimit, upperLimit);
		pool.pop();
	}

}
