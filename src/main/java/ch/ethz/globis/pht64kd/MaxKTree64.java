package ch.ethz.globis.pht64kd;

import ch.ethz.globis.phtree.v12.nt.NodeTreeV12;

public class MaxKTree64<T> implements MaxKTreeI {

	private final long[] dummy;
	private final NodeTree64<T> tree;
	
	private MaxKTree64(int keyWidth) {
		this.tree = NodeTreeV12.create(keyWidth);
		this.dummy = new long[getKeyBitWidth()];
	}
	
	public static <T> MaxKTree64<T> create() {
		return create(64);
	}
	
	public static <T> MaxKTree64<T> create(int keyWidth) {
		return new MaxKTree64<>(keyWidth);
	}
	
	public T put(long key, T value) {
		return tree.put(key, dummy, value);
	}
	
	public T putKD(long key, long[] kdKey, T value) {
		return tree.put(key, kdKey, value);
	}
	
	public boolean contains(long key) {
		return tree.contains(key, dummy);
	}
	
	public T get(long key) {
		return tree.get(key, dummy);
	}
	
	public T getKd(long key, long[] kdKeyOut) {
		return tree.get(key, kdKeyOut);
	}
	
	public T remove(long key) {
		return tree.remove(key);
	}
	
	public boolean delete(long key) {
		return tree.remove(key) != null;
	}
	
	public String toStringTree() {
		return tree.toStringTree();
	}
	
	public PhIterator64<T> queryWithMask(long minMask, long maxMask) {
		return tree.queryWithMask(minMask, maxMask);
	}
	
	public PhIterator64<T> query(long min, long max) {
		return tree.query(min, max);
	}
	
	public PhIterator64<T> iterator() {
		return tree.iterator();
	}
	
	public boolean checkTree() {
		return tree.checkTree();
	}

	@Override
	public int size() {
		return tree.size();
	}

	@Override
	public int getKeyBitWidth() {
		return tree.getKeyBitWidth();
	}

	@Override
	public Object getRoot() {
		return tree.getRoot();
	}
	
}