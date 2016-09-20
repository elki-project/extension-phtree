package ch.ethz.globis.phtree.test.util;

import ch.ethz.globis.phtree.PhTree;

public class TestUtilInMemory implements TestUtilAPI {

	@Override
	public <T> PhTree<T> newTreeV(int dim, int depth) {
		return PhTree.create(dim);
	}

	@Override
	public <T> PhTree<T> newTreeV(int dim) {
		return PhTree.create(dim);
	}

	@Override
	public <T> void close(PhTree<T> tree) {
		//nothing to do
	}

	@Override
	public void beforeTest() {
		//nothing to do
	}

	@Override
	public void beforeTest(Object[] args) {
		//nothing to do
	}

	@Override
	public void afterTest() {
		//nothing to do
	}

	@Override
	public void beforeSuite() {
		//nothing to do
	}

	@Override
	public void afterSuite() {
		//nothing to do
	}

	@Override
	public void beforeClass() {
		//nothing to do
	}

	@Override
	public void afterClass() {
		//nothing to do
	}

}
