package ch.ethz.globis.phtree.test.util;

import ch.ethz.globis.phtree.PhTree;

public interface TestUtilAPI {

	public <T> PhTree<T> newTreeV(int dim, int depth);
	public <T> PhTree<T> newTreeV(int dim);

	public <T> void close(PhTree<T> tree);
	public void beforeTest();
	public void beforeTest(Object[] args);
	public void afterTest();
	public void beforeSuite();
	public void afterSuite();
	public void beforeClass();
	public void afterClass();
}
