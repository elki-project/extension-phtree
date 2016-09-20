package ch.ethz.globis.phtree.test;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import ch.ethz.globis.phtree.PersistenceProvider;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.PhTreeConfig;
import ch.ethz.globis.phtree.PhTree.PhIterator;
import ch.ethz.globis.phtree.test.util.TestSuper;
import ch.ethz.globis.phtree.v12.PersProviderPagedSerBuf;
import ch.ethz.globis.phtree.v12.PhTree12;

public class TestExternalization extends TestSuper {

    public static <T> PhTree<T> createTree(int dim) {
    	PersistenceProvider pp = new PersProviderPagedSerBuf();
    	PhTreeConfig cfg = new PhTreeConfig(dim);
    	cfg.setPersistenceProvider(pp);
    	return PhTree.create(cfg);
    }
    
    private <T> PhTree<T> storeAndRead(PhTree<T> aTree) {
   		PhTree12<T> tree = (PhTree12<T>) aTree;
   		PersistenceProvider pp = tree.getPersistenceProvider();
   		pp.flush();
   		System.out.println(pp);
   		return pp.loadTree();
    }

	@Test
	public void testBIG() {
		for (int d = 2; d < 10; d++) {
			smokeTest(1000, d, 0);
		}
	}
	
	@Test
	public void test3D() {
		smokeTest(10000, 3, 0);
	}
	
	@Test
	public void test2D() {
		smokeTest(10000, 2, 0);
	}
	
	@Test
	public void test2D_8() {
		smokeTest(100, 2, 2);
	}
	
	@Test
	public void test2D_8_Bug10b() {
//		for (int i = 0; i < 10000; i++) {
//			System.out.println("iii=" + i);
			smokeTest(5, 2, 1619);
//		}
	}
	
	@Test
	public void test2D_8_BugNP() {
//		for (int i = 0; i < 1000; i++) {
//			System.out.println("iii=" + i);
			smokeTest(20, 2, 205);
//		}
	}
	
	private void smokeTest(int N, int DIM, long SEED) { 
		Random R = new Random(SEED);
		PhTree<Integer> ind = createTree(DIM);
		long[][] keys = new long[N][DIM];
		for (int i = 0; i < N; i++) {
			for (int d = 0; d < DIM; d++) {
				keys[i][d] = R.nextInt(); //INT!
			}
			if (ind.contains(keys[i])) {
				i--;
				continue;
			}
			//build
			assertNull(ind.put(keys[i], Integer.valueOf(i)));
			//System.out.println("key=" + Bits.toBinary(keys[i], 64));
			//System.out.println(ind.toStringTree());
			assertTrue("i="+ i, ind.contains(keys[i]));
			assertEquals(i, (int)ind.get(keys[i]));
		}
		
		ind = storeAndRead(ind);
		
		//first check
		for (int i = 0; i < N; i++) {
			assertTrue(ind.contains(keys[i]));
			assertEquals(i, (int)ind.get(keys[i]));
		}
		
		//update
		for (int i = 0; i < N; i++) {
			assertEquals(i, (int)ind.put(keys[i], Integer.valueOf(-i)));
			assertTrue(ind.contains(keys[i]));
			assertEquals(-i, (int)ind.get(keys[i]));
		}
		
		//check again
		for (int i = 0; i < N; i++) {
			assertTrue(ind.contains(keys[i]));
			assertEquals(-i, (int)ind.get(keys[i]));
		}
		
		//delete
		for (int i = 0; i < N; i++) {
			//System.out.println("Removing: " + Bits.toBinary(keys[i], 64));
			//System.out.println("Tree: \n" + ind);
			assertEquals(-i, (int)ind.remove(keys[i]));
			assertFalse(ind.contains(keys[i]));
			assertNull(ind.get(keys[i]));
		}
		
		assertEquals(0, ind.size());
	}
	
	
	
	@Test
	public void testQuery() {
		int N = 1000;
		int DIM = 3;
		Random R = new Random(0);
		PhTree<Integer> ind = createTree(DIM);
		long[][] keys = new long[N][DIM];
		for (int i = 0; i < N; i++) {
			for (int d = 0; d < DIM; d++) {
				keys[i][d] = R.nextInt(); //INT!
			}
			if (ind.contains(keys[i])) {
				i--;
				continue;
			}
			//build
			assertNull(ind.put(keys[i], Integer.valueOf(i)));
			assertTrue(ind.contains(keys[i]));
			assertEquals(i, (int)ind.get(keys[i]));
		}
		
		ind = storeAndRead(ind);
		
		//extent query
		PhIterator<Integer> i1 = ind.queryExtent();
		int n = 0;
		while (i1.hasNext()) {
			PhEntry<Integer> e = i1.nextEntry();
			assertArrayEquals(keys[e.getValue()], e.getKey());
			n++;
		}
		assertEquals(N, n);
		
		//full range query
		long[] min = new long[DIM];
		long[] max = new long[DIM];
		Arrays.fill(min, Long.MIN_VALUE);
		Arrays.fill(max, Long.MAX_VALUE);
		i1 = ind.query(min, max);
		n = 0;
		while (i1.hasNext()) {
			PhEntry<Integer> e = i1.nextEntry();
			assertNotNull(e);
			assertArrayEquals(keys[e.getValue()], e.getKey());
			n++;
		}
		assertEquals(N, n);
		
		//spot queries
		for (int i = 0; i < N; i++) {
			i1 = ind.query(keys[i], keys[i]);
			assertTrue(i1.hasNext());
			PhEntry<Integer> e = i1.nextEntry();
			assertArrayEquals(keys[i], e.getKey());
			assertEquals(i, (int)e.getValue());
			assertFalse(i1.hasNext());
		}
		
		//delete
		for (int i = 0; i < N; i++) {
			//System.out.println("Removing: " + Bits.toBinary(keys[i], 64));
			//System.out.println("Tree: \n" + ind);
			assertEquals(i, (int)ind.remove(keys[i]));
			assertFalse(ind.contains(keys[i]));
			assertNull(ind.get(keys[i]));
		}
		
		assertEquals(0, ind.size());
	}
	

	@Test
	public void testQueryBug() {
		int DIM = 3;
		PhTree<Integer> ind = createTree(DIM);
		long[][] keys = {
				{629649304, -1266264776, 99807007},
				{5955764, -1946737912, 39620447},
				{1086124775, -1609984092, 1227951724},
		};
		int N = keys.length;
		for (int i = 0; i < N; i++) {
			if (ind.contains(keys[i])) {
				i--;
				continue;
			}
			//build
			assertNull(ind.put(keys[i], Integer.valueOf(i)));
			assertTrue(ind.contains(keys[i]));
			assertEquals(i, (int)ind.get(keys[i]));
		}
		
		ind = storeAndRead(ind);

		//extent query
		PhIterator<Integer> i1 = ind.queryExtent();
		int n = 0;
		while (i1.hasNext()) {
			PhEntry<Integer> e = i1.nextEntry();
			assertArrayEquals(keys[e.getValue()], e.getKey());
			n++;
		}
		assertEquals(N, n);
		
		//full range query
		long[] min = new long[DIM];
		long[] max = new long[DIM];
		Arrays.fill(min, Long.MIN_VALUE);
		Arrays.fill(max, Long.MAX_VALUE);
		i1 = ind.query(min, max);
		n = 0;
		while (i1.hasNext()) {
			PhEntry<Integer> e = i1.nextEntry();
			assertNotNull(e);
			assertArrayEquals(keys[e.getValue()], e.getKey());
			n++;
		}
		assertEquals(N, n);
		
		//spot queries
		for (int i = 0; i < N; i++) {
			i1 = ind.query(keys[i], keys[i]);
			assertTrue(i1.hasNext());
			PhEntry<Integer> e = i1.nextEntry();
			assertArrayEquals(keys[i], e.getKey());
			assertEquals(i, (int)e.getValue());
			assertFalse(i1.hasNext());
		}
		
		//delete
		for (int i = 0; i < N; i++) {
			//System.out.println("Removing: " + Bits.toBinary(keys[i], 64));
			//System.out.println("Tree: \n" + ind);
			assertEquals(i, (int)ind.remove(keys[i]));
			assertFalse(ind.contains(keys[i]));
			assertNull(ind.get(keys[i]));
		}
		
		assertEquals(0, ind.size());
	}
	
	@Test
	public void testQuerySet() {
		int N = 1000;
		int DIM = 3;
		Random R = new Random(0);
		PhTree<Integer> ind = createTree(DIM);
		long[][] keys = new long[N][DIM];
		for (int i = 0; i < N; i++) {
			for (int d = 0; d < DIM; d++) {
				keys[i][d] = R.nextInt(); //INT!
			}
			if (ind.contains(keys[i])) {
				i--;
				continue;
			}
			//build
			assertNull(ind.put(keys[i], Integer.valueOf(i)));
			assertTrue(ind.contains(keys[i]));
			assertEquals(i, (int)ind.get(keys[i]));
		}
		
		ind = storeAndRead(ind);

		//full range query
		List<PhEntry<Integer>> list;
		long[] min = new long[DIM];
		long[] max = new long[DIM];
		Arrays.fill(min, Long.MIN_VALUE);
		Arrays.fill(max, Long.MAX_VALUE);
		list = ind.queryAll(min, max);
		int n = 0;
		for (PhEntry<Integer> e: list) {
			assertNotNull(e);
			assertArrayEquals(keys[e.getValue()], e.getKey());
			n++;
		}
		assertEquals(N, n);
		
		//spot queries
		for (int i = 0; i < N; i++) {
			list = ind.queryAll(keys[i], keys[i]);
			assertFalse("i=" + i, list.isEmpty());
			PhEntry<Integer> e = list.iterator().next();
			assertArrayEquals(keys[i], e.getKey());
			assertEquals(i, (int)e.getValue());
			assertEquals(1, list.size());
		}
		
		//delete
		for (int i = 0; i < N; i++) {
			//System.out.println("Removing: " + Bits.toBinary(keys[i], 64));
			//System.out.println("Tree: \n" + ind);
			assertEquals(i, (int)ind.remove(keys[i]));
			assertFalse(ind.contains(keys[i]));
			assertNull(ind.get(keys[i]));
		}
		
		assertEquals(0, ind.size());
	}
	

}
