package ch.ethz.globis.pht.util;

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


import java.io.Serializable;

import ch.ethz.globis.pht.PhEntry;

/**
 * A mapping function that maps long[] / T to a desired output format.
 *
 * This interface needs to be serializable because in the distributed version of the PhTree, 
 * it is send from the client machine to the server machine.
 *
 * @author ztilmann
 */
public interface PhMapper<T, R> extends Serializable {

//	static <T> PhMapper<T, PhEntry<T>> PVENTRY() {
//		return e -> e;
//	}
//
//	static <T, R> PhMapper<T, R> MAP(final PhMapperKey<R> mapper) {
//		return e -> mapper.map(e.getKey());
//	}

	/**
	 * Maps a PhEntry to something else. 
	 * @param e
	 * @return The converted entry
	 */
	R map(PhEntry<T> e);
}