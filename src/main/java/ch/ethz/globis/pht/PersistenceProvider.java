package ch.ethz.globis.pht;

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

public interface PersistenceProvider {
	
	/**
	 * The empty implementation of a persistence provide, it does not provide persistence.
	 */
	public static final PersistenceProvider NONE = new PersistenceProvider() {
		@Override
		public Object storeObject(Externalizable o) {
			return o;
		}
		
		@Override
		public Object resolveObject(Object o) {
			return o;
		}

		@Override
		public String getDescription() {
			return "NONE";
		}

		@Override
		public int statsGetPageReads() {
			return -1;
		}

		@Override
		public int statsGetPageWrites() {
			return -1;
		}

		@Override
		public void statsReset() {
			//
		}
	};
	
	public Object resolveObject(Object o);
	public Object storeObject(Externalizable o);
	public String getDescription();
	public int statsGetPageReads();
	public int statsGetPageWrites();
	public void statsReset();
}