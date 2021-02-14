/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.mockito.Mockito;

public class EndpointImplTest {
	@Test
	public void testRepeatOpenDoesNotModifyEndpoint() {
		ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
		EndpointImpl endpoint = Mockito.spy(EndpointImpl.class);
		int[] endpointLocalOpenCallCount = new int[1];
		int[] endpointLocalCloseCallCount = new int[1];
		ConnectionImpl endpointConnectionImpl;
		endpointConnectionImpl = mockConnection;
		Mockito.doAnswer((stubInvo) -> {
			endpointLocalOpenCallCount[0]++;
			return null;
		}).when(endpoint).localOpen();
		Mockito.doAnswer((stubInvo) -> {
			return endpointConnectionImpl;
		}).when(endpoint).getConnectionImpl();
		Mockito.doAnswer((stubInvo) -> {
			endpointLocalCloseCallCount[0]++;
			return null;
		}).when(endpoint).localClose();

		// Check starting state
		assertFalse("Should not be modified", endpoint.isModified());
		assertEquals("Unexpected localOpen call count", 0, endpointLocalOpenCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(0)).addModified(Mockito.any(EndpointImpl.class));

		endpoint.open();

		// Check endpoint was modified
		assertTrue("Should be modified", endpoint.isModified());
		assertEquals("Unexpected localOpen call count", 1, endpointLocalOpenCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(1)).addModified(Mockito.any(EndpointImpl.class));

		// Clear the modified state, open again, verify no change
		endpoint.clearModified();
		assertFalse("Should no longer be modified", endpoint.isModified());

		endpoint.open();

		assertFalse("Should not be modified", endpoint.isModified());
		assertEquals("Unexpected localOpen call count", 1, endpointLocalOpenCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(1)).addModified(Mockito.any(EndpointImpl.class));
	}

	@Test
	public void testRepeatCloseDoesNotModifyEndpoint() {
		ConnectionImpl mockConnection = Mockito.mock(ConnectionImpl.class);
		EndpointImpl endpoint = Mockito.spy(EndpointImpl.class);
		int[] endpointLocalOpenCallCount = new int[1];
		int[] endpointLocalCloseCallCount = new int[1];
		ConnectionImpl endpointConnectionImpl;
		endpointConnectionImpl = mockConnection;
		Mockito.doAnswer((stubInvo) -> {
			endpointLocalOpenCallCount[0]++;
			return null;
		}).when(endpoint).localOpen();
		Mockito.doAnswer((stubInvo) -> {
			return endpointConnectionImpl;
		}).when(endpoint).getConnectionImpl();
		Mockito.doAnswer((stubInvo) -> {
			endpointLocalCloseCallCount[0]++;
			return null;
		}).when(endpoint).localClose();

		// Open endpoint, clear the modified state, verify current state
		endpoint.open();
		endpoint.clearModified();
		assertFalse("Should no longer be modified", endpoint.isModified());
		assertEquals("Unexpected localClose call count", 0, endpointLocalCloseCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(1)).addModified(Mockito.any(EndpointImpl.class));

		// Now close, verify changes
		endpoint.close();

		// Check endpoint was modified
		assertTrue("Should be modified", endpoint.isModified());
		assertEquals("Unexpected localClose call count", 1, endpointLocalCloseCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(2)).addModified(Mockito.any(EndpointImpl.class));

		// Clear the modified state, close again, verify no change
		endpoint.clearModified();
		assertFalse("Should no longer be modified", endpoint.isModified());

		endpoint.close();

		assertFalse("Should not be modified", endpoint.isModified());
		assertEquals("Unexpected localClose call count", 1, endpointLocalCloseCallCount[0]);
		Mockito.verify(mockConnection, Mockito.times(2)).addModified(Mockito.any(EndpointImpl.class));
	}
}
