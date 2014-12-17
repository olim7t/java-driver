/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.List;

import javax.management.openmbean.TabularData;

/**
 * Exposes information about a running {@link Cluster} instance.
 */
public interface ClusterDiagnosticsMBean {
    /**
     * Return the driver version.
     *
     * @return the version.
     */
    String getDriverVersion();

    /**
     * Return the version of the native protocol used by the driver.
     *
     * @return the protocol version.
     */
    String getProtocolVersion();

    /**
     * Return the address of the host to which the control connection is currently connected.
     *
     * @return the address, or {@code null} if the control connection is disconnected.
     */
    String getControlConnectionHost();

    /**
     * Retrieve the next query plan from the {@link com.datastax.driver.core.policies.LoadBalancingPolicy}. This is the order
     * in which hosts would be tried if a query was executed right now.
     *
     * @return the addresses of the hosts in the query plan, in order.
     */
    List<String> computeNextQueryPlan();

    /**
     * Return the list of hosts currently known to the driver (both contact points and auto-discovered peers).
     *
     * @return various information about each host: location, state, distance, C* version, whether reconnection attempts are in
     * progress...
     */
    TabularData getHosts();

    /**
     * Return the contact points that were provided to initialize the driver.
     *
     * @return the addresses of the contact points.
     */
    List<String> getContactPoints();

    /**
     * Return whether this {@code Cluster} instance is currently closing (i.e. it has been closed but is still handling the last
     * pending requests). Note that once the instance is completely closed, the MBean will be unregistered.
     *
     * @return whether the cluster is closing.
     */
    boolean isClosing();
}
