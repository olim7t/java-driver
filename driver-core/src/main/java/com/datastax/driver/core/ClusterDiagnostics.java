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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.policies.LoadBalancingPolicy;

class ClusterDiagnostics extends AbstractMBean implements ClusterDiagnosticsMBean {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDiagnostics.class);

    private final Cluster.Manager manager;

    ClusterDiagnostics(Cluster.Manager manager) {
        super(buildObjectName(manager));
        this.manager = manager;
    }

    @Override
    public String getDriverVersion() {
        return Cluster.getDriverVersion();
    }

    @Override
    public String getProtocolVersion() {
        return Integer.toString(manager.protocolVersion());
    }

    @Override
    public String getControlConnectionHost() {
        ControlConnection connection = manager.controlConnection;
        return connection.isOpen()
            ? connection.connectedHost().toString()
            : null;
    }

    @Override
    public List<String> computeNextQueryPlan() {
        LoadBalancingPolicy lbp = manager.configuration.getPolicies().getLoadBalancingPolicy();
        Iterator<Host> iter = lbp.newQueryPlan(null, new SimpleStatement(""));
        List<String> queryPlan = Lists.newArrayList();
        while (iter.hasNext()) {
            queryPlan.add(iter.next().toString());
        }
        return queryPlan;
    }

    @Override
    public TabularData getHosts() {
        return HostTabularData.from(manager.metadata.getAllHosts(),
            manager.configuration.getPolicies().getLoadBalancingPolicy());
    }

    @Override
    public List<String> getContactPoints() {
        List<String> result = Lists.newArrayListWithExpectedSize(manager.contactPoints.size());
        for (InetSocketAddress contactPoint : manager.contactPoints) {
            result.add(contactPoint.toString());
        }
        return result;
    }

    @Override
    public boolean isClosing() {
        // NB: makes sense to call the MBean method "isClosing", because once the cluster is *really* closed the
        // MBean is unregistered.
        return manager.isClosed();
    }

    @Override
    public String dumpConfiguration() {
        return manager.configuration.toString();
    }

    private static ObjectName buildObjectName(Cluster.Manager manager) {
        try {
            return new ObjectName("com.datastax.driver.core:type=Cluster,name=" + manager.clusterName);
        } catch (MalformedObjectNameException e) {
            logger.warn("Could not build object name", e);
            return null;
        }
    }

    static class HostTabularData {
        private static final String TYPE_NAME = "Host";

        private static final String ROW_DESC = "Host";

        private static final String[] ITEM_NAMES = new String[]{ "datacenter", "rack", "address", "version", "state", "distance",
            "reconnectFromSuspected", "reconnectFromDown" };

        private static final String[] ITEM_DESCS = new String[]{ "Datacenter", "Rack", "Host address", "Cassandra version",
            "Host state", "Host distance", "Reconnecting from SUSPECTED", "Reconnecting from DOWN" };

        private static final OpenType<?>[] ITEM_TYPES = new OpenType[]{ SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
            SimpleType.STRING, SimpleType.STRING, SimpleType.BOOLEAN, SimpleType.BOOLEAN };

        private static final CompositeType COMPOSITE_TYPE;

        private static final TabularType TABULAR_TYPE;

        static {
            try {
                COMPOSITE_TYPE = new CompositeType(TYPE_NAME, ROW_DESC, ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);

                TABULAR_TYPE = new TabularType(TYPE_NAME, ROW_DESC, COMPOSITE_TYPE, ITEM_NAMES);
            } catch (OpenDataException e) {
                throw Throwables.propagate(e);
            }
        }

        static TabularData from(Collection<Host> hosts, LoadBalancingPolicy loadBalancingPolicy) {
            try {
                TabularDataSupport result = new TabularDataSupport(TABULAR_TYPE);
                for (Host host : hosts) {
                    HostDistance distance = loadBalancingPolicy.distance(host);

                    boolean isReconnectingFromSuspected = host.getInitialReconnectionAttemptFuture() != null
                        && !host.getInitialReconnectionAttemptFuture().isDone();
                    boolean isReconnectingFromDown = host.getReconnectionAttemptFuture() != null
                        && !host.getReconnectionAttemptFuture().isDone();

                    result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES,
                        new Object[]{ host.getDatacenter(), host.getRack(), host.toString(),
                            host.getCassandraVersion().toString(),
                            host.state.toString(),
                            distance.toString(),
                            isReconnectingFromSuspected,
                            isReconnectingFromDown }));
                }
                return result;
            } catch (OpenDataException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
