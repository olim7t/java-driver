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

import com.google.common.base.Throwables;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionDiagnostics extends AbstractMBean implements SessionDiagnosticsMBean {
    private static final Logger logger = LoggerFactory.getLogger(SessionDiagnostics.class);

    private SessionManager session;

    SessionDiagnostics(SessionManager session) {
        super(buildObjectName(session));
        this.session = session;
    }

    @Override
    public String getLoggedKeyspace() {
        return session.getLoggedKeyspace();
    }

    @Override
    public boolean isClosing() {
        return session.isClosed();
    }

    @Override
    public TabularData getConnectionStats() {
        return ConnectionTabularData.from(session.getState());
    }

    private static ObjectName buildObjectName(SessionManager session) {
        try {
            return new ObjectName("com.datastax.driver.core:type=Session,name="
                + session.cluster.getClusterName() + "-" + session.hashCode());
        } catch (MalformedObjectNameException e) {
            logger.warn("Could not build object name", e);
            return null;
        }
    }

    static class ConnectionTabularData {
        private static final String TYPE_NAME = "Host";

        private static final String ROW_DESC = "Host";

        private static final String[] ITEM_NAMES = new String[]{ "datacenter", "rack", "address", "connections", "inFlight", "meanInFlight" };

        private static final String[] ITEM_DESCS = new String[]{ "Datacenter", "Rack", "Host address", "Open connections",
            "Total in flight queries", "Mean in flight queries per connection" };

        private static final OpenType<?>[] ITEM_TYPES = new OpenType[]{ SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.INTEGER, SimpleType.INTEGER, SimpleType.INTEGER };

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

        static TabularData from(Session.State state) {
            try {
                TabularDataSupport result = new TabularDataSupport(TABULAR_TYPE);
                for (Host host : state.getConnectedHosts()) {

                    int openConnections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    int meanInFlightPerConnection = inFlightQueries / openConnections;

                    result.put(new CompositeDataSupport(COMPOSITE_TYPE, ITEM_NAMES,
                        new Object[]{ host.getDatacenter(), host.getRack(), host.toString(),
                            openConnections, inFlightQueries, meanInFlightPerConnection }));
                }
                return result;
            } catch (OpenDataException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

