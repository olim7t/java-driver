package com.datastax.driver.core;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.datastax.driver.core.CCMBridge.CCMCluster;

import static com.datastax.driver.core.TestUtils.waitFor;

/**
 * Due to C* gossip bugs, system.peers may report nodes that are gone from the cluster.
 *
 * This class tests scenarios where these nodes have been recommissionned to another cluster and
 * come back up. The driver must detect that they are not part of the cluster anymore, and ignore them.
 */
public class RecommissionnedNodeTest {
    CCMCluster current;
    CCMCluster other;

    @Test(groups = "long")
    public void testNodeAdded() throws InterruptedException {
        CCMBridge ccm = CCMBridge.create("currentCcm", 3);
        // node1 will be our "recommissionned" node, we simulate the gossip bug by just stopping it so that it stays in the
        // peers table.
        // Do it right now so that the driver never has a chance to connect to it.
        ccm.stop(1);

        current = CCMCluster.create(ccm, Cluster.builder(), 3);
        waitForCountUpHosts(current, 2);

        // Start other cluster that will reuse node1's address
        other = CCMCluster.create(1, Cluster.builder());
        waitFor("node1", other.cluster);

        // Give the first cluster the time to re-add node1 (which it shouldn't do, so this is a somewhat arbitrary delay --
        // the value used here made the test consistently fail before the fix was applied).
        TimeUnit.SECONDS.sleep(32);

        assertEquals(countUpHosts(current), 2);
    }

    @Test(groups = "long")
    public void testNodeUp() throws InterruptedException {
        // Same as other method, except we let the driver see the node UP before we stop it.

        CCMBridge ccm = CCMBridge.create("currentCcm", 3);
        current = CCMCluster.create(ccm, Cluster.builder(), 3);
        waitForCountUpHosts(current, 3);

        ccm.stop(1);

        other = CCMCluster.create(1, Cluster.builder());
        waitFor("node1", other.cluster);

        assertEquals(countUpHosts(current), 2);
    }

    @AfterMethod(groups = "long")
    public void teardown() {
        if (current != null)
            current.discard();
        if (other != null)
            other.discard();
    }

    private static int countUpHosts(CCMCluster c) {
        int ups = 0;
        for (Host host : c.cluster.getMetadata().getAllHosts()) {
            if (host.isUp())
                ups += 1;
        }
        return ups;
    }

    private void waitForCountUpHosts(CCMCluster cluster, int count) throws InterruptedException {
        int maxRetries = 10;
        int interval = 5;

        for (int i = 0; i <= maxRetries; i++) {
            if (countUpHosts(cluster) == count)
                return;
            if (i == maxRetries)
                fail(String.format("Up host count didn't reach %d after %d seconds",
                                   count, i * interval));
            TimeUnit.SECONDS.sleep(interval);
        }
    }
}
