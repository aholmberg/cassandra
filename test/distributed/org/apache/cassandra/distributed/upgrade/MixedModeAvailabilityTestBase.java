/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.upgrade;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.exceptions.UnavailableException;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static java.lang.String.format;

public class MixedModeAvailabilityTestBase extends UpgradeTestBase
{
    private static final int NUM_NODES = 3;
    private static final int COORDINATOR = 1;
    private static final List<Tester> TESTERS = Arrays.asList(new Tester(ONE, ALL),
                                                              new Tester(QUORUM, QUORUM),
                                                              new Tester(ALL, ONE));


    protected static void testAvailability(Versions.Major initial, Versions.Major... upgrade) throws Throwable
    {
        testAvailability(true, initial, upgrade);
        testAvailability(false, initial, upgrade);
    }

    private static void testAvailability(boolean upgradedCoordinator,
                                         Versions.Major initial,
                                         Versions.Major... upgrade) throws Throwable
    {
        new TestCase()
        .nodes(NUM_NODES)
        .nodesToUpgrade(upgradedCoordinator ? 1 : 2)
        .upgrade(initial, upgrade)
        .withConfig(config -> config.with(NETWORK, GOSSIP))
        .setup(c -> c.schemaChange(withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c))")))
        .runAfterNodeUpgrade((cluster, n) -> {

            // using 0 to 2 down nodes...
            for (int numNodesDown = 0; numNodesDown < NUM_NODES; numNodesDown++)
            {
                // disable communications to the down nodes
                if (numNodesDown > 0)
                    shutDownWaitForStatus(cluster.get(COORDINATOR), cluster.get(replica(COORDINATOR, numNodesDown)));

                // run the test cases that are compatible with the number of down nodes
                ICoordinator coordinator = cluster.coordinator(COORDINATOR);
                for (Tester tester : TESTERS)
                    tester.test(coordinator, numNodesDown, upgradedCoordinator);
            }
        }).run();
    }

    private static void shutDownWaitForStatus(IInstance observer, IInstance toBeDown) throws Throwable
    {
        InetSocketAddress downNodeAddress = toBeDown.broadcastAddress();
        toBeDown.shutdown().get();
        while (!nodeIsDownInStatusResult(downNodeAddress.getAddress().getHostAddress(), observer.nodetoolResult("status")))
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }

    private static boolean nodeIsDownInStatusResult(String node, NodeToolResult result)
    {
        String[] lines = result.getStdout().split("\n");
        return Arrays.stream(lines).anyMatch(line -> {
            String[] tokens = line.split("\\s+");
            return tokens.length > 1 && tokens[1].equals(node) && tokens[0].equals("DN");
        });
    }

    private static int replica(int node, int depth)
    {
        assert depth >= 0;
        return depth == 0 ? node : replica(node == NUM_NODES ? 1 : node + 1, depth - 1);
    }

    private static class Tester
    {
        private static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
        private static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k = ?");

        private final ConsistencyLevel writeConsistencyLevel;
        private final ConsistencyLevel readConsistencyLevel;

        private Tester(ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
        {
            this.writeConsistencyLevel = writeConsistencyLevel;
            this.readConsistencyLevel = readConsistencyLevel;
        }

        public void test(ICoordinator coordinator, int numNodesDown, boolean upgradedCoordinator)
        {
            UUID key = UUID.randomUUID();
            Object[] row1 = row(key, 1, 10);
            Object[] row2 = row(key, 2, 20);

            boolean wrote = false;
            try
            {
                // test write
                maybeFailUnavailable(numNodesDown > maxNodesDown(writeConsistencyLevel), () -> {
                    coordinator.execute(INSERT, writeConsistencyLevel, row1);
                    coordinator.execute(INSERT, writeConsistencyLevel, row2);
                });

                wrote = true;

                // test read
                maybeFailUnavailable(numNodesDown > maxNodesDown(readConsistencyLevel), () -> {
                    Object[][] rows = coordinator.execute(SELECT, readConsistencyLevel, key);
                    if (numNodesDown <= maxNodesDown(writeConsistencyLevel))
                        assertRows(rows, row1, row2);
                });
            }
            catch (Throwable t)
            {
                throw new AssertionError(format("Unexpected error while %s in case %s-%s with %s coordinator and %d nodes down",
                                                wrote ? "reading" : "writing",
                                                writeConsistencyLevel,
                                                readConsistencyLevel,
                                                upgradedCoordinator ? "upgraded" : "not upgraded",
                                                numNodesDown), t);
            }
        }

        private static void maybeFailUnavailable(boolean shouldFail, Runnable test)
        {
            try
            {
                test.run();
                assertFalse(shouldFail);
            }
            catch (Exception e)
            {
                // we should use exception class names due to the different classpaths
                String className = e.getClass().getCanonicalName();
                if (e instanceof RuntimeException && e.getCause() != null)
                    className = e.getCause().getClass().getCanonicalName();

                if (shouldFail)
                    assertEquals(UnavailableException.class.getCanonicalName(), className);
                else
                    throw e;
            }
        }

        private static int maxNodesDown(ConsistencyLevel cl)
        {
            if (cl == ONE)
                return 2;

            if (cl == QUORUM)
                return 1;

            if (cl == ALL)
                return 0;

            throw new IllegalArgumentException("Usupported consistency level: " + cl);
        }
    }
}
