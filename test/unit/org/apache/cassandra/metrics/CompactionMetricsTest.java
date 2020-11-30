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

package org.apache.cassandra.metrics;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionMetricsTest extends CQLTester
{
    private static final CompactionMetrics compactionMetrics = CompactionManager.instance.getMetrics();
    private static final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    private static final String metricsPath = "org.apache.cassandra.metrics:type=Compaction";

    private <T> void validateMBean(String name, String attribute, Class<T> c) throws Throwable
    {
        ObjectName objectName = new ObjectName(String.format("%s,name=%s", metricsPath, name));
        T value = c.cast(mBeanServer.getAttribute(objectName, attribute));
        assertNotNull(value);
    }

    @Test
    public void testMetricLocationsAndTypes() throws Throwable
    {
        validateMBean("PendingTasks", "Value", Integer.class);
        validateMBean("PendingTasksByTableName", "Value", Map.class);
        validateMBean("CompletedTasks", "Value", Long.class);
        validateMBean("TotalCompactionsCompleted", "Count", Long.class);
        validateMBean("BytesCompacted", "Count", Long.class);
        validateMBean("CompactionsReduced", "Count", Long.class);
        validateMBean("SSTablesDroppedFromCompaction", "Count", Long.class);
        validateMBean("CompactionsAborted", "Count", Long.class);

        // Detect new metrics that are not characterized
        Set<String> known = new HashSet<>(Arrays.asList("PendingTasks",
                                                        "PendingTasksByTableName",
                                                        "CompletedTasks",
                                                        "TotalCompactionsCompleted",
                                                        "BytesCompacted",
                                                        "CompactionsReduced",
                                                        "SSTablesDroppedFromCompaction",
                                                        "CompactionsAborted"));
        for (ObjectInstance o: mBeanServer.queryMBeans(new ObjectName(String.format("%s,*", metricsPath)), null))
        {
            String name = o.getObjectName().getKeyProperty("name");
            if (!known.remove(name))
            {
                fail(String.format("New compaction metric is not known or tested: %s", name));
            }
        }
        assertTrue(String.format("Previously known MBean(s) not found: %s", known), known.isEmpty());
    }

    @Test
    public void testCompletionAndPending() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = " +
                    "{'class': 'org.apache.cassandra.metrics.CompactionMetricsTest$TestCompactionStrategy', " +
                    "'task_action': 'stall'}");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");

        flush();

        final long initialCompletedTasks = compactionMetrics.completedTasks.getValue();
        final long initialTotalCompactionsCompleted = compactionMetrics.totalCompactionsCompleted.getCount();
        final long initialBytesCompacted = compactionMetrics.bytesCompacted.getCount();
        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
        assertFalse(compactionMetrics.pendingTasksByTableName.getValue().containsKey(KEYSPACE));

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<Future<?>> futures =
        CompactionManager.instance.submitMaximal(cfs,
                                                 CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()),
                                                 false);
        StallingCompactionWriter.waitForStart();

        assertEquals(1, (long)compactionMetrics.pendingTasks.getValue());
        assertEquals(1, (long)compactionMetrics.pendingTasksByTableName.getValue().get(KEYSPACE).get(currentTable()));

        StallingCompactionWriter.releaseFinish();
        FBUtilities.waitOnFutures(futures);

        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
        assertFalse(compactionMetrics.pendingTasksByTableName.getValue().containsKey(KEYSPACE));
        assertEquals(initialTotalCompactionsCompleted + 1, compactionMetrics.totalCompactionsCompleted.getCount());
        assertTrue(compactionMetrics.bytesCompacted.getCount() > initialBytesCompacted);
        assertTrue(initialCompletedTasks < compactionMetrics.completedTasks.getValue());
    }

    @Test
    public void testCompactionsAborted() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) values (1, 1)");
        flush();

        for(File dir : getCurrentColumnFamilyStore().getDirectories().getCFDirectories()){
            DisallowedDirectories.maybeMarkUnwritable(dir);
        }

        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
        try
        {
            compact();
            fail("Did not receive expected exception");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage().contains("Not enough space for compaction"));
        }
        assertEquals(1, compactionMetrics.compactionsAborted.getCount());
        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
    }

    @Test
    public void testCompactionsReducedDropped () throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = " +
                    "{'class': 'org.apache.cassandra.metrics.CompactionMetricsTest$TestCompactionStrategy'}");

        // Three SS tables on disk
        for (int i = 0; i < 3; ++i)
        {
            execute("INSERT INTO %s (k, v) values (?, ?)", i, i);
            flush();
        }
        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
        assertEquals(0, compactionMetrics.compactionsReduced.getCount());
        assertEquals(0, compactionMetrics.sstablesDropppedFromCompactions.getCount());

        // SizeLimitingCommpactionTask will lie about disk space for two of the three
        compact();

        assertEquals(1, compactionMetrics.compactionsReduced.getCount());
        assertEquals(2, compactionMetrics.sstablesDropppedFromCompactions.getCount());
        assertEquals(0, (long)compactionMetrics.pendingTasks.getValue());
    }

    public static class TestCompactionStrategy extends SizeTieredCompactionStrategy
    {
        static final String TASK_ACTION_KEY = "task_action";

        public TestCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
        {
            super(cfs, options);

        }

        public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
        {
            Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
            if (Iterables.isEmpty(filteredSSTables))
                return null;
            LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
            return Arrays.<AbstractCompactionTask>asList(makeTask(txn, gcBefore));
        }

        private CompactionTask makeTask(LifecycleTransaction txn, int gcBefore)
        {
            if ("stall".equals(options.get(TASK_ACTION_KEY)))
            {
                return new StallingCompactionTask(cfs, txn, gcBefore);
            }
            else
            {
                return new SpaceLimitingCompactionTask(cfs, txn, gcBefore);
            }
        }

        public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
        {
            Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
            uncheckedOptions.remove(TASK_ACTION_KEY);
            return uncheckedOptions;
        }
    }

    private static class StallingCompactionTask extends CompactionTask
    {
        public StallingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
        {
            super(cfs, txn, gcBefore);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction transaction,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new StallingCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
        }
    }

    private static class StallingCompactionWriter extends DefaultCompactionWriter
    {
        public static final CountDownLatch start = new CountDownLatch(1);
        public static final CountDownLatch finish = new CountDownLatch(1);

        static void waitForStart()
        {
            try
            {
                start.await();
            }
            catch (InterruptedException e) {}
        }

        static void releaseFinish()
        {
            finish.countDown();
        }

        public StallingCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals, int sstableLevel)
        {
            super(cfs, directories, txn, nonExpiredSSTables, keepOriginals, sstableLevel);
            start.countDown();
        }

        @Override
        public Collection<SSTableReader> finish()
        {
            try
            {
                finish.await();
            }
            catch (InterruptedException e) {}
            return super.finish();
        }
    }

    private static class SpaceLimitingCompactionTask extends CompactionTask
    {
        public SpaceLimitingCompactionTask(ColumnFamilyStore cfStore, LifecycleTransaction txn, int gcBefore)
        {
            super(spy(cfStore), txn, gcBefore);
            Directories dirSpy = spy(cfs.getDirectories());
            doReturn(dirSpy).when(cfs).getDirectories();
            // fail the first two calls, succeed thereafter
            doReturn(false)
            .doReturn(false)
            .doCallRealMethod()
            .when(dirSpy)
            .hasAvailableDiskSpace(anyLong(), anyLong());
        }
    }

}