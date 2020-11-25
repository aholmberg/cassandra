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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionMetricsTest extends CQLTester
{
    //TODO: remove one of these?
    private static final CompactionManager compactionManager = CompactionManager.instance;
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
                                                        "CompactionsAborted", "x"));
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
    public void testSimpleCompactionMetricsForCompletedTasks() throws Throwable
    {
        final long initialCompletedTasks = compactionManager.getCompletedTasks();
        assertEquals(0L, compactionManager.getTotalCompactionsCompleted());
        assertEquals(0L, compactionManager.getTotalBytesCompacted());

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
        }

        getCurrentColumnFamilyStore().forceBlockingFlush();
        getCurrentColumnFamilyStore().forceMajorCompaction();

        assertEquals(1L, compactionManager.getTotalCompactionsCompleted());
        assertTrue(compactionManager.getTotalBytesCompacted() > 0L);
        assertTrue(initialCompletedTasks < compactionManager.getCompletedTasks());
    }

    @Test
    public void testCompactionMetricsForPendingTasks() throws Throwable
    {
        //TODO: make this a single test with completionss
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compaction = {'class': 'org.apache.cassandra.metrics.CompactionMetricsTest$TestCompactionStrategy'}");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 10; ++i)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush();
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
    }

    public static class TestCompactionStrategy extends SizeTieredCompactionStrategy
    {
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
            return Arrays.<AbstractCompactionTask>asList(new StallingCompactionTask(cfs, txn, gcBefore));
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


    @Test
    public void testCompactionMetricsForCompactionsAborted() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t text, primary key (id, id2))");
        execute("insert into %s (id, id2) values (1, 1)");
        flush();

        for(File dir : getCurrentColumnFamilyStore().getDirectories().getCFDirectories()){
            DisallowedDirectories.maybeMarkUnwritable(dir);
        }

        try
        {
            getCurrentColumnFamilyStore().forceMajorCompaction();
        }
        catch (Exception e){
            logger.info("Exception will be thrown when compacting with unwritable directories.");
        }
        assertTrue(compactionManager.getMetrics().compactionsAborted.getCount() > 0);
    }

    @Test
    public void testCompactionMetricsForDroppedSSTables () throws Throwable
    {
        // assertTrue(compactionManager.getMetrics().compactionsReduced.getCount() > 0);
        // assertTrue(compactionManager.getMetrics().sstablesDropppedFromCompactions.getCount() > 0);
    }
}