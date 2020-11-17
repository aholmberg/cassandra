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
package org.apache.cassandra.service.reads;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.ReplicaPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReadCallback<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>> implements RequestCallback<ReadResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCallback.class);

    public final ResponseResolver resolver;
    final SimpleCondition condition = new SimpleCondition();
    private final long queryStartNanoTime;
    // this uses a plain reference, but is initialised before handoff to any other threads; the later updates
    // may not be visible to the threads immediately, but ReplicaPlan only contains final fields, so they will never see an uninitialised object
    final ReplicaPlan.Shared<E, P> replicaPlan;
    private final ReadCommand command;

    private volatile int received = 0;
    private volatile int failed = 0;
    private int expectedResponses;
    private int requiredResponses;

    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;

    public ReadCallback(ResponseResolver resolver, ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        this.command = command;
        this.resolver = resolver;
        this.queryStartNanoTime = queryStartNanoTime;
        this.replicaPlan = replicaPlan;
        this.expectedResponses = replicaPlan.get().blockFor();
        this.requiredResponses = expectedResponses;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        // we don't support read repair (or rapid read protection) for range scans yet (CASSANDRA-6897)
        assert !(command instanceof PartitionRangeReadCommand) || expectedResponses >= replicaPlan().contacts().size();

        if (logger.isTraceEnabled())
            logger.trace("ReadCallback expecting {} responses; setup for requests to {}", expectedResponses, this.replicaPlan);
    }

    protected P replicaPlan()
    {
        return replicaPlan.get();
    }

    public boolean await(long timePastStart, TimeUnit unit)
    {
        long time = unit.toNanos(timePastStart) - (System.nanoTime() - queryStartNanoTime);
        try
        {
            return condition.await(time, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    public void awaitResults() throws ReadFailureException, ReadTimeoutException
    {
        boolean signaled = await(command.getTimeout(MILLISECONDS), TimeUnit.MILLISECONDS);
        int clRequiredResponses = replicaPlan.get().blockFor();
        boolean failed = this.failed > 0 && resolver.responses.size() < clRequiredResponses;
        if (signaled && !failed)
        {
            assert resolver.isDataPresent();
            return;
        }

        if (Tracing.isTracing())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            Tracing.trace("{}; received {} of {} responses{}", new Object[]{ (failed ? "Failed" : "Timed out"), received, clRequiredResponses, gotData });
        }
        else if (logger.isDebugEnabled())
        {
            String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
            logger.debug("{}; received {} of {} responses{}", new Object[]{ (failed ? "Failed" : "Timed out"), received, clRequiredResponses, gotData });
        }

        // Same as for writes, see AbstractWriteResponseHandler
        throw failed
              ? new ReadFailureException(replicaPlan().consistencyLevel(), received, clRequiredResponses, resolver.isDataPresent(), failureReasonByEndpoint)
              : new ReadTimeoutException(replicaPlan().consistencyLevel(), received, clRequiredResponses, resolver.isDataPresent());
    }

    public void onResponse(Message<ReadResponse> message)
    {
        resolver.preprocess(message);
        incrementRecievedMaybeSignal(waitingFor(message.from()));
    }

    private synchronized void incrementRecievedMaybeSignal(boolean countsForBlocking)
    {
        if (countsForBlocking)
            ++received;
        maybeSignal();
    }

    /**
     * @return true if the message counts towards the blockFor threshold
     */
    private boolean waitingFor(InetAddressAndPort from)
    {
        return !replicaPlan().consistencyLevel().isDatacenterLocal() || DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from));
    }

    public void response(ReadResponse result)
    {
        Verb kind = command.isRangeRequest() ? Verb.RANGE_RSP : Verb.READ_RSP;
        Message<ReadResponse> message = Message.internalResponse(kind, result);
        onResponse(message);
    }

    @Override
    public boolean trackLatencyForSnitch()
    {
        return true;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        failureReasonByEndpoint.put(from, failureReason);
        incrementFailedMaybeSignal(waitingFor(from));
    }

    private synchronized void incrementFailedMaybeSignal(boolean countsForBlocking)
    {
        if (countsForBlocking)
            ++failed;
        maybeSignal();
    }

    private void maybeSignal()
    {
        if ((received >= requiredResponses && resolver.isDataPresent()) ||
            (received + failed == expectedResponses))
        {
            condition.signalAll();
        }
    }

    public synchronized boolean shouldSendSpecExec()
    {
        if (!condition.isSignaled())
        {
            ++expectedResponses;
            return true;
        }
        return false;
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }
}
