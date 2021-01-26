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

package org.apache.cassandra.tools.nodetool;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class RingTest extends CQLTester
{
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
        token = StorageService.instance.getTokens().get(0);
        startJMXServer();
    }

    /**
     * Validate output, making sure the table mappings work with various host-modifying arguments in use.
     */
    @Test
    public void testRingOutput()
    {
        HostStatWithPort host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(), false, null);
        validateRingOutput(host.ipOrDns(false),
                            "ring");
        validateRingOutput(host.ipOrDns(true),
                            "-pp", "ring");
        host = new HostStatWithPort(null, FBUtilities.getBroadcastAddressAndPort(), true, null);
        validateRingOutput(host.ipOrDns(false),
                            "ring", "-r");
        validateRingOutput(host.ipOrDns(true),
                            "-pp", "ring", "-r");
    }

    private void validateRingOutput(String hostForm, String... args)
    {
        ToolRunner.ToolResult nodetool = ToolRunner.invokeNodetool(args);
        nodetool.assertOnCleanExit();
        /**

         Datacenter: datacenter1
         ==========
         Address         Rack        Status State   Load            Owns                Token

         127.0.0.1       rack1       Up     Normal  45.71 KiB       100.00%             4652409154190094022

         */
        String[] lines = nodetool.getStdout().split("\\R");
        assertThat(lines[1].trim(), endsWith(SimpleSnitch.DATA_CENTER_NAME));
        String hostRing = lines[lines.length-4].trim(); // this command has a couple extra newlines and an empty error message at the end. Not messing with it.
        assertThat(hostRing, startsWith(hostForm));
        assertThat(hostRing, containsString(SimpleSnitch.RACK_NAME));
        assertThat(hostRing, containsString("Up"));
        assertThat(hostRing, containsString("Normal"));
        assertThat(hostRing, matchesPattern(".*\\d+\\.\\d+ KiB.*"));
        assertThat(hostRing, matchesPattern(".*\\d+\\.\\d+%.*"));
        assertThat(hostRing, endsWith(token));
        assertThat(hostRing, not(containsString("?")));
    }
}
