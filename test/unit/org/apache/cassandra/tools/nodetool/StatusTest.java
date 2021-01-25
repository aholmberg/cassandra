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

import javax.xml.crypto.Data;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StatusTest extends CQLTester
{
    private static String localHostId;
    private static String token;

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
        localHostId = StorageService.instance.getLocalHostId();
        token = StorageService.instance.getTokens().get(0);
        startJMXServer();
    }

    /**
     * Validate output, making sure the table mappings work with various host-modifying arguments in use.
     */
    @Test
    public void testStatusOutput()
    {
        validateSatusOutput("127.0.0.1",
                            "status");
        validateSatusOutput("localhost",
                            "status", "-r");
        validateSatusOutput("localhost:7012",
                            "-pp", "status", "-r");
        validateSatusOutput("127.0.0.1:7012",
                            "-pp", "status");
    }

    private void validateSatusOutput(String hostForm, String... args)
    {
        ToolRunner.ToolResult nodetool = ToolRunner.invokeNodetool(args);
        nodetool.assertOnCleanExit();
        /**
         * Datacenter: datacenter1
         * =======================
         * Status=Up/Down
         * |/ State=Normal/Leaving/Joining/Moving
         * --  Address    Load       Owns (effective)  Host ID                               Token                Rack
         * UN  localhost  45.71 KiB  100.0%            0b1b5e91-ad3b-444e-9c24-50578486978a  1849950853373272258  rack1
         */
        String[] lines = nodetool.getStdout().split("\\R");
        assertThat(lines[0].trim(), endsWith(SimpleSnitch.DATA_CENTER_NAME));
        String hostStatus = lines[lines.length-1].trim();
        assertThat(hostStatus, startsWith("UN"));
        assertThat(hostStatus, containsString(hostForm));
        assertThat(hostStatus, matchesPattern(".*\\d+\\.\\d+ KiB.*"));//TODO regex for number
        assertThat(hostStatus, matchesPattern(".*\\d+\\.\\d+%.*"));
        assertThat(hostStatus, endsWith(SimpleSnitch.RACK_NAME));
        assertThat(hostStatus, not(containsString("?")));
    }
}
