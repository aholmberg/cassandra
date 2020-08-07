/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.serializers;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import javafx.util.Pair;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimestampSerializerTest
{
    public static final long ONE_SECOND = 1000L;
    public static final long ONE_MINUTE = 60 * ONE_SECOND;
    public static final long ONE_HOUR = 60 * ONE_MINUTE;
    public static final long ONE_DAY = 24 * ONE_HOUR;
    // if there is no timezone, the server assumes the timestamp is in the local timezone.
    // This feels problematic since the same query hitting nodes in different regions could store different timestamps.
    // It's 2020 -- too late to change now.
    public static final long BASE_OFFSET = TimestampSerializer.dateStringToTimestamp("1970-01-01");

    @Test
    public void testFormatResults() throws MarshalException, ParseException
    {
        List<Pair<String, Long>> ioPairs = new ArrayList<>(
            Arrays.asList(
                new Pair<String, Long>("1970-01-01 00:00", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01 00:01", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01 01:00", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02 00:00", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02 00:00 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01 00:01+01", ONE_MINUTE - ONE_HOUR),
                new Pair<String, Long>("1970-01-01 01:00+0100", ONE_HOUR - ONE_HOUR),
                new Pair<String, Long>("1970-01-02 00:00+01:00", ONE_DAY - ONE_HOUR),
                new Pair<String, Long>("1970-01-01 01:00-0200", ONE_HOUR + 2 * ONE_HOUR),

                new Pair<String, Long>("1970-01-01 00:00:00", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01 00:00:01", BASE_OFFSET + ONE_SECOND),
                new Pair<String, Long>("1970-01-01 00:01:00", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01 01:00:00", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02 00:00:00", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02 00:00:00 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01 00:01:00+01", ONE_MINUTE - ONE_HOUR),
                new Pair<String, Long>("1970-01-01 01:00:00+0100", ONE_HOUR - ONE_HOUR),
                new Pair<String, Long>("1970-01-02 00:00:00+01:00", ONE_DAY - ONE_HOUR),
                new Pair<String, Long>("1970-01-01 01:00:00-0200", ONE_HOUR + 2 * ONE_HOUR),

                new Pair<String, Long>("1970-01-01 00:00:00.000", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01 00:00:00.000", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01 00:00:01.000", BASE_OFFSET + ONE_SECOND),
                new Pair<String, Long>("1970-01-01 00:01:00.000", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01 01:00:00.000", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02 00:00:00.000", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02 00:00:00.000 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01 00:00:00.100 UTC", 100L),
                new Pair<String, Long>("1970-01-01 00:01:00.001+01", ONE_MINUTE - ONE_HOUR + 1),
                new Pair<String, Long>("1970-01-01 01:00:00.002+0100", ONE_HOUR - ONE_HOUR + 2),
                new Pair<String, Long>("1970-01-02 00:00:00.003+01:00", ONE_DAY - ONE_HOUR + 3),
                new Pair<String, Long>("1970-01-01 01:00:00.004-0200", ONE_HOUR + 2 * ONE_HOUR + 4),

                new Pair<String, Long>("1970-01-01T00:00", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01T00:01", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01T01:00", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02T00:00", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02T00:00 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01T00:01+01", ONE_MINUTE - ONE_HOUR),
                new Pair<String, Long>("1970-01-01T01:00+0100", ONE_HOUR - ONE_HOUR),
                new Pair<String, Long>("1970-01-02T00:00+01:00", ONE_DAY - ONE_HOUR),
                new Pair<String, Long>("1970-01-01T01:00-0200", ONE_HOUR + 2 * ONE_HOUR),

                new Pair<String, Long>("1970-01-01T00:00:00", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01T00:00:01", BASE_OFFSET + ONE_SECOND),
                new Pair<String, Long>("1970-01-01T00:01:00", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01T01:00:00", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02T00:00:00", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02T00:00:00 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01T00:01:00+01", ONE_MINUTE - ONE_HOUR),
                new Pair<String, Long>("1970-01-01T01:00:00+0100", ONE_HOUR - ONE_HOUR),
                new Pair<String, Long>("1970-01-02T00:00:00+01:00", ONE_DAY - ONE_HOUR),
                new Pair<String, Long>("1970-01-01T01:00:00-0200", ONE_HOUR + 2 * ONE_HOUR),

                new Pair<String, Long>("1970-01-01T00:00:00.000", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01T00:00:00.000", BASE_OFFSET),
                new Pair<String, Long>("1970-01-01T00:00:01.000", BASE_OFFSET + ONE_SECOND),
                new Pair<String, Long>("1970-01-01T00:01:00.000", BASE_OFFSET + ONE_MINUTE),
                new Pair<String, Long>("1970-01-01T01:00:00.000", BASE_OFFSET + ONE_HOUR),
                new Pair<String, Long>("1970-01-02T00:00:00.000", BASE_OFFSET + ONE_DAY),
                new Pair<String, Long>("1970-01-02T00:00:00.000 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01T00:00:00.100 UTC", 100L),
                new Pair<String, Long>("1970-01-01T00:01:00.001+01", ONE_MINUTE - ONE_HOUR + 1),
                new Pair<String, Long>("1970-01-01T01:00:00.002+0100", ONE_HOUR - ONE_HOUR + 2),
                new Pair<String, Long>("1970-01-02T00:00:00.003+01:00", ONE_DAY - ONE_HOUR + 3),
                new Pair<String, Long>("1970-01-01T01:00:00.004-0200", ONE_HOUR + 2 * ONE_HOUR + 4),

                new Pair<String, Long>("1970-01-01", BASE_OFFSET),
                new Pair<String, Long>("1970-01-02 UTC", ONE_DAY),
                new Pair<String, Long>("1970-01-01+01", -ONE_HOUR),
                new Pair<String, Long>("1970-01-01+0100", -ONE_HOUR),
                new Pair<String, Long>("1970-01-02+01:00", ONE_DAY - ONE_HOUR),
                new Pair<String, Long>("1970-01-01-0200", 2 * ONE_HOUR)
            )
        );

        validateStringTimestampPairs(ioPairs);
    }

    @Test
    public void testGeneralTimeZoneFormats()
    {
        // Central Standard Time; CST, GMT-06:00
        final long cstOffset = 6 * ONE_HOUR;
        List<Pair<String, Long>> ioPairs = new ArrayList<>(
            Arrays.asList(
                new Pair<String, Long>("1970-01-01 00:00:00 Central Standard Time", cstOffset),
                new Pair<String, Long>("1970-01-01 00:00:00 CST", cstOffset),
                new Pair<String, Long>("1970-01-01T00:00:00 GMT-06:00", cstOffset)
            )
        );
        validateStringTimestampPairs(ioPairs);
    }

    @Ignore("CASSANDRA-15976 and other incongruities")
    @Test
    public void testVaryingFractionalPrecision()
    {
        List<Pair<String, Long>> ioPairs = new ArrayList<>(
            Arrays.asList(
                new Pair<String, Long>("1970-01-01 00:00:00.10 UTC", 100L), // CASSANDRA-15976
                new Pair<String, Long>("1970-01-01 00:00:00.1 UTC", 100L),  //  ^
                new Pair<String, Long>("1970-01-01T00:00:00.10 UTC", 100L), // CASSANDRA-15976
                new Pair<String, Long>("1970-01-01T00:00:00.1 UTC", 100L),
                // Decimal fractions may be added to any of the three time elements. However, a fraction may only be added to the lowest order time element in the representation.
                new Pair<String, Long>("1970-01-01T00:0.5 UTC", 30000L),
                // There is no limit on the number of decimal places for the decimal fraction. However, the number of decimal places needs to be agreed to by the communicating parties. For example, in Microsoft SQL Server, the precision of a decimal fraction is 3, i.e., "yyyy-mm-ddThh:mm:ss[.mmm]".[27]
                new Pair<String, Long>("1970-01-01T00:00:00.5999 UTC", 600L)

            )
        );
        validateStringTimestampPairs(ioPairs);
    }

    @Test
    public void testInvalidTimezones()
    {
        List<String> timestamps = new ArrayList<String>(
            Arrays.asList(
                "1970-01-01 00:00:00 xentral Standard Time",
                "1970-01-01 00:00:00-060",
                "1970-01-01T00:00:00 GMT-14:00"
            )
        );
        expectStringTimestampsExcept(timestamps);
    }

    @Test
    public void testInvalidFormat()
    {
        List<String> timestamps = new ArrayList<String>(
            Arrays.asList(
                "1970-01-01 00:00:00andsomeextra",
                "prefix1970-01-01 00:00:00",
                "1970-01-01 00.56",
                "1970-01-0100:00:00"
            )
        );
        expectStringTimestampsExcept(timestamps);
    }

    @Test
    public void testNumeric()
    {
        // now (positive
        final long now = System.currentTimeMillis();
        assertEquals(now, TimestampSerializer.dateStringToTimestamp(Long.toString(now)));

        // negative
        final long then = -11234L;
        assertEquals(then, TimestampSerializer.dateStringToTimestamp(Long.toString(then)));

        // overflows
        for (Long l: Arrays.asList(Long.MAX_VALUE, Long.MIN_VALUE))
        {
            try
            {
                TimestampSerializer.dateStringToTimestamp(Long.toString(l) + "1");
                fail("Expected exception was not raised while parsing overflowed long.");
            }
            catch (MarshalException e)
            {
                continue;
            }
        }
    }

    @Test
    public void testNow()
    {
        final long threshold = 5;
        final long now = System.currentTimeMillis();
        final long parsed = TimestampSerializer.dateStringToTimestamp("now");
        assertTrue("'now' timestamp not within expected tolerance.", now <= parsed && parsed <= now + threshold);
    }

    private void validateStringTimestampPairs(List<Pair<String,Long>> ioPairs)
    {
        for (Pair<String,Long> p: ioPairs)
        {
            try
            {
                long ts = TimestampSerializer.dateStringToTimestamp(p.getKey());
                assertEquals( "Failed to parse expected timestamp value from " + p.getKey(), (long)p.getValue(), ts);
            }
            catch (MarshalException e)
            {
                fail(String.format("Failed to parse \"%s\" as timestamp.", p.getKey()));
            }
        }
    }

    private void expectStringTimestampsExcept(List<String> timeStrings)
    {
        for (String s: timeStrings)
        {
            try
            {
                TimestampSerializer.dateStringToTimestamp(s);
                fail(String.format("Parsing succeeded while expecting failure from \"%s\"", s));
            }
            catch(MarshalException e)
            {
                continue;
            }

        }
    }
}
