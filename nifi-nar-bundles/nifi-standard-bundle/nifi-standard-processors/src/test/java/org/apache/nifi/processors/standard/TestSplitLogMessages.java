/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.nifi.processors.standard.SplitLogMessages.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.SplitLogMessages.FRAGMENT_ID;

public class TestSplitLogMessages {
    @Test
    public void testWithSingleRegexSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitLogMessages());
        // \S\S\S [ \d]{2} \d\d:\d\d:\d\d
        runner.setProperty(SplitLogMessages.REGEX, "\\S\\S\\S [ \\d]{2} \\d\\d:\\d\\d:\\d\\d");

        String data = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics\n" +
                "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(SplitLogMessages.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitLogMessages.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitLogMessages.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitLogMessages.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        String expectedSplit1 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics";
        String expectedSplit2 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";
        split1.assertContentEquals(expectedSplit1);
        split2.assertContentEquals(expectedSplit2);
    }

    @Test
    public void testWithRegexTwoMessageSplit() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitLogMessages());
        // \S\S\S [ \d]{2} \d\d:\d\d:\d\d
        runner.setProperty(SplitLogMessages.REGEX, "\\S\\S\\S [ \\d]{2} \\d\\d:\\d\\d:\\d\\d");

        String data = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics\n" +
                "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(SplitLogMessages.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitLogMessages.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitLogMessages.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitLogMessages.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        String expectedSplit1 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics";
        String expectedSplit2 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";
        split1.assertContentEquals(expectedSplit1);
        split2.assertContentEquals(expectedSplit2);
    }

    @Test
    public void testWithCarriageReturn() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitLogMessages());
        // \S\S\S [ \d]{2} \d\d:\d\d:\d\d
        runner.setProperty(SplitLogMessages.REGEX, "\\S\\S\\S [ \\d]{2} \\d\\d:\\d\\d:\\d\\d");

        String data = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics\r\n" +
                "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\r\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\r\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(SplitLogMessages.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitLogMessages.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "2");
        runner.assertTransferCount(SplitLogMessages.REL_SPLITS, 2);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitLogMessages.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);
        final MockFlowFile split2 = splits.get(1);

        String expectedSplit1 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics";
        String expectedSplit2 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: Configuration Notice:\r\n" +
                "        ASL Module \"com.apple.AccountPolicyHelper\" claims selected messages.\r\n" +
                "        Those messages may not appear in standard system log files or in the ASL database.";
        split1.assertContentEquals(expectedSplit1);
        split2.assertContentEquals(expectedSplit2);
    }

    @Test
    public void testWithRegexOneMessage() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitLogMessages());
        // \S\S\S [ \d]{2} \d\d:\d\d:\d\d
        runner.setProperty(SplitLogMessages.REGEX, "\\S\\S\\S [ \\d]{2} \\d\\d:\\d\\d:\\d\\d");

        String data = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics\n";

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(SplitLogMessages.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitLogMessages.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(SplitLogMessages.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitLogMessages.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        String expectedSplit1 = "Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics";
        split1.assertContentEquals(expectedSplit1);
    }

    @Test
    public void testWithRegexNoMatch() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new SplitLogMessages());
        // \S\S\S [ \d]{2} \d\d:\d\d:\d\d
        runner.setProperty(SplitLogMessages.REGEX, "\\S\\S\\S [ \\d]{2} \\d\\d:\\d\\d:\\d\\d");

        String data = "Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics\n";

        runner.enqueue(data);
        runner.run();

        runner.assertTransferCount(SplitLogMessages.REL_ORIGINAL, 1);
        runner.getFlowFilesForRelationship(SplitLogMessages.REL_ORIGINAL).get(0).assertAttributeEquals(FRAGMENT_COUNT, "1");
        runner.assertTransferCount(SplitLogMessages.REL_SPLITS, 1);

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(SplitLogMessages.REL_SPLITS);
        final MockFlowFile split1 = splits.get(0);

        String expectedSplit1 = "Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics";
        split1.assertContentEquals(expectedSplit1);
    }
}
