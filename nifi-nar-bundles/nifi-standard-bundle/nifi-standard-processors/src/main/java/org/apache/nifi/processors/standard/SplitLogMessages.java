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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.NaiveSearchRingBuffer;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"log", "split", "text", "regex"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits incoming FlowFiles comprising concatenated log messages into individual log messages. " +
        "The Regular Expression property is used to identify a new line. Any newline or carriage return immediately" +
        "preceeding the matched regular expression is removed. Embedded newlines are preserved.")
@WritesAttributes({
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")})
public class SplitLogMessages extends AbstractProcessor {

    // attribute keys
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("Regular Expression")
            .description("Java Regular expression defining the pattern used to start a new line")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(null)
            .build();

    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("All Splits will be routed to the splits relationship")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original file")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<byte[]> byteSequence = new AtomicReference<>();

    // Not sure if AtomicReference is necessary. But following the pattern
    private final AtomicReference<Pattern> regexPattern = new AtomicReference<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SPLITS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REGEX);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        return results;
    }

    @OnScheduled
    public void initializeRegex(final ProcessContext context) throws DecoderException {
        final String regex = context.getProperty(REGEX).getValue();
        if (regex != null) {
            final Pattern pattern = Pattern.compile(regex);
            this.regexPattern.set(pattern);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final List<Tuple<Long, Long>> splits = new ArrayList<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                long bytesRead = 0L;
                long startOffset = 0L;

                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    final String regex = context.getProperty(REGEX).getValue();
                    if (regex != null) {
                        // inefficient: allocate new array every time, copy to array, then copy to string
                        // assume a maximum line size of 1024
                        final InputStreamReader reader = new InputStreamReader(in);
                        final char[] buf = new char[1024];
                        final int numRead = reader.read(buf, 0, 1024);
                        final Pattern p = regexPattern.get();
                        final String s = String.valueOf(buf, 0, numRead);
                        final Matcher m = p.matcher(s);
                        int patternLength = 0;
                        while (true) {
                            boolean found = false;
                            long splitLength;
                            if (m.find()) {
                                found = true;
                                patternLength = m.group().length();
                                bytesRead = m.end();
                                splitLength = bytesRead - startOffset - patternLength;
                            } else {
                                bytesRead = s.length();
                                splitLength = bytesRead - startOffset;
                            }
                            if (startOffset > 0) {
                                splitLength += patternLength;
                            }
                            final long splitStart = (startOffset > 0) ? startOffset - patternLength : startOffset;
                            int lastCharOffset = (int) splitStart + (int) splitLength - 1;
                            if (lastCharOffset >= 0) {
                                switch (buf[lastCharOffset]) {
                                    case '\n':
                                        splitLength--;
                                        if (lastCharOffset > 0 && buf[--lastCharOffset] == '\r') {
                                            splitLength--;
                                        }
                                        break;
                                }
                            }
                            splits.add(new Tuple<>(splitStart, splitLength));
                            startOffset = bytesRead;
                            if (!found) {
                                return;
                            }
                        }
                    }
                }
            }
        });

        if (splits.isEmpty()) {
            FlowFile clone = session.clone(flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(clone, REL_SPLITS);
            logger.info("Found no match for {}; transferring original 'original' and transferring clone {} to 'splits'", new Object[]{flowFile, clone});
            return;
        }

        final ArrayList<FlowFile> splitList = new ArrayList<>();
        for (final Tuple<Long, Long> tuple : splits) {
            long offset = tuple.getKey();
            long size = tuple.getValue();
            if (size > 0) {
                FlowFile split = session.clone(flowFile, offset, size);
                splitList.add(split);
            }
        }

        final String fragmentId = finishFragmentAttributes(session, flowFile, splitList);
        session.transfer(splitList, REL_SPLITS);
        flowFile = FragmentAttributes.copyAttributesToOriginal(session, flowFile, fragmentId, splitList.size());
        session.transfer(flowFile, REL_ORIGINAL);

        if (splitList.size() > 10) {
            logger.info("Split {} into {} files", new Object[]{flowFile, splitList.size()});
        } else {
            logger.info("Split {} into {} files: {}", new Object[]{flowFile, splitList.size(), splitList});
        }
    }

    /**
     * Apply split index, count and other attributes.
     *
     * @param session session
     * @param source source
     * @param splits splits
     * @return generated fragment identifier for the splits
     */
    private String finishFragmentAttributes(final ProcessSession session, final FlowFile source, final List<FlowFile> splits) {
        final String originalFilename = source.getAttribute(CoreAttributes.FILENAME.key());

        final String fragmentId = UUID.randomUUID().toString();
        final ArrayList<FlowFile> newList = new ArrayList<>(splits);
        splits.clear();
        for (int i = 1; i <= newList.size(); i++) {
            FlowFile ff = newList.get(i - 1);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, String.valueOf(i));
            attributes.put(FRAGMENT_COUNT, String.valueOf(newList.size()));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFilename);
            FlowFile newFF = session.putAllAttributes(ff, attributes);
            splits.add(newFF);
        }
        return fragmentId;
    }
}
