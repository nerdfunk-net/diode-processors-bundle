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
package net.nerdfunk.nifi.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Hex;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.stream.io.StreamUtils;
import java.io.InputStream;
import org.apache.nifi.components.AllowableValue;
import java.io.BufferedInputStream;
import org.apache.commons.codec.DecoderException;
import java.util.Collection;
import org.apache.nifi.util.NaiveSearchRingBuffer;
import org.apache.nifi.logging.ComponentLog;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.Validator;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;


@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"nerdfunk", "content", "split", "binary"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits json and binary FlowFile by a specified byte sequence")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The FlowFile with its attributes is stored in memory, not the content of the FlowFile. If many splits are generated " +
        "due to the size of the content, or how the content is configured to be split, a two-phase approach may be necessary to avoid excessive use of memory.")

public class SplitJsonAndContent extends AbstractProcessor {

    // attribute keys
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    static final AllowableValue HEX_FORMAT = new AllowableValue("Hexadecimal", "Hexadecimal", "The Byte Sequence will be interpreted as a hexadecimal representation of bytes");
    static final AllowableValue UTF8_FORMAT = new AllowableValue("Text", "Text", "The Byte Sequence will be interpreted as UTF-8 Encoded text");

    static final AllowableValue TRAILING_POSITION = new AllowableValue("Trailing", "Trailing", "Keep the Byte Sequence at the end of the first split if <Keep Byte Sequence> is true");
    static final AllowableValue LEADING_POSITION = new AllowableValue("Leading", "Leading", "Keep the Byte Sequence at the beginning of the second split if <Keep Byte Sequence> is true");

    public static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("Byte Sequence Format")
            .description("Specifies how the <Byte Sequence> property should be interpreted")
            .required(true)
            .allowableValues(HEX_FORMAT, UTF8_FORMAT)
            .defaultValue(UTF8_FORMAT.getValue())
            .build();
    public static final PropertyDescriptor BYTE_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Byte Sequence")
            .description("A representation of bytes to look for and upon which to split the source file into separate files")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("===nerdfunkdelimiter===")
            .required(true)
            .build();
    public static final PropertyDescriptor KEEP_SEQUENCE = new PropertyDescriptor.Builder()
            .name("Keep Byte Sequence")
            .description("Determines whether or not the Byte Sequence should be included with each Split")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor BYTE_SEQUENCE_LOCATION = new PropertyDescriptor.Builder()
            .name("Byte Sequence Location")
            .description("If <Keep Byte Sequence> is set to true, specifies whether the byte sequence should be added to the end of the first "
                    + "split or the beginning of the second; if <Keep Byte Sequence> is false, this property is ignored.")
            .required(true)
            .allowableValues(TRAILING_POSITION, LEADING_POSITION)
            .defaultValue(TRAILING_POSITION.getValue())
            .build();

    public static final Relationship REL_CONTENT = new Relationship.Builder()
            .name("content")
            .description("The content part of the original content file")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original file including the json header")
            .build();
    public static final Relationship REL_JSON = new Relationship.Builder()
            .name("json")
            .description("The json part")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("An error occured (eg. failed json)")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;
    private final AtomicReference<byte[]> byteSequence = new AtomicReference<>();

    // mystuff
    String jsonInput;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_CONTENT);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_JSON);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FORMAT);
        properties.add(BYTE_SEQUENCE);
        properties.add(KEEP_SEQUENCE);
        properties.add(BYTE_SEQUENCE_LOCATION);
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
        final String format = validationContext.getProperty(FORMAT).getValue();
        if (HEX_FORMAT.getValue().equals(format)) {
            final String byteSequence = validationContext.getProperty(BYTE_SEQUENCE).getValue();
            final ValidationResult result = new HexStringPropertyValidator().validate(BYTE_SEQUENCE.getName(), byteSequence, validationContext);
            results.add(result);
        }
        return results;
    }

    @OnScheduled
    public void initializeByteSequence(final ProcessContext context) throws DecoderException {
        final String bytePattern = context.getProperty(BYTE_SEQUENCE).getValue();

        final String format = context.getProperty(FORMAT).getValue();
        if (HEX_FORMAT.getValue().equals(format)) {
            this.byteSequence.set(Hex.decodeHex(bytePattern.toCharArray()));
        } else {
            this.byteSequence.set(bytePattern.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final boolean keepSequence = context.getProperty(KEEP_SEQUENCE).asBoolean();
        final boolean keepTrailingSequence;
        final boolean keepLeadingSequence;
        
        if (keepSequence) {
            if (context.getProperty(BYTE_SEQUENCE_LOCATION).getValue().equals(TRAILING_POSITION.getValue())) {
                keepTrailingSequence = true;
                keepLeadingSequence = false;
            } else {
                keepTrailingSequence = false;
                keepLeadingSequence = true;
            }
        } else {
            keepTrailingSequence = false;
            keepLeadingSequence = false;
        }

        final byte[] byteSequence = this.byteSequence.get();
        if (byteSequence == null) {   // should never happen. But just in case...
            logger.error("{} Unable to obtain Byte Sequence", new Object[]{this});
            session.rollback();
            return;
        }

        final List<Tuple<Long, Long>> splits = new ArrayList<>();

        final NaiveSearchRingBuffer buffer = new NaiveSearchRingBuffer(byteSequence);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                long bytesRead = 0L;
                long startOffset = 0L;

                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    while (true) {
                        final int nextByte = in.read();
                        if (nextByte == -1) {
                            return;
                        }

                        bytesRead++;
                        boolean matched = buffer.addAndCompare((byte) (nextByte & 0xFF));
                        if (matched) {
                            long splitLength;

                            if (keepTrailingSequence) {
                                splitLength = bytesRead - startOffset;
                            } else {
                                splitLength = bytesRead - startOffset - byteSequence.length;
                            }

                            if (keepLeadingSequence && startOffset > 0) {
                                splitLength += byteSequence.length;
                            }

                            final long splitStart = (keepLeadingSequence && startOffset > 0) ? startOffset - byteSequence.length : startOffset;
                            splits.add(new Tuple<>(splitStart, splitLength));
                            startOffset = bytesRead;
                            buffer.clear();
                            // we expect one match only, so let's return
                            return;
                        }
                    }
                }
            }
        });

        long lastOffsetPlusSize = -1L;
        final ArrayList<FlowFile> splitList = new ArrayList<>();

        if (splits.isEmpty()) {
            logger.info("Found no match for. Transfering flow to original");
        } else if (splits.size() == 1) {

            // get json split and convert it to map
            final Tuple<Long, Long> tuple = splits.get(0);
            final long offset = tuple.getKey();
            final long size = tuple.getValue();
            logger.debug("json part offset {} size {} flowfile size {}", new Object[]{offset, size,  flowFile.getSize()});

            FlowFile jsonFile = session.clone(flowFile, offset, size);
            final byte[] jsonbuffer = new byte[(int) jsonFile.getSize()];

            session.read(jsonFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, jsonbuffer);
                }
            });

            String jsonString = new String(jsonbuffer);

            final ObjectMapper mapper = new ObjectMapper();
            Map<String, String> map = new HashMap<String, String>();
            try {
                map = mapper.readValue(jsonString, Map.class);
            } catch (IOException e) {
                logger.error ("could not convert json string to map");
                session.transfer(flowFile, REL_FAILURE);
                session.remove(jsonFile);
                return;
            }

            for (String key : map.keySet()) {
                session.putAttribute(jsonFile, key, String.valueOf(map.get(key)));
            }

            logger.debug("pb2elk put all Attributes to jsonFile");
            session.transfer(jsonFile, REL_JSON);
            logger.info("route jsonFile to REL_JSON");

            // now get the content and route it to REL_CONTENT
            lastOffsetPlusSize = offset + size;
            long finalSplitOffset = lastOffsetPlusSize + byteSequence.length;
            if ( (finalSplitOffset > -1L && finalSplitOffset < flowFile.getSize() ) || (flowFile.getSize() - finalSplitOffset == 0) ) {
                logger.debug("pb2elk content part offset {} size {} flowfile size {}", new Object[]{finalSplitOffset, flowFile.getSize() - finalSplitOffset,  flowFile.getSize()});
                FlowFile contentFile = session.clone(flowFile, finalSplitOffset, flowFile.getSize() - finalSplitOffset);

                for (String key : map.keySet()) {
                    session.putAttribute(contentFile, key, String.valueOf(map.get(key)));
                }

                logger.debug("put all Attributes to contentFile");
                session.transfer(contentFile, REL_CONTENT);
                logger.info("route contentFile to REL_CONTENT");
            }
        }
        else {
            logger.info("Found too many matches for {}", new Object[]{flowFile});
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }

    static class HexStringPropertyValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext validationContext) {
            try {
                Hex.decodeHex(input.toCharArray());
                return new ValidationResult.Builder().valid(true).input(input).subject(subject).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().valid(false).explanation("Not a valid Hex String").input(input).subject(subject).build();
            }
        }
    }
}
