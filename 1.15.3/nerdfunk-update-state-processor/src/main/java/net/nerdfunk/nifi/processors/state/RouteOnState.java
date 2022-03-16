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
package net.nerdfunk.nifi.processors.state;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@Tags({"route", "state", "cache"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Gets value of distributed cache and route flowfile depending of the value")
@WritesAttributes({
    @WritesAttribute(attribute = "RouteOnState.State", description = "The value gathered from the map cache"),
    @WritesAttribute(attribute = "RouteOnState.Route", description = "The relation to which the FlowFile was routed")
})
@SeeAlso(classNames = {"net.nerdfunk.nifi.processors.state.UpdateState"})
@DynamicProperty(name = "Relationship Name", value = "Attribute Expression Language or String",
                 expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, description = "Routes FlowFiles whose attributes match the "
                 + "Attribute Expression Language specified in the Dynamic Property Value to the Relationship specified in the Dynamic Property Key")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Attribute Expression Language")
public class RouteOnState extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_VALUE = "RouteOnState.State";
    public static final String ROUTE_ATTRIBUTE_ROUTE = "RouteOnState.Route";

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("The Controller Service that is used to cache flow files")
        .required(true)
        .identifiesControllerService(DistributedMapCacheClient.class)
        .build();

    // Selects the FlowFile attribute, whose value is used as cache key
    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
        .name("Cache Entry Identifier")
        .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will " +
            "be evaluated against a FlowFile in order to determine the cache key")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    private static final String attributeIsExpression = "AttributeValue is Expression";
    private static final String attributeIsString = "AttributeValue is String";
    public static final AllowableValue TREAT_AS_EXPRESSIONLANG = new AllowableValue(attributeIsExpression,
            "Attribute is treated as expression language",
            "The Attribute Value is treated as expression language (bool)");
    public static final AllowableValue TREAT_AS_STRING = new AllowableValue(attributeIsString,
            "Attribute is treated as String",
            "The Attribute Value is treated as String and compared literally");
    public static final PropertyDescriptor COMPARING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Comparing Strategy")
            .description("Specifies how to determine how to treat the Attribute")
            .required(true)
            .allowableValues(TREAT_AS_EXPRESSIONLANG, TREAT_AS_STRING)
            .defaultValue(TREAT_AS_EXPRESSIONLANG.getValue())
            .build();

    public static final PropertyDescriptor PROP_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the cached value is encoded. This will only be used when routing to an attribute.")
            .required(false)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor CACHE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Cache Timeout")
            .description("Time in seconds after the cached value is refreshed. 0 means get the value for each flowfile.")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("60")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not-found")
            .description("If there is no Property that matches the state")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFile is routed here if a failure (eg. problems getting the state) occures")
        .build();

    private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private final List<PropertyDescriptor> properties;
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();
    private volatile String comparingStrategy = COMPARING_STRATEGY.getDefaultValue();

    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

    /**
     * Cache of the state used in getCacheValue
     */
    private String cached_cache_value = null;
    private long last_check = 0;
    
    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<byte[]> valueSerializer = new CacheValueSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();

    public RouteOnState() {
        final Set<Relationship> set = new HashSet<>();
        set.add(REL_NOT_FOUND);
        set.add(REL_FAILURE);
        relationships = new AtomicReference<>(set);
        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CACHE_ENTRY_IDENTIFIER);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(COMPARING_STRATEGY);
        properties.add(CACHE_TIMEOUT);
        properties.add(PROP_CHARACTER_SET);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                //.addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.BOOLEAN, false))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {

        final ComponentLog logger = getLogger();
        logger.info("got new property: " + descriptor.getName() + " " + oldValue + " " + newValue);

        if (descriptor.equals(COMPARING_STRATEGY)) {
            comparingStrategy = newValue;
        }
        else if (descriptor.equals(DISTRIBUTED_CACHE_SERVICE)) {
            // nothing to do
        } else if (descriptor.equals(CACHE_ENTRY_IDENTIFIER)) {
            // nothing to do
        } else if (descriptor.equals(CACHE_TIMEOUT)) {
            // nothing to do
        } else if (descriptor.equals(PROP_CHARACTER_SET)) {
            // nothing to do
        } else {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        for (final String propName : allDynamicProps) {
            logger.info("new relationship " + propName);
            newRelationships.add(new Relationship.Builder().name(propName).build());
        }

        newRelationships.add(REL_NOT_FOUND);
        this.relationships.set(newRelationships);
    }

    /**
     * When this processor is scheduled, update the dynamic properties into the map
     * for quick access during each onTrigger call
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", new Object[]{descriptor});
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String state_value = getCacheValue(context, flowFile, session);

        if (state_value == null) {
            return;
        }

        // write state value to property
        flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_VALUE, state_value);

        // now check attribute value
        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        final Set<Relationship> destinationRelationships = new HashSet<>();
        for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {
            final PropertyValue value = entry.getValue();

            switch (context.getProperty(COMPARING_STRATEGY).getValue()) {
                case attributeIsString:
                    final String prop_value = value.evaluateAttributeExpressions(flowFile).getValue().toString();
                    if (new String(state_value).equals(prop_value)) {
                        destinationRelationships.add(entry.getKey());
                    }
                    break;
                case attributeIsExpression:
                default:
                    final Boolean prop_value_bool = value.evaluateAttributeExpressions(flowFile).asBoolean();
                    if (prop_value_bool) {
                        destinationRelationships.add(entry.getKey());
                    }
                    break;
            }
        }

        if (destinationRelationships.isEmpty()) {
            logger.info("Routing {} to unmatched", new Object[]{ flowFile });
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_ROUTE, REL_NOT_FOUND.getName());
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            session.transfer(flowFile, REL_NOT_FOUND);
        } else {
            final Iterator<Relationship> relationshipNameIterator = destinationRelationships.iterator();
            final Relationship firstRelationship = relationshipNameIterator.next();
            final Map<Relationship, FlowFile> transferMap = new HashMap<>();
            final Set<FlowFile> clones = new HashSet<>();

            // make all the clones for any remaining relationships
            while (relationshipNameIterator.hasNext()) {
                final Relationship relationship = relationshipNameIterator.next();
                final FlowFile cloneFlowFile = session.clone(flowFile);
                clones.add(cloneFlowFile);
                transferMap.put(relationship, cloneFlowFile);
            }

            // now transfer any clones generated
            for (final Map.Entry<Relationship, FlowFile> entry : transferMap.entrySet()) {
                logger.info("Cloned {} into {} and routing clone to relationship {}", new Object[]{ flowFile, entry.getValue(), entry.getKey() });
                FlowFile updatedFlowFile = session.putAttribute(entry.getValue(), ROUTE_ATTRIBUTE_ROUTE, entry.getKey().getName());
                session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
                session.transfer(updatedFlowFile, entry.getKey());
            }

            //now transfer the original flow file
            logger.info("Routing {} to {}", new Object[]{flowFile, firstRelationship});
            session.getProvenanceReporter().route(flowFile, firstRelationship);
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_ROUTE, firstRelationship.getName());
            session.transfer(flowFile, firstRelationship);
        }
    }

    private String getCacheValue(final ProcessContext context, FlowFile flowFile, final ProcessSession session) {

        final ComponentLog logger = getLogger();
        String state_value = null;

        // cache key is computed from attribute 'CACHE_ENTRY_IDENTIFIER' with expression language support
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final long timeout = context.getProperty(CACHE_TIMEOUT).asLong() * 1000;
        final long next_check_at = last_check + timeout;

        if ( (cached_cache_value != null) & (next_check_at >= System.currentTimeMillis())) {
            return cached_cache_value;
        }

        // if the computed value is null, or empty, we transfer the flow file to failure relationship
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return null;
        }

        List<String> cacheKeys = Arrays.stream(cacheKey.split(",")).filter(path -> !StringUtils.isEmpty(path)).map(String::trim).collect(Collectors.toList());

        // the cache client used to interact with the distributed cache
        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        try {
            final byte[] cacheValue = cache.get(cacheKey, keySerializer, valueDeserializer);
            if (cacheValue == null) {
                logger.info("Could not find an entry in cache for {}; routing to not-found", new Object[]{flowFile});
                session.transfer(flowFile, REL_NOT_FOUND);
                return null;
            } else {
                state_value = new String(cacheValue, context.getProperty(PROP_CHARACTER_SET).getValue());
                logger.info("routeOnState: got cached value " + state_value);
            }
        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
        }

        last_check = System.currentTimeMillis();
        cached_cache_value = state_value;
        return state_value;
    }

    public static class CacheValueSerializer implements Serializer<byte[]> {

        @Override
        public void serialize(final byte[] bytes, final OutputStream out) throws SerializationException, IOException {
            out.write(bytes);
        }
    }

    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     */
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
