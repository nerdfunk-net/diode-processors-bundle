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
package net.nerdfunk.nifi.processors.attributes;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Iterator;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;

@Tags({"nerdfunk", "Attributes"})
@CapabilityDescription("Sets preconfigured Attributes")
@SeeAlso({})
@ReadsAttributes({
@ReadsAttribute(attribute = "match attribute", description = "The name of the attribute we are looking at.")})

public class AddAttributes extends AbstractProcessor {

    private String matchAttribute;
    private Map<String, Map<String, String>> json;

    public static final PropertyDescriptor MATCH_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("match attribute")
            .displayName("Matching Attribute")
            .description("The name of the attribute we are looking at.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MATCH_ATTRIBUTE);
        return Collections.unmodifiableList(descriptors);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        matchAttribute = context.getProperty(MATCH_ATTRIBUTE).evaluateAttributeExpressions().getValue();
        
        getLogger().debug("preparing json value");
        final Map<String, String> properties = context.getAllProperties();
        final ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
        json = new HashMap<String, Map<String, String>>();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (!MATCH_ATTRIBUTE.getName().equals(entry.getKey())) {
                try {
                    map = mapper.readValue(entry.getValue(), Map.class);
                    json.put(entry.getKey(), map);
                } catch (IOException e) {
                    getLogger().error("cannot validate json");
                }
            }
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        PropertyDescriptor.Builder propertyBuilder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true);

        return propertyBuilder
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> reasons = new ArrayList<>(super.customValidate(context));
        final Map<String, String> properties = context.getAllProperties();
        final ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<String, String>();
        
        Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (!MATCH_ATTRIBUTE.getName().equals(entry.getKey())) {
                try {
                    map = mapper.readValue(entry.getValue(), Map.class);
                } catch (IOException e) {
                    getLogger().error("cannot validate json");
                    reasons.add(new ValidationResult.Builder().valid(false).explanation("Unable to parse " + entry.getValue()).build());;
                }
            }
        }
        return reasons;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        /*
         * look into the existing flowfile attributes if the matched attribute exists
         */
        final String value = flowFile.getAttribute(matchAttribute);
        if (!value.isEmpty()) {
            putAttributes(value, flowFile, session);
        }
        session.transfer(flowFile, REL_SUCCESS);

    }

    private void putAttributes(final String header, final FlowFile flowFile, final ProcessSession session) {
        Map<String, String> map = json.get(header);
        for (String key : map.keySet()) {
            session.putAttribute(flowFile, key, String.valueOf(map.get(key)));
        }
    }
}
