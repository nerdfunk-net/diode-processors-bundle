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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"nerdfunk", "Attributes"})
@CapabilityDescription("Sets mandatory nerdfunk Attributes")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
    @WritesAttribute(attribute="nerdfunk.destination.dc", description="The Destination Domain Component"),
    @WritesAttribute(attribute="nerdfunk.destination.department", description="The Destination Department"),
    @WritesAttribute(attribute="nerdfunk.destination.ou", description="The Destination Organizational Unit"),
    @WritesAttribute(attribute="nerdfunk.destination.dc", description="The Destination Common Name"),
    @WritesAttribute(attribute="nerdfunk.source.dc", description="The Source Domain Component"),
    @WritesAttribute(attribute="nerdfunk.source.department", description="The Source Department"),
    @WritesAttribute(attribute="nerdfunk.source.ou", description="The Source Organizational Unit"),
    @WritesAttribute(attribute="nerdfunk.source.dc", description="The Source Common Name"),
    @WritesAttribute(attribute="nerdfunk.priority", description="The QoS Parameter"),
    @WritesAttribute(attribute="nerdfunk.hiddencopy", description="Create hiddencopy"),
    @WritesAttribute(attribute="nerdfunk.ulid", description="A unique log Identifier"),
    @WritesAttribute(attribute="nerdfunk.file.incomingat", description="Current Timestamp")
})

public class NerdfunkAttributes extends AbstractProcessor {

    static final AllowableValue HC_TRUE = new AllowableValue ("true", "true", "Make hidden copy");
    static final AllowableValue HC_FALSE = new AllowableValue ("false", "false", "do not make hidden copy");
    
    static final AllowableValue NET1 = new AllowableValue ("net1", "net1", "net1");
    static final AllowableValue NET2 = new AllowableValue ("net2", "net2","net2");
    
    static final AllowableValue DEP1 = new AllowableValue ("dep1", "dep1", "dep1");
    static final AllowableValue DEP2 = new AllowableValue ("dep2", "dep2","dep2");
    static final AllowableValue DEP3 = new AllowableValue ("dep3", "dep3", "dep3");

    static final AllowableValue PRIO_STATUS =  new AllowableValue ("1", "Control file", "Control file");
    static final AllowableValue PRIO_PROD_FAST =  new AllowableValue ("10", "Production Data (prio)", "Production Data (prio)");
    static final AllowableValue PRIO_PROD_NORMAL =  new AllowableValue ("20", "Production Data", "Production Data");
    static final AllowableValue PRIO_PROD_BULK =  new AllowableValue ("50", "Production Bulk Data", "Production Bulk Data");
    static final AllowableValue PRIO_OPDATA =  new AllowableValue ("90", "Operational Data", "Operational Data");
    static final AllowableValue PRIO_OPBULK =  new AllowableValue ("99", "Operational Bulk Data", "Operational Bulk Data");

    public static final PropertyDescriptor NERDFUNK_PRIORITY = new PropertyDescriptor.Builder()
            .name("nerdfunk.priority")
            .description("Priority of the Data")
            .required(true)
            .allowableValues(PRIO_STATUS, PRIO_PROD_FAST, PRIO_PROD_NORMAL, PRIO_PROD_BULK, PRIO_OPDATA, PRIO_OPBULK)
            .defaultValue(PRIO_PROD_NORMAL.getValue())
            .build();
    
    public static final PropertyDescriptor NERDFUNK_HIDDENCOPY = new PropertyDescriptor.Builder()
            .name("nerdfunk.hiddencopy")
            .description("make hidden copy")
            .required(true)
            .allowableValues(HC_TRUE, HC_FALSE)
            .defaultValue(HC_FALSE.getValue())
            .build();
    
    public static final PropertyDescriptor NERDFUNK_DST_DC = new PropertyDescriptor.Builder()
            .name("nerdfunk.destination.dc")
            .description("The Destination Domain Component")
            .required(true)
            .allowableValues(NET1, NET2)
            .defaultValue(NET1.getValue())
            .build();

    public static final PropertyDescriptor NERDFUNK_DST_DEPARTMENT = new PropertyDescriptor.Builder()
            .name("nerdfunk.destination.department")
            .description("The Destination Department")
            .required(true)
            .allowableValues(DEP1, DEP2, DEP3)
            .defaultValue(DEP1.getValue())
            .build();

    public static final PropertyDescriptor NERDFUNK_DST_OU = new PropertyDescriptor.Builder()
            .name("nerdfunk.destination.ou")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
 
    public static final PropertyDescriptor NERDFUNK_DST_CN = new PropertyDescriptor.Builder()
            .name("nerdfunk.destination.cn")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor NERDFUNK_SRC_DC = new PropertyDescriptor.Builder()
            .name("nerdfunk.source.dc")
            .description("The Source Domain Component")
            .required(true)
            .allowableValues(NET1, NET2)
            .defaultValue(NET1.getValue())
            .build();

    public static final PropertyDescriptor NERDFUNK_SRC_DEPARTMENT = new PropertyDescriptor.Builder()
            .name("nerdfunk.source.department")
            .description("The Source Department")
            .required(true)
            .allowableValues(DEP1, DEP2, DEP3)
            .defaultValue(DEP1.getValue())
            .build();

    public static final PropertyDescriptor NERDFUNK_SRC_OU = new PropertyDescriptor.Builder()
            .name("nerdfunk.source.ou")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
 
    public static final PropertyDescriptor NERDFUNK_SRC_CN = new PropertyDescriptor.Builder()
            .name("nerdfunk.source.cn")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NERDFUNK_PRIORITY);
        descriptors.add(NERDFUNK_HIDDENCOPY);
        descriptors.add(NERDFUNK_DST_DC);
        descriptors.add(NERDFUNK_DST_DEPARTMENT);
        descriptors.add(NERDFUNK_DST_OU);
        descriptors.add(NERDFUNK_DST_CN);
        descriptors.add(NERDFUNK_SRC_DC);
        descriptors.add(NERDFUNK_SRC_DEPARTMENT);
        descriptors.add(NERDFUNK_SRC_OU);
        descriptors.add(NERDFUNK_SRC_CN);
    
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
    
        //final long currentNanos = System.nanoTime();
        final long currentMillis = System.currentTimeMillis();
        
        flowFile = session.putAttribute(flowFile, "nerdfunk.priority", context.getProperty(NERDFUNK_PRIORITY).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.destination.dc", context.getProperty(NERDFUNK_DST_DC).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.destination.department", context.getProperty(NERDFUNK_DST_DEPARTMENT).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.destination.ou", context.getProperty(NERDFUNK_DST_OU).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.destination.cn", context.getProperty(NERDFUNK_DST_CN).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.source.dc", context.getProperty(NERDFUNK_SRC_DC).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.source.department", context.getProperty(NERDFUNK_SRC_DEPARTMENT).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.source.ou", context.getProperty(NERDFUNK_SRC_OU).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.source.cn", context.getProperty(NERDFUNK_SRC_CN).getValue());
        flowFile = session.putAttribute(flowFile, "nerdfunk.hiddencopy", context.getProperty(NERDFUNK_HIDDENCOPY).getValue());
        
        flowFile = session.putAttribute(flowFile, "nerdfunk.ulid", Long.toString(currentMillis));
        flowFile = session.putAttribute(flowFile, "nerdfunk.file.incomingat", Long.toString(currentMillis));
        
        session.transfer(flowFile, REL_SUCCESS);
        
    }
}
