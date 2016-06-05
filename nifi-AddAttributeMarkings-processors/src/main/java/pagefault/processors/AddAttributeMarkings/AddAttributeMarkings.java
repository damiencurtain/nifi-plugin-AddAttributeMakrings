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
package pagefault.processors.AddAttributeMarkings;

import java.io.BufferedReader;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import org.apache.nifi.components.AllowableValue;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ProcessorLog;


@Tags({"attributes"})
@CapabilityDescription("Adds classification, source identity and processing method attributes to flowfiles to aid in generic routing at the destination")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
    @WritesAttribute(attribute = "label", description = "The label to apply to content that identifies the source"),
    @WritesAttribute(attribute = "classification", description = "The classification of the content"),
    @WritesAttribute(attribute = "processing", description = "The processing method that describes how to process the content at the destination"),
})public class AddAttributeMarkings extends AbstractProcessor {

    private static Path label_file = Paths.get("./conf/label.conf");;
    private static Path classification_file = Paths.get("./conf/classification.conf");
    private static Path processing_file = Paths.get("./conf/processing.conf");
    
    public static final PropertyDescriptor CLASSIFICATION_FILE = new PropertyDescriptor.Builder()
        .name("Classification's File")
        .description("File containing all known classifications to the system at this level on ingestion")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .required(true)
        .defaultValue("./conf/classification.conf")
        .build();
    public static final PropertyDescriptor LABEL_FILE = new PropertyDescriptor.Builder()
        .name("Label's File")
        .description("File containing all known labels to the system at this level on ingestion")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .required(true)
        .defaultValue("./conf/label.conf")
        .build();
    public static final PropertyDescriptor PROCESSING_FILE = new PropertyDescriptor.Builder()
        .name("Processing File")
        .description("File containing methods for processing the content at the destination")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .required(true)
        .defaultValue("./conf/processing.conf")
        .build();
    
    public PropertyDescriptor LABEL = new PropertyDescriptor.Builder()
            .name("Label")
            .description("Unique label to identify source")
            .required(true)
            .allowableValues(buildAllowableValues(label_file.toString()))
            .defaultValue(getDefaultAllowableValues(label_file.toString()))
            .build();
    public PropertyDescriptor CLASSIFICATION = new PropertyDescriptor.Builder()
            .name("Classification")
            .description("Classification label to apply to this source")
            .required(true)
            .allowableValues(buildAllowableValues(classification_file.toString()))
            .defaultValue(getDefaultAllowableValues(classification_file.toString()))
            .build();
    public PropertyDescriptor PROCESSING = new PropertyDescriptor.Builder()
            .name("Processing")
            .description("Processing label to apply to this source")
            .required(true)
            .allowableValues(buildAllowableValues(processing_file.toString()))
            .defaultValue(getDefaultAllowableValues(processing_file.toString()))
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully marked up are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be marked up for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        
        if (descriptor.equals(LABEL_FILE)) {
            
            this.label_file = Paths.get(newValue);
        
            this.LABEL = new PropertyDescriptor.Builder()
                .name("Label")
                .description("Unique label to identify source")
                .required(true)
                .allowableValues(buildAllowableValues(label_file.toString()))
                .defaultValue(getDefaultAllowableValues(label_file.toString()))
                .build(); 
        } else if (descriptor.equals(CLASSIFICATION_FILE)) {
            
            this.classification_file = Paths.get(newValue);
            
            this.CLASSIFICATION = new PropertyDescriptor.Builder()
                .name("Classification")
                .description("Classification label to apply to this source")
                .required(true)
                .allowableValues(buildAllowableValues(classification_file.toString()))
                .build();                 
        } else if (descriptor.equals(PROCESSING_FILE)) {
            
            this.processing_file = Paths.get(newValue);
            
            this.PROCESSING = new PropertyDescriptor.Builder()
                .name("Processing")
                .description("Processing label to apply to this source")
                .required(true)
                .allowableValues(buildAllowableValues(processing_file.toString()))
                .build();
        }

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CLASSIFICATION_FILE);
        descriptors.add(LABEL_FILE);
        descriptors.add(PROCESSING_FILE);
        descriptors.add(CLASSIFICATION);
        descriptors.add(LABEL);
        descriptors.add(PROCESSING);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }
   
    private String getDefaultAllowableValues(String source_file) {
    
        return buildAllowableValues(source_file)[0].getValue();
    }
    
    private AllowableValue[] buildAllowableValues(String source_file) {
        
        final ProcessorLog logger = getLogger();
        
        final File file = new File(source_file);
        
        if (file.exists() && file.isFile() && file.canRead()) {
        
            try (FileInputStream is = new FileInputStream(file)) {

                final Map<String, String> mapping = loadMappingFile(is);
                List<AllowableValue> allowableValues = new ArrayList<>(mapping.size());
                
                for (final Map.Entry<String, String> entry : mapping.entrySet()) {
            
                    final String key = entry.getKey();
                    final String comment = entry.getValue();
                    allowableValues.add(new AllowableValue(key, key, comment));
                }
                
                return allowableValues.toArray(new AllowableValue[0]);
            } catch (IOException e) {
                logger.error("Error reading mapping file: {}", new Object[]{e.getMessage()});
            }
        }
          
        List<AllowableValue> allowableValues = new ArrayList<>(1);
        allowableValues.add(new AllowableValue("INVALID", "Invalid content", "Invalid content"));
        return allowableValues.toArray(new AllowableValue[0]);
    }

    protected Map<String, String> loadMappingFile(InputStream is) throws IOException {
        
        Map<String, String> mapping = new HashMap<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        
        String line = null;
        
        while ((line = reader.readLine()) != null) {
            final String[] splits = StringUtils.split(line, ":", 2);
            if (splits.length == 1) {
                mapping.put(splits[0].trim(), ""); // support key with empty value
            } else if (splits.length == 2) {
                final String key = splits[0].trim();
                final String value = splits[1].trim();
                mapping.put(key, value);
            }
        }
        return mapping;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> _descriptors = new ArrayList<PropertyDescriptor>();
        _descriptors.add(CLASSIFICATION_FILE);
        _descriptors.add(LABEL_FILE);
        _descriptors.add(PROCESSING_FILE);
        _descriptors.add(CLASSIFICATION);
        _descriptors.add(LABEL);
        _descriptors.add(PROCESSING);
        this.descriptors = Collections.unmodifiableList(_descriptors);

        final Set<Relationship> _relationships = new HashSet<Relationship>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(_relationships);
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

        try {
            flowFile = session.putAttribute(flowFile, LABEL.getName().toLowerCase(), context.getProperty(LABEL).getValue());
            flowFile = session.putAttribute(flowFile, CLASSIFICATION.getName().toLowerCase(), context.getProperty(CLASSIFICATION).getValue());
            flowFile = session.putAttribute(flowFile, PROCESSING.getName().toLowerCase(), context.getProperty(PROCESSING).getValue());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Throwable t) {
                System.out.println("Error applying attributes to flowfile -> " + t);
                session.transfer(flowFile, REL_FAILURE);
        }
    }
}
