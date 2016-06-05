# nifi-processor-AddAttributeMarkings

Apache NiFi plugin to enable adding attributes loaded from configuration files to describe content source, classification and handling instructions. This could be performed by UpdateAttributes with the difference in that this processor reads the available values for each settable attribute from a configuration file rather than requiring manual entry in each instance of the processor.

This processor has 2 configuration files, specified by the properties Label file, Classification file and Handling file. These files have the following structure:

Label file: one label per line, eg.:
SOURCE-LABEL1
SOURCE-LABEL2
ANOTHERLOCATION
CRITICALFILES-WEBSITE

Classification file: one classification per line, with optional colon delimited comment for the classifications help, eg.:
BUSINESS-IN-CONFIDENCE
PRIVATE-CONFIDENTIAL:Information confidential to the internal organisation
UNCLASSIFIED:Publicly available information

Handling file: one handling instruction per line, with optional colon delimited comment for the handeling instructions help, eg.:
ARCHIVE_ONLY:Files are archived in long term storage
TRANSFORM1:Transform the files through chain 1

Once these files are populated and the location specified in properties, apply and re-configure the processor to have the available values populated for each variable available for selection, the first entry is the default value.

The label, classification, handling attributes will now be set on all flowfiles transiting the processor.
