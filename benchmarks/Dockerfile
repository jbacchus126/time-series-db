# Dockerfile for OpenSearch with TSDB Plugin
FROM opensearchproject/opensearch:latest

# Copy the TSDB plugin ZIP from the build directory
# Note: The build context must be set to the repo root
COPY build/distributions/tsdb-*.zip /tmp/

# Install the TSDB plugin
RUN PLUGIN_ZIP=$(ls /tmp/tsdb-*.zip) && \
    /usr/share/opensearch/bin/opensearch-plugin install --batch file://$PLUGIN_ZIP && \
    rm -f $PLUGIN_ZIP || true

# Use the default entrypoint from the base image

