FROM htcondor/mini:9.10-el7


RUN yum update -y \
    && yum clean all \
    && rm -rf /var/cache/yum/


# Remove the "minicondor" configuration.
# Prepare /condor/local for storing HTCondor's logs and ephemeral data.
# Prepare /condor/tokens.d for storing the EP's IDTOKEN.


RUN rm \
      /etc/condor/config.d/00-htcondor-9.0.config \
      /etc/condor/config.d/00-minicondor \
    && mkdir -m 0755 /condor \
    && mkdir -m 0777 /condor/local \
    && mkdir -m 0755 /condor/tokens.d


# Copy in the new startup script, which should prepare HTCondor's "local"
# directory by creating the necessary subdirectories as the user running the
# container.


COPY bin/start.sh /opt/osg/bin/
COPY config.d/10-xfer-host /etc/condor/config.d/

ENTRYPOINT ["/opt/osg/bin/start.sh"]
