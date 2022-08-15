FROM htcondor/mini:9.10-el7

ENV CONFIG_D_DIR=/etc/condor/config.d

RUN rm \
      ${CONFIG_D_DIR}/00-htcondor-9.0.config \
      ${CONFIG_D_DIR}/00-minicondor

COPY templates/10-xfer-host ${CONFIG_D_DIR}/
