FROM htcondor/mini:9.10-el7

ENV CONFIG_D_DIR=/etc/condor/config.d

ARG TRANSFER_UID
ENV TRANSFER_UID=${TRANSFER_UID:-1010}

ARG TRANSFER_GID
ENV TRANSFER_GID=${TRANSFER_GID:-1010}

RUN set -eu \
    && rm \
        ${CONFIG_D_DIR}/00-htcondor-9.0.config \
        ${CONFIG_D_DIR}/00-minicondor \
    && groupadd -g ${TRANSFER_GID} slotuser \
    && useradd -g ${TRANSFER_GID} -u ${TRANSFER_UID} slotuser

COPY config.d/10-xfer-host config.d/11-xfer-user ${CONFIG_D_DIR}/
