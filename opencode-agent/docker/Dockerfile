FROM node:22.19.0-bookworm-slim

ARG OPENCODE_VERSION=2.0.23
ARG ABAP_MCP_VERSION=2.7.0
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git python3 ripgrep unzip \
    && rm -rf /var/lib/apt/lists/*
RUN npm install -g @opencode/cli@${OPENCODE_VERSION} \
    && npm install --prefix /opt/abap-mcp abap-adt-mcp@${ABAP_MCP_VERSION} \
    && npm cache clean --force
COPY shared/patches/patch-preview-session.py /opt/setup/patch-preview-session.py
RUN python3 /opt/setup/patch-preview-session.py /opt/abap-mcp/node_modules/abap-adt-mcp \
    && node --check /opt/abap-mcp/node_modules/abap-adt-mcp/dist/handlers/QueryHandlers.js
COPY docker/config/opencode.json /home/node/.config/opencode/opencode.json
COPY docker/config/systems.sap.json /opt/setup/systems.template.json
COPY docker/configure-systems.py /opt/setup/configure-systems.py
COPY shared/sap-policy.json /opt/setup/sap-policy.json
COPY shared/skills/ /home/node/.config/opencode/skills/
COPY docker/entrypoint.sh /usr/local/bin/start-opencode-sap
RUN sed -i 's/\r$//' /usr/local/bin/start-opencode-sap \
    && chmod 755 /usr/local/bin/start-opencode-sap \
    && mkdir -p /workspace /home/node/.local/share/opencode /home/node/.cache/opencode /home/node/.local/state/opencode \
    && chown -R node:node /home/node /workspace
USER node
ENV HOME=/home/node TERM=xterm-256color COLORTERM=truecolor
WORKDIR /workspace
ENTRYPOINT ["start-opencode-sap"]
CMD ["opencode", "--standalone", "/workspace"]

