FROM keymetrics/pm2:8-alpine

WORKDIR /srv/node-scripts/
# Bundle APP files
COPY package.json .
COPY ecosystem.config.js .

# Install app dependencies
ENV NPM_CONFIG_LOGLEVEL warn
RUN apk add --no-cache git
# RUN apk add --update \
#     python \
#     build-base \
#     && rm -rf /var/cache/apk/*

RUN npm install --production

# Expose the listening port of your app
EXPOSE 1883

CMD [ "pm2-runtime", "start", "ecosystem.config.js" ]
