FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY server.js ./

RUN mkdir /data && chown node:node /data
USER node

CMD ["node", "--max-old-space-size=160", "server.js"]
