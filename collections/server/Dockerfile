FROM node:12

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# App
EXPOSE 8080

# Monitoring (Prometheus)
EXPOSE 8081

ENTRYPOINT ["npm", "run", "start"]

