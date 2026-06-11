FROM node:26-slim

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

EXPOSE 6881

CMD ["npm", "start"]
