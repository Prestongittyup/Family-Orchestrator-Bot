FROM node:18-alpine

WORKDIR /app

COPY package*.json /app/
RUN npm install --no-audit --no-fund

COPY . /app

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
