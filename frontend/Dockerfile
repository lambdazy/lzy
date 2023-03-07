FROM node:16.16 as builder
WORKDIR /app
COPY package.json .
COPY package-lock.json .
RUN npm install

COPY . .
RUN npm run build

FROM nginx:1.13.12-alpine
COPY --from=builder /app/build /usr/share/nginx/html
EXPOSE 80

COPY nginx.conf /etc/nginx/nginx.conf
CMD ["nginx", "-g", "daemon off;"]