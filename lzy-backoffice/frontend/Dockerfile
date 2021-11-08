FROM nginx:1.13.12-alpine
COPY build /usr/share/nginx/html
EXPOSE 80
COPY nginx.conf /etc/nginx/nginx.conf
CMD ["nginx", "-g", "daemon off;"]