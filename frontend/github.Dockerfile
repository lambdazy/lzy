FROM valian/docker-nginx-auto-ssl:1.2.0

EXPOSE 80
EXPOSE 443

COPY github-redirect-nginx.conf /etc/nginx/conf.d/server.conf