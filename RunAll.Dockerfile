FROM maven:amazoncorretto
COPY . /app
RUN cd /app && mvn install -DskipTests