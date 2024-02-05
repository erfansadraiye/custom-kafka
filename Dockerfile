FROM openjdk:17-oracle

WORKDIR /app

COPY target/*.jar /app/application.jar

CMD ["java", "-jar", "application.jar"]
