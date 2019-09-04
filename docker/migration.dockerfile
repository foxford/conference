FROM clux/diesel-cli:latest
WORKDIR /app
CMD ["diesel", "migration", "run"]
COPY ./migrations /app/migrations
