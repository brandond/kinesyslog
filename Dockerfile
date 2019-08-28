FROM python:3-alpine AS builder
RUN apk --no-cache upgrade
RUN apk --no-cache add alpine-sdk
RUN pip install virtualenv
RUN virtualenv app
COPY ./ /usr/src/kinesyslog
RUN /app/bin/pip install /usr/src/kinesyslog/

FROM python:3-alpine
LABEL maintainer="Brad Davidson <brad@oatmail.org>"
RUN apk --no-cache upgrade
COPY --from=builder /app /app
ENTRYPOINT ["/app/bin/kinesyslog"]
