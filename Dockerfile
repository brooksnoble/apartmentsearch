FROM brooksnoble/fsharpbase
MAINTAINER Brooks Noble <brooks@brooksnoble.com>

ADD . /app

WORKDIR /app

RUN mono ./tools/paket.bootstrapper.exe && mono ./tools/paket.exe install && \
    xbuild ApartmentSearch.sln /p:Configuration=Release

EXPOSE 8083

ENV StaticFilePath /app/src/Web.Server/static/

ENTRYPOINT ["/usr/bin/env", "mono", "/app/src/ApartmentSearch/bin/Release/ApartmentSearch.exe"]
