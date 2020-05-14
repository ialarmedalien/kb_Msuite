FROM ialarmedalien/kbase_checkm_base:latest

COPY ./ /kb/module

WORKDIR /kb/module

RUN chmod -R a+rw /kb/module \
    && make all \
    && rm -f /data/__READY__

ENTRYPOINT [ "./scripts/entrypoint.sh" ]

CMD [ ]
