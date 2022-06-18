FROM centos:latest
COPY ./etcd /etcd_dir/
COPY ./minio /minio_dir/
COPY ./rtstore /rtstore_dir/
COPY ./vars.txt /rtstore_dir/
COPY ./start_all.sh /
CMD [ "/bin/bash", "/start_all.sh" ]
