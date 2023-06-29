FROM ubuntu
RUN apt update && apt install -y curl git
RUN curl -sL https://deb.nodesource.com/setup_18.x -o /tmp/nodesource_setup.sh
RUN bash /tmp/nodesource_setup.sh && apt install nodejs
RUN npm install -g arlocal serve 
ADD db3 /usr/bin/db3
RUN chmod +x /usr/bin/db3
ADD ar_miner.sh /usr/bin/ar_miner.sh
ADD start_localnet.sh /usr/bin/start_localnet.sh
ADD pages /pages
CMD ["/bin/bash", "/usr/bin/start_localnet.sh"]
