#! /bin/bash
#

mkdir -p ~/.db3/bin

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "download db3 package from github"
    wget https://github.com/dbpunk-labs/db3/releases/download/v0.2.6/db3-v0.2.6-linux-x86_64.tar.gz -O /tmp/db3-v0.2.6-linux-x86_64.tar.gz
    tar -zxvf /tmp/db3-v0.2.6-linux-x86_64.tar.gz
    cp db3-v0.2.6-linux-x86_64/bin/db3 ~/.db3/bin
elif [[ "$OSTYPE" == "darwin"* ]]; then
    wget https://github.com/dbpunk-labs/db3/releases/download/v0.2.6/db3-v0.2.6-macos-x86_64.tar.gz -O /tmp/db3-v0.2.6-macos-x86_64.tar.gz
    tar -zxvf /tmp/db3-v0.2.6-macos-x86_64.tar.gz
    cp db3-v0.2.6-macos-x86_64/bin/db3 ~/.db3/bin
else
    echo "$OSTYPE is not supported, please give us a issue https://github.com/dbpunk-labs/db3/issues/new/choose"
    exit 1
fi

export PATH=~/.db3/bin:$PATH
echo "please add ~/.db3/bin to the PATH"
