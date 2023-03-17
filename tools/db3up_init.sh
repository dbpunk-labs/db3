#! /bin/bash
#
# db3up_init.sh

mkdir -p ~/.db3/bin
VERSION=`curl -s https://api.github.com/repos/dbpunk-labs/db3/releases/latest | python3  -c 'import sys, json; print(json.load(sys.stdin)["name"])'`
curl -L --max-redirs 10 https://github.com/dbpunk-labs/db3/releases/download/${VERSION}/db3up -o ~/.db3/bin/db3up
chmod +x ~/.db3/bin/db3up
if [ -f ~/.zshrc ]; then
    read -p "Add ~/.db3/bin to your PATH(y/n)? " yn
    case $yn in
        [Yy]* ) echo "PATH=~/.db3/bin:\$PATH" >> ~/.zshrc && echo "please run source ~/.zshrc manually";;
        [Nn]* ) echo "please add PATH=~/.db3/bin:\$PATH to ~/.zshrc manually";;
    esac
elif [ -f ~/.bashrc ]; then
    read -p "Add ~/.db3/bin to your PATH(y/n)? " yn
    case $yn in
        [Yy]* ) echo "PATH=~/.db3/bin:\$PATH" >> ~/.zshrc && echo "please run source ~/.bashrc manually";;
        [Nn]* ) echo "please add PATH=~/.db3/bin:\$PATH to ~/.bashrc manually";;
    esac
else
    echo "please add PATH=~/.db3/bin:\$PATH to your enviroment manually"
fi
echo "install db3up successfully"
export PATH=~/.db3/bin:$PATH
echo "start to install db3 network"
db3up install
