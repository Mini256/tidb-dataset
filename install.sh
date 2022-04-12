#!/bin/sh

version=0.0.5

case $(uname -s) in
    Linux|linux) os=linux ;;
    Darwin|darwin) os=darwin ;;
    *) os= ;;
esac

if [ -z "$os" ]; then
    echo "OS $(uname -s) not supported." >&2
    exit 1
fi

case $(uname -m) in
    amd64|x86_64) arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) arch= ;;
esac

if [ -z "$arch" ]; then
    echo "Arch $(uname -m) not supported." >&2
    exit 1
fi

binary_url="https://github.com/Mini256/tidb-dataset/releases/download/v${version}/tidb-dataset_${version}_${os}_${arch}.tar.gz"
output_file="/tmp/tidb-dataset_${version}_${os}_${arch}.tar.gz"

if [ -z "$GO_TPC_HOME" ]; then
    GO_TPC_HOME=$HOME/.tidb-dataset
fi
bin_dir=$GO_TPC_HOME/bin
mkdir -p "$bin_dir"

install_binary() {
    curl -L $binary_url -o $output_file || return 1
    tar -zxf $output_file -C "$bin_dir" || return 1
    rm $output_file
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract tidb-dataset archive."
    exit 1
fi

chmod 755 "$bin_dir/tidb-dataset"


bold=$(tput bold 2>/dev/null)
sgr0=$(tput sgr0 2>/dev/null)

# Refrence: https://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux-unix
shell=$(echo $SHELL | awk 'BEGIN {FS="/";} { print $NF }')
echo "Detected shell: ${bold}$shell${sgr0}"
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE=${HOME}/.${shell}_profile
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE=${HOME}/.${shell}_login
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE=${HOME}/.${shell}rc
else
    PROFILE=${HOME}/.profile
fi
echo "Shell profile:  ${bold}$PROFILE${sgr0}"

case :$PATH: in
    *:$bin_dir:*) : "PATH already contains $bin_dir" ;;
    *) printf 'export PATH=%s:$PATH\n' "$bin_dir" >> "$PROFILE"
        echo "$PROFILE has been modified to to add tidb-dataset to PATH"
        echo "open a new terminal or ${bold}source ${PROFILE}${sgr0} to use it"
        ;;
esac

echo "Installed path: ${bold}$bin_dir/tidb-dataset${sgr0}"
echo "==============================================="
echo "Have a try:     ${bold}tidb-dataset movie prepare ${sgr0}"
echo "==============================================="
