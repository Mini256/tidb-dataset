#！/bin/bash
set -e

repo_name="Mini256/tidb-dataset"
version="0.0.11"
binary_name="tidb-dataset"
component_name="demo"
component_desc="The dataset import tools for the demo of TiDB"

matrix=(
    'darwin amd64'
    'darwin arm64'
    'linux amd64'
    'linux arm64'
)

for item in "${matrix[@]}" ; do
    os_arch=($item)
    os=(${os_arch[0]})
    arch=(${os_arch[1]})

    url="https://github.com/${repo_name}/releases/download/v${version}/${binary_name}_${version}_${os}_${arch}.tar.gz"
    filename="./tarball/${binary_name}_${version}_${os}_${arch}.tar.gz"

    echo "Downloading release v${version} for os $os arch $arch..."
    wget $url -c -q -O $filename

    tiup mirror publish $component_name "v${version}" $filename $binary_name --os=$os --arch=$arch --hide --desc="$component_desc"
done

echo 'Finished!'
