# tidb-dataset

The dataset import tool for the demo of TiDB.

## Install

You can use one of the three approaches.

### Install using script(recommend)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/Mini256/tidb-dataset/main/install.sh | sh
```

And then open a new terminal to try `tidb-dataset`.

### Download binary

You can download the pre-built binary [here](https://github.com/Mini256/tidb-dataset/releases/latest) and then gunzip it.

### Build from source

```bash
git clone https://github.com/pingcap/go-tpc.git
make build
```

Then you can find the `tidb-dataset` binary file in the `./bin` directory.

## Usage

The syntax style of the command line is like this:

```bash
tidb-dataset <dataset_name> <command>
```

For example, if you plan to use the `movie` dataset, you can use the command to import data into the test database (Before this, you can quickly start a TIDB database locally through the `tiup playground` command).

```bash
tidb-dataset movie prepare
```

The tool will import the data into the database named 'test' by default. You can specify it through the following parameters:

```
  -D, --db string           Database name (default "test")
  -H, --host string         Database host (default "127.0.0.1")
  -p, --password string     Database password
  -P, --port int            Database port (default 4000)
  -U, --user string         Database user (default "root")
```

After your test is completed, you can clear the database table generated during the test by using the following command:

```bash
tidb-dataset movie cleanup
```

If you want to know more usage, please use the 'tidb-dataset --help' command.

