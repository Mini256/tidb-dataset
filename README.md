# tidb-dataset

The dataset import tool for the demo of TiDB.

## Install

You can use one of the three approaches:

### 1. Install using script (recommend)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/Mini256/tidb-dataset/main/install.sh | sh
```

And then open a new terminal to try `tidb-dataset`.

### 2. Download binary

You can download the pre-built binary [here](https://github.com/Mini256/tidb-dataset/releases/latest) and then gunzip it.

### 3. Build from source

```bash
git clone https://github.com/Mini256/tidb-dataset.git
make build
```

Then you can find the `tidb-dataset` binary file in the `./bin` directory.

## Usage

The syntax style of the command line is like this:

```bash
tidb-dataset <dataset_name> <command>
```

If you start the tool via [tiup](https://tiup.io), use the following command:

```bash
tiup demo <dataset_name> <command>
```

Currently available datasets are:

- `bookshop`

### Import test data

For example, if you plan to use the `bookshop` dataset, you can use the command to import data into the test database (Before this, you can quickly start a TIDB database locally through the `tiup playground` command).

```bash
tidb-dataset bookshop prepare
```

For tiup:

```bash
tiup demo bookshop prepare
```

The tool will import the data into the database named `test` by default. You can specify it through the following parameters:

```
  -D, --db string           Database name (default "test")
  -H, --host string         Database host (default "127.0.0.1")
  -p, --password string     Database password
  -P, --port int            Database port (default 4000)
  -U, --user string         Database user (default "root")
```

### Clean up data

After your test is completed, you can clear the database table generated during the test by using the following command:

```bash
tidb-dataset bookshop cleanup
```

For tiup:

```bash
tiup demo bookshop cleanup
```

### More details

If you want to know more usage, please use the `tidb-dataset --help` or `tiup demo --help` command.

