# SqlConsole

SqlConsole is a python3 SQL REPL for Oracle and SQL Server (Postgres coming soon).  You can enter connect to different databases and save results to python for later analysis.  The layout resembles SQLPlus and allows you to run the same metadata commands from SqlConsole.


## Getting Started

To get started, clone the repo:

```
git clone https://github.com/tongelja/SqlConsole.git
```



### Prerequisites

You will need to have cx_Oracle and pypyodbc installed.


## Running the tests

You can run some simple tests by importing the module and starting to connect.

```
myDB = sqlconsole.Oracle()
myDB.open()
```

You will be prompted for database, server, user, password.  You can also pass these as parameters to the open call.


```
myDB.open('DB01',' myDevServer', 'system', 'mypassword')
```



## Deployment

Once you are connected you can run the REPL by running the cmd module

```
>>> myDB.cmd()

---------------------------
| MYDEVSERVER/DB01
---------------------------

SQL> 

```

From here you can run SQL commands as normal.


## Authors

* **John Tongelidis** - *Initial work* - [tongelja](https://github.com/tongelja)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details



