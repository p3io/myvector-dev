**BUILD**

```
/// get the MyVector sources
$ cd mysql-server/src/plugin
$ git clone https://github.com/p3io/myvector-dev/ ./myvector

/// Generate makefile for the new plugin
$ cd mysql-server/bld
$ cmake .. <other options used for this build >

/// Build the plugin
$ cd mysql-server/bld/plugin/myvector
$ make
```

---

**INSTALL**

```
/// Copy the MyVector plugin shared library to the MySQL installation plugins
$ cp mysql-server/bld/plugin_output_directory/myvector.so   /usr/local/mysql/lib/plugin/

/// Register the MyVector plugin and create MyVector stored procedures.
$ cd mysql-server/plugin/myvector

/// Connect to 'mysql' database as 'root' 
$ mysql -u root -p   mysql
mysql> source myvectorplugin.sql
```

---

**CONFIGURE**

The MyVector plugin introduces 2 system variables in MySQL :-

```
static MYSQL_SYSVAR_STR(
    index_dir, myvector_index_dir,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "MyVector index files directory.",
    nullptr, nullptr, "/mysqldata");

static MYSQL_SYSVAR_STR(
    config_file, myvector_config_file,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "MyVector config file.",
    nullptr, nullptr, "myvector.cnf");
```

The ```myvector_config_file``` variable needs to be set to point to a simple key-value config file.

```
myvector_config_file=/mysqldata/myvector.cnf
```

The ```myvector_index_dir``` variable specifies the directory path where the vector index files should be saved.

```
myvector_index_dir=/mysqldata
```

The MyVector config file is a simple key-value file listing a few variables for the plugin to connect to the MySQL database.

```
$ cat myvector.cnf
myvector_user_id=root
myvector_user_password=...
myvector_socket=/tmp/mysql.sock
```

This file should be readable **ONLY** by the "mysql" user.


---

**TRYING IT OUT**

A simple demo is present in demo/stanford50d/ folder. To run the demo -

```
$ gunzip insert50d.sql.gz

/// Connect to the 'test'  database
$ mysql <credentials> test
mysql>  source create.sql

mysql>  source insert50d.sql

mysql>  source buildindex.sql

```
After the above steps, vector search examples listed in ```search.sql``` can be tried out. 

```

-- Vector distance illustration! 'university' and 'college' are more nearer to each other than compared to 'factory' and 'college'
mysql> select myvector_distance((select wordvec from words50d where word = 'school'), (select wordvec from words50d where word='university'));
+---------------------------------------------------------------------------------------------------------------------------------+
| myvector_distance((select wordvec from words50d where word = 'school'), (select wordvec from words50d where word='university')) |
+---------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                              13.956673622131348 |
+---------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.18 sec)

mysql> select myvector_distance((select wordvec from words50d where word = 'school'), (select wordvec from words50d where word='factory'));
+------------------------------------------------------------------------------------------------------------------------------+
| myvector_distance((select wordvec from words50d where word = 'school'), (select wordvec from words50d where word='factory')) |
+------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                           33.608272552490234 |
+------------------------------------------------------------------------------------------------------------------------------+

-- Display 10 words "nearest" (or similar) to 'harvard'
mysql> select word from words50d where MYVECTOR_IS_ANN('test.words50d.wordvec','wordid',myvector_construct('[-0.8597 1.11297 -0.2997 -1.1093 0.15653 -0.13244 -1.05244 -0.92624 -0.52924 -0.24501 -0.22653 0.252993 -0.099125 -0.406425 0.00097853 -0.0358083 -0.1868983 0.7115799 -0.4448983 0.8665198 0.5433998 0.5982698 -0.0315843 -0.4635143 -0.0850383 -1.890238 0.1114238 -0.7560483 -1.696548 -0.3975283 1.297653 -0.3412783 -0.2289783 -1.452478 -0.2985583 -0.2029783 -0.4421183 1.152112 1.505912 -0.4881983 -0.2117683 -0.3618683 -0.0911083 0.9526609 0.2025408 0.1006808 0.6931608 0.2621508 -0.9098683 0.5950769]'));
+------------+
| word       |
+------------+
| harvard    |
| yale       |
| princeton  |
| graduate   |
| cornell    |
| stanford   |
| berkeley   |
| professor  |
| graduated  |
| university |
+------------+
```

----
