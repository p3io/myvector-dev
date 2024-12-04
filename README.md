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
