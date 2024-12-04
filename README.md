[BUILD]
$ cd mysql-server/src/plugin
$ git clone https://github.com/p3io/myvector-dev/ ./myvector
// regenerate cmake files
$ cd mysql-server/bld
$ cmake .. <other options>
$ cd mysql-server/bld/plugin/myvector
$ make

[INSTALL]
$ cp mysql-server/bld/plugin_output_directory/myvector.so   /usr/local/mysql/lib/plugin/

/// Register the plugin and create stored procedures.
$ cd mysql-server/plugin/myvector

/// Connect to 'mysql' database as 'root' 
$ mysql -u root -p mysql
mysql> source myvectorplugin.sql
