MYSQL_ADD_PLUGIN(myvector
  myvector_plugin.cc
  myvector.cc
  myvector_binlog.cc
  LINK_LIBRARIES ${LIBMYSQLCLIENT}
  MODULE_ONLY
  MODULE_OUTPUT_NAME "myvector"
  )
