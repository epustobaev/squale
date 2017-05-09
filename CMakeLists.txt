cmake_minimum_required(VERSION 3.7)
project(squale)

set(CMAKE_C_STANDARD 99)
set(VERSION 0.1.13)
set(SQUALESYSCONFDIR "/etc/squale")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

configure_file(config.h.in config.h)

find_package(Glib)
include_directories(${GLib_INCLUDE_DIRS})
set(LIBS ${LIBS} ${GLib_LIBRARY})
find_package(LibXml2)
include_directories(${LIBXML2_INCLUDE_DIR})
set(LIBS ${LIBS} ${LIBXML2_LIBRARIES})
find_package(PostgreSQL)
include_directories(${PostgreSQL_INCLUDE_DIRS})
set(LIBS ${LIBS} ${PostgreSQL_LIBRARIES})

set(SOURCE_FILES config.h squale.c squale.h squaleclient.c squaleclient.h squale-i18n.h squalejoblist.c squalejoblist.h squalejob.c squalejob.h squaleworker.c squaleworker.h squalelistener.c squalelistener.h squalexml.c squalexml.h squalelog.c squaleoracleworker.c squaleoracleworker.h)
add_library(squale SHARED ${SOURCE_FILES} squale.c squale.h squaleclient.c squaleclient.h squale-i18n.h squalejoblist.c squalejoblist.h squalejob.c squalejob.h squaleworker.c squaleworker.h squalelistener.c squalelistener.h squalexml.c squalexml.h squalelog.c squaleoracleworker.c squaleoracleworker.h config.h)
