/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squale.h : Header for Squale.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Library General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

#ifndef __SQUALE_H__
#define __SQUALE_H__

#include <glib.h>
#include "squale-i18n.h"
#include <signal.h>
#include <popt.h>
#include <string.h>

typedef struct _Squale Squale;

#include "squalejoblist.h"
#include "squalejob.h"
#include "squaleworker.h"
#include "squalelistener.h"
#include "squaleclient.h"
#include "squalexml.h"
#include "config.h"

struct _Squale
{
    GMutex *log_mutex;

    SqualeXML *xml;

    SqualeListener *listener;

    struct timeval startup_ts;

    GLogLevelFlags log_level;
    char *log_file_name;
    FILE *log_fd;

    gboolean no_detach;

    char *socket_name;
};

#define SQUALE_LOG_LEVEL_ERROR    "ERROR"
#define SQUALE_LOG_LEVEL_CRITICAL "CRITICAL"
#define SQUALE_LOG_LEVEL_WARNING  "WARNING"
#define SQUALE_LOG_LEVEL_MESSAGE  "MESSAGE"
#define SQUALE_LOG_LEVEL_INFO     "INFO"
#define SQUALE_LOG_LEVEL_DEBUG    "DEBUG"

void squale_set_log_level (Squale *squale, const char *log_level,
                           gboolean override);
void squale_set_log_file (Squale *squale, const char *log_file,
                          gboolean override);
void squale_set_socket_name (Squale *squale, const char *socket_name);

void squale_quit (int sig);

void squale_log_handler (const gchar *log_domain,
                         GLogLevelFlags log_level,
                         const gchar *message,
                         gpointer user_data);

gboolean squale_check_mem_block (char **data_pointer, gulong *allocated_bytes,
                                 gulong current_offset, gulong needed_bytes);
#endif /* __SQUALE_H__ */
