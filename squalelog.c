/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "squale.h"
#include <fcntl.h>
#include <errno.h>
#include <time.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

void
squale_log_handler (const gchar *log_domain,
                    GLogLevelFlags log_level,
                    const gchar *message,
                    gpointer user_data)
{
    time_t now;
    char *datetime = NULL, *str_level = NULL, *final_msg = NULL;
    Squale *squale = (Squale *) user_data;

    if (log_level > squale->log_level) {
        return;
    }

    g_mutex_lock (squale->log_mutex);

    now = time (NULL);
    datetime = ctime (&now);
    datetime[strlen (datetime) - 1] = '\0';

    switch (log_level) {
        case G_LOG_LEVEL_ERROR:
            str_level = g_strdup (_("Error"));
            break;
        case G_LOG_LEVEL_CRITICAL:
            str_level = g_strdup (_("Critical"));
            break;
        case G_LOG_LEVEL_WARNING:
            str_level = g_strdup (_("Warning"));
            break;
        case G_LOG_LEVEL_MESSAGE:
            str_level = g_strdup (_("Message"));
            break;
        case G_LOG_LEVEL_INFO:
            str_level = g_strdup (_("Info"));
            break;
        case G_LOG_LEVEL_DEBUG:
            str_level = g_strdup (_("Debug"));
            break;
        default:
            str_level = g_strdup (_("Unknown"));
            break;
    }

    final_msg = g_strjoin (" - ", datetime, log_domain, str_level, message, NULL);

    if (str_level)
        g_free (str_level);

    if (squale->log_fd) {
        gint return_code;
        return_code = fprintf (squale->log_fd, "%s\n", final_msg);
        if (return_code < 0) {
            if (squale->no_detach) {
                g_print ("Can't write to log file: %s\n", strerror (errno));
            }
        }
        else {
            if (fflush (squale->log_fd) != 0) {
                if (squale->no_detach) {
                    g_print ("Can't write to log file: %s\n", strerror (errno));
                }
            }
        }
    }
    else {
        /* We only print something if we are in no detach mode, because FDS 0,1 & 2
           are closed otherwise */
        if (squale->no_detach) {
            g_print ("%s\n", final_msg);
        }
    }

    if (final_msg)
        g_free (final_msg);

    g_mutex_unlock (squale->log_mutex);
}
