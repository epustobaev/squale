/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalejob.h : Header for SqualeJob object.
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

#ifndef __SQUALE_JOB_H__
#define __SQUALE_JOB_H__

#include <glib-object.h>
#include <sys/time.h>

#define SQUALE_TYPE_JOB            (squale_job_get_type ())
#define SQUALE_JOB(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_JOB, SqualeJob))
#define SQUALE_JOB_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_JOB, SqualeJobClass))
#define SQUALE_IS_JOB(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_JOB))
#define SQUALE_IS_JOB_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_JOB))
#define SQUALE_JOB_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_JOB, SqualeJobClass))

typedef struct _SqualeJob SqualeJob;
typedef struct _SqualeJobClass SqualeJobClass;
typedef struct _SqualeResultSet SqualeResultSet;

#define SQUALE_GLOBAL_STATS_ORDER     "squale_global_stats"
#define SQUALE_LOCAL_STATS_ORDER      "squale_local_stats"
#define SQUALE_SHUTDOWN_ORDER         "squale_shutdown"
#define SQUALE_GLOBAL_SHUTDOWN_ORDER  "squale_global_shutdown"
#define SQUALE_STARTUP_ORDER          "squale_startup"

typedef enum {
    SQUALE_JOB_NORMAL,
    SQUALE_JOB_GLOBAL_STATS,
    SQUALE_JOB_LOCAL_STATS,
    SQUALE_JOB_SHUTDOWN,
    SQUALE_JOB_GLOBAL_SHUTDOWN,
    SQUALE_JOB_STARTUP
} SqualeJobType;

typedef enum {
    SQUALE_JOB_PENDING,
    SQUALE_JOB_PROCESSING,
    SQUALE_JOB_COMPLETE
} SqualeJobStatus;

struct _SqualeResultSet
{
    char *data;
    gulong data_size;
    gulong allocated_memory;
};

struct _SqualeJob
{
    GObject object;

    GMutex *status_mutex;

    SqualeJobStatus status;
    SqualeJobType job_type;

    gint control_socket[2];

    char *query;
    SqualeResultSet *resultset;
    GError *error;
    GError *warning;
    gint32 affected_rows;

    struct timeval creation_ts;
    struct timeval assign_ts;
    struct timeval complete_ts;
};

struct _SqualeJobClass
{
    GObjectClass parent_class;
};

GType squale_job_get_type (void);

SqualeJob *squale_job_new (void);

gint32 squale_job_get_assignation_delay (SqualeJob *job);
gint32 squale_job_get_processing_time (SqualeJob *job);

gboolean squale_job_set_query (SqualeJob *job, const char *query);

gboolean squale_job_set_status_if_match (SqualeJob *job, SqualeJobStatus status,
                                         SqualeJobStatus match);

gboolean squale_job_complete_from_hashtable (SqualeJob *job, GHashTable *hash);

gboolean squale_job_set_error (SqualeJob *job, GError *error);
gboolean squale_job_set_warning (SqualeJob *job, GError *warning);

#endif /* __SQUALE_JOB_H__ */
