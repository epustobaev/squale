/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squaleworker.h : Header for SqualeWorker object.
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

#ifndef __SQUALE_WORKER_H__
#define __SQUALE_WORKER_H__

#include <glib-object.h>

typedef struct _SqualeWorker SqualeWorker;
typedef struct _SqualeWorkerClass SqualeWorkerClass;

#include "squalejoblist.h"

#define SQUALE_TYPE_WORKER            (squale_worker_get_type ())
#define SQUALE_WORKER(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_WORKER, SqualeWorker))
#define SQUALE_WORKER_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_WORKER, SqualeWorkerClass))
#define SQUALE_IS_WORKER(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_WORKER))
#define SQUALE_IS_WORKER_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_WORKER))
#define SQUALE_WORKER_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_WORKER, SqualeWorkerClass))

struct _SqualeWorker
{
    GObject object;

    GThread *thread;

    SqualeJobList *joblist;

    GMutex *status_mutex;
    char *status;

    /* States and requests */
    gboolean running;
    gboolean shutdown_requested;
    gboolean shutdown_complete;

    /* Number of jobs after which we cycle database connection */
    gulong cycle_after;
    gulong cycle_counter;

    /* Statistics */
    gulong nb_jobs_processed;
    gulong nb_errors;
    gulong nb_db_conn_cycles;
};

struct _SqualeWorkerClass
{
    GObjectClass parent_class;

    gboolean (*connect) (SqualeWorker *worker);
    gboolean (*disconnect) (SqualeWorker *worker);
    gpointer (*run) (gpointer worker);
};

GType squale_worker_get_type (void);

SqualeWorker *squale_worker_new (void);

gboolean squale_worker_connect (SqualeWorker *worker);
gboolean squale_worker_disconnect (SqualeWorker *worker);
gpointer squale_worker_run (gpointer worker);
void squale_worker_cycle_connection (SqualeWorker *worker);

void squale_worker_shutdown (SqualeWorker *worker);
gboolean squale_worker_check_shutdown (SqualeWorker *worker);
void squale_worker_shutdown_complete (SqualeWorker *worker);

void squale_worker_set_running (SqualeWorker *worker, gboolean running);
gboolean squale_worker_is_running (SqualeWorker *worker);

void squale_worker_set_joblist (SqualeWorker *worker, SqualeJobList *joblist);
SqualeJobList *squale_worker_get_joblist (SqualeWorker *worker);

void squale_worker_set_status (SqualeWorker *worker, const char *status);
char *squale_worker_get_status (SqualeWorker *worker);

SqualeJob *squale_worker_wait (SqualeWorker *worker);

#endif /* __SQUALE_WORKER_H__ */
