/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squaleworker.c : Source for SqualeWorker object.
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

#include "squaleworker.h"
#include "squale-i18n.h"

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

#include <signal.h>
#include <unistd.h>
#include <stdlib.h>

enum
{
    PROP_0,
    PROP_CYCLE_AFTER
};

static GObjectClass *parent_class = NULL;

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static void
squale_worker_set_property (GObject *object, guint prop_id,
                            const GValue *value, GParamSpec *pspec)
{
    SqualeWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_WORKER (object));

    worker = SQUALE_WORKER (object);

    switch (prop_id)
    {
        case PROP_CYCLE_AFTER:
            worker->cycle_after = atoi (g_value_get_string (value));
            break;
        default :
            G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
            break;
    }
}

static void
squale_worker_get_property (GObject *object, guint prop_id,
                            GValue *value, GParamSpec *pspec)
{
    SqualeWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_WORKER (object));

    worker = SQUALE_WORKER (object);

    switch (prop_id)
    {
        case PROP_CYCLE_AFTER:
            g_value_set_string (value,
                                g_strdup_printf ("%lu", worker->cycle_after));
            break;
        default :
            G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
            break;
    }
}

/* =========================================== */
/*                                             */
/*              Init & Class init              */
/*                                             */
/* =========================================== */

static void
squale_worker_dispose (GObject *object)
{
    SqualeWorker *worker = NULL;

    worker = SQUALE_WORKER (object);

    if (worker->status) {
        g_free (worker->status);
        worker->status = NULL;
    }

    if (worker->status_mutex) {
        g_mutex_free (worker->status_mutex);
        worker->status_mutex = NULL;
    }

    if (SQUALE_IS_JOBLIST (worker->joblist)) {
        g_object_unref (worker->joblist);
        worker->joblist = NULL;
    }

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_worker_init (SqualeWorker *worker)
{
    worker->thread = NULL;
    worker->joblist = NULL;
    worker->status = NULL;
    worker->status_mutex = g_mutex_new ();
    worker->shutdown_requested = FALSE;
    worker->shutdown_complete = FALSE;
    worker->running = FALSE;
    worker->cycle_after = 0;
    worker->cycle_counter = 0;
    worker->nb_errors = 0;
    worker->nb_jobs_processed = 0;
    worker->nb_db_conn_cycles = 0;
}

static void
squale_worker_class_init (SqualeWorkerClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->dispose = squale_worker_dispose;
    gobject_class->set_property = squale_worker_set_property;
    gobject_class->get_property = squale_worker_get_property;

    g_object_class_install_property (gobject_class, PROP_CYCLE_AFTER,
                                     g_param_spec_string ("cycle-after",
                                                          "Jobs threshold for connection cycling",
                                                          "The number of jobs after which the worker cycles the database " \
          "connection", NULL, G_PARAM_READWRITE));
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

gboolean
squale_worker_connect (SqualeWorker *worker)
{
    SqualeWorkerClass *class;

    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);

    class = SQUALE_WORKER_GET_CLASS (worker);

    if (class->connect)
        return class->connect (worker);
    else
        return FALSE;
}

gboolean
squale_worker_disconnect (SqualeWorker *worker)
{
    SqualeWorkerClass *class;

    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);

    class = SQUALE_WORKER_GET_CLASS (worker);

    if (class->disconnect)
        return class->disconnect (worker);
    else
        return FALSE;
}

gpointer
squale_worker_run (gpointer worker)
{
    SqualeWorkerClass *class;
    sigset_t mask;

    g_return_val_if_fail (SQUALE_IS_WORKER (worker), NULL);

    class = SQUALE_WORKER_GET_CLASS (worker);

    /* Disconnecting signals */
    sigfillset (&mask);
    pthread_sigmask (SIG_BLOCK, &mask, NULL);

    if (class->run)
        return class->run (worker);
    else
        return NULL;
}

void
squale_worker_cycle_connection (SqualeWorker *worker)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));

    if (worker->cycle_after) {
        worker->cycle_counter++;
        if (worker->cycle_counter >= worker->cycle_after) {
            g_message (_("Cycling database connection for worker %p after %lu jobs"),
                       worker, worker->cycle_counter);
            squale_worker_disconnect (worker);
            squale_worker_connect (worker);
            worker->cycle_counter = 0;
        }
    }
}

gboolean
squale_worker_check_shutdown (SqualeWorker *worker)
{
    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);

    return worker->shutdown_requested;
}

void
squale_worker_shutdown_complete (SqualeWorker *worker)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));

    worker->shutdown_complete = TRUE;
}

void
squale_worker_shutdown (SqualeWorker *worker)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));

    if (!squale_worker_is_running (worker))
        return;

    g_message (_("Shutting down worker %p"), worker);

    worker->shutdown_requested = TRUE;

    /* We lock the joblist and signal the cond to the worker to wake it up. If
       we don't lock the joblist here we can have the shutdown being requested
       between the shutdown check and the g_cond_wait (tricky race huh ?) */
    if (SQUALE_IS_JOBLIST (worker->joblist)) {
        g_mutex_lock (worker->joblist->list_mutex);
        g_cond_broadcast (worker->joblist->cond);
        g_mutex_unlock (worker->joblist->list_mutex);
    }

    while (!worker->shutdown_complete)
        usleep (1000);

    worker->shutdown_complete = worker->shutdown_requested = FALSE;
}

void
squale_worker_set_running (SqualeWorker *worker, gboolean running)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));

    if (running) {
        squale_worker_set_status (worker, _("Running"));
    }
    else {
        squale_worker_set_status (worker, _("Stopped"));
    }

    worker->running = running;
}

gboolean
squale_worker_is_running (SqualeWorker *worker)
{
    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);
    return worker->running;
}

SqualeJobList *
squale_worker_get_joblist (SqualeWorker *worker)
{
    g_return_val_if_fail (SQUALE_IS_WORKER (worker), NULL);

    if (SQUALE_IS_JOBLIST (worker->joblist)) {
        g_object_ref (worker->joblist);
        return worker->joblist;
    }
    else {
        return NULL;
    }
}

void
squale_worker_set_joblist (SqualeWorker *worker, SqualeJobList *joblist)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    /* What happens if we change the joblist while worker runs */
    if (joblist == worker->joblist)
        return;

    g_message (_("Assigning joblist %p to worker %p"), joblist, worker);

    if (worker->joblist) {
        squale_joblist_remove_worker (worker->joblist, worker);
        g_object_unref (worker->joblist);
    }

    g_object_ref (joblist);
    worker->joblist = joblist;

    squale_joblist_add_worker (joblist, worker);
}

void
squale_worker_set_status (SqualeWorker *worker, const char *status)
{
    g_return_if_fail (SQUALE_IS_WORKER (worker));

    g_mutex_lock (worker->status_mutex);

    if (worker->status)
        g_free (worker->status);

    worker->status = g_strdup (status);

    g_mutex_unlock (worker->status_mutex);
}

char *
squale_worker_get_status (SqualeWorker *worker)
{
    char *status = NULL;

    g_return_val_if_fail (SQUALE_IS_WORKER (worker), NULL);

    g_mutex_lock (worker->status_mutex);

    if (worker->status)
        status = g_strdup (worker->status);

    g_mutex_unlock (worker->status_mutex);

    return status;
}

SqualeJob *
squale_worker_wait (SqualeWorker *worker)
{
    SqualeJob *job = NULL;

    g_return_val_if_fail (SQUALE_IS_WORKER (worker), NULL);

    /* We get a job and keep locking the joblist to block the main thread */
    job = squale_joblist_assign_pending_job (worker->joblist, TRUE);

    /* Atomic unlock of the joblist mutex, we are sure that no signal can be sent
       before we are actually waiting on the cond. */
    if (job == NULL) {
        /* Before waiting we make sure a shutdown has not been requested */
        if (!worker->shutdown_requested) {
            g_cond_wait (worker->joblist->cond, worker->joblist->list_mutex);
        }
        g_mutex_unlock (worker->joblist->list_mutex);
    }

    return job;
}

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_worker_get_type (void)
{
    static GType worker_type = 0;

    if (!worker_type) {
        static const GTypeInfo worker_info = {
                sizeof (SqualeWorkerClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_worker_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeWorker),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_worker_init,
                NULL                    /* value_table */
        };

        worker_type = g_type_register_static (G_TYPE_OBJECT, "SqualeWorker",
                                              &worker_info, (GTypeFlags) 0);
    }
    return worker_type;
}

SqualeWorker *
squale_worker_new (void)
{
    SqualeWorker *worker = g_object_new (SQUALE_TYPE_WORKER, NULL);

    return worker;
}
