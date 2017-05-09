/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalejoblist.c : Source for SqualeJobList object.
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

#include "squalejoblist.h"
#include "squale-i18n.h"

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

enum
{
    SHUTDOWN,
    STARTUP,
    STATS,
    LAST_SIGNAL
};

static GObjectClass *parent_class = NULL;
static guint joblist_signals[LAST_SIGNAL] = { 0 };

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static GQuark
squale_joblist_error_quark (void)
{
    static GQuark quark = 0;
    if (quark == 0)
        quark = g_quark_from_static_string ("SQuaLe-joblist");
    return quark;
}

/* =========================================== */
/*                                             */
/*              Init & Class init              */
/*                                             */
/* =========================================== */

static void
squale_joblist_dispose (GObject *object)
{
    GList *workers = NULL, *jobs = NULL;
    SqualeJobList *joblist = NULL;

    joblist = SQUALE_JOBLIST (object);

    squale_joblist_clear (joblist);

    if (joblist->list_mutex) {
        g_mutex_free (joblist->list_mutex);
        joblist->list_mutex = NULL;
    }

    workers = joblist->workers;

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);
        if (SQUALE_IS_WORKER (worker)) {
            g_object_unref (worker);
        }
        workers = g_list_next (workers);
    }

    if (joblist->workers) {
        g_list_free (joblist->workers);
        joblist->workers = NULL;
    }

    jobs = joblist->jobs;

    while (jobs) {
        SqualeJob *job = SQUALE_JOB (jobs->data);
        if (SQUALE_IS_JOB (job)) {
            g_object_unref (job);
        }
        jobs = g_list_next (jobs);
    }

    if (joblist->jobs) {
        g_list_free (joblist->jobs);
        joblist->jobs = NULL;
    }

    if (joblist->cond) {
        g_cond_free (joblist->cond);
        joblist->cond = NULL;
    }

    if (joblist->name) {
        g_free (joblist->name);
        joblist->name = NULL;
    }

    if (joblist->backend) {
        g_free (joblist->backend);
        joblist->backend = NULL;
    }

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_joblist_init (SqualeJobList *joblist)
{
    joblist->name = NULL;
    joblist->backend = NULL;
    joblist->list_mutex = g_mutex_new ();
    joblist->cond = g_cond_new ();
    joblist->jobs = NULL;
    joblist->workers = NULL;
    joblist->max_pending_warn = 0;
    joblist->max_pending_block = 0;
    joblist->assign_total_time = 0;
    joblist->nb_assign = 0;
    joblist->process_total_time = 0;
    joblist->nb_process = 0;
    joblist->nb_errors = 0;
    joblist->status = SQUALE_JOBLIST_OPENED;
    gettimeofday (&(joblist->startup_ts), NULL);
}

static void
squale_joblist_class_init (SqualeJobListClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->dispose = squale_joblist_dispose;

    joblist_signals[SHUTDOWN] =
            g_signal_new ("shutdown",
                          G_TYPE_FROM_CLASS (klass),
                          G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeJobListClass, shutdown),
                          NULL, NULL, g_cclosure_marshal_VOID__VOID, G_TYPE_NONE, 0);
    joblist_signals[STARTUP] =
            g_signal_new ("startup",
                          G_TYPE_FROM_CLASS (klass),
                          G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeJobListClass, startup),
                          NULL, NULL, g_cclosure_marshal_VOID__VOID, G_TYPE_NONE, 0);
    joblist_signals[STATS] =
            g_signal_new ("stats",
                          G_TYPE_FROM_CLASS (klass),
                          G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeJobListClass, stats),
                          NULL, NULL, g_cclosure_marshal_VOID__POINTER, G_TYPE_NONE,
                          1, G_TYPE_POINTER);
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

gboolean
squale_joblist_set_status (SqualeJobList *joblist, SqualeJobListStatus status)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);

    if (joblist->status == SQUALE_JOBLIST_CLOSED &&
        status == SQUALE_JOBLIST_OPENED) {
        /* Resetting some stats */
        gettimeofday (&(joblist->startup_ts), NULL);
        joblist->nb_process = joblist->nb_assign = joblist->nb_errors = 0;
        joblist->assign_total_time = joblist->process_total_time = 0;
    }

    joblist->status = status;

    return TRUE;
}

gboolean
squale_joblist_get_stats (SqualeJobList *joblist, GHashTable *hash)
{
    GList *jobs = NULL, *workers = NULL;
    guint pending_jobs = 0, nb_jobs = 0, nb_workers = 0;
    struct timeval current_time;

    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (hash != NULL, FALSE);

    g_message (_("Generating local joblist stats for joblist %s"), joblist->name);

    g_mutex_lock (joblist->list_mutex);

    nb_jobs = g_list_length (joblist->jobs);

    jobs = joblist->jobs;

    while (jobs) {
        SqualeJob *job = SQUALE_JOB (jobs->data);
        if (job->status == SQUALE_JOB_PENDING)
            pending_jobs++;
        jobs = g_list_next (jobs);
    }

    g_mutex_unlock (joblist->list_mutex);

    workers = joblist->workers;

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);

        if (SQUALE_IS_WORKER (worker)) {
            nb_workers++;
            g_hash_table_insert (hash,
                                 g_strdup_printf ("%s_%d_%s", _("worker"), nb_workers,
                                                  _("reconnections")),
                                 g_strdup_printf ("%lu", worker->nb_db_conn_cycles));
            g_hash_table_insert (hash,
                                 g_strdup_printf ("%s_%d_%s", _("worker"), nb_workers, _("errors")),
                                 g_strdup_printf ("%lu", worker->nb_errors));
            g_hash_table_insert (hash,
                                 g_strdup_printf ("%s_%d_%s", _("worker"), nb_workers,
                                                  _("processed_jobs")),
                                 g_strdup_printf ("%lu", worker->nb_jobs_processed));
            g_hash_table_insert (hash,
                                 g_strdup_printf ("%s_%d_%s", _("worker"), nb_workers, _("status")),
                                 squale_worker_get_status (worker));
        }
        workers = g_list_next (workers);
    }

    g_hash_table_insert (hash, g_strdup (_("nb_workers")),
                         g_strdup_printf ("%d", nb_workers));

    g_hash_table_insert (hash, g_strdup (_("pending_jobs")),
                         g_strdup_printf ("%d", pending_jobs));
    g_hash_table_insert (hash, g_strdup (_("jobs_in_list")),
                         g_strdup_printf ("%d", nb_jobs));
    g_hash_table_insert (hash, g_strdup (_("processed_jobs")),
                         g_strdup_printf ("%lu", joblist->nb_process));
    g_hash_table_insert (hash, g_strdup (_("errors")),
                         g_strdup_printf ("%lu", joblist->nb_errors));
    g_hash_table_insert (hash, g_strdup (_("backend")),
                         g_strdup (joblist->backend));

    if (joblist->nb_assign) {
        g_hash_table_insert (hash, g_strdup (_("avg_assign_delay (ms)")),
                             g_strdup_printf ("%lu",
                                              joblist->assign_total_time / joblist->nb_assign));
    }
    else {
        g_hash_table_insert (hash, g_strdup (_("avg_assign_delay (ms)")), g_strdup ("0"));
    }
    if (joblist->nb_process) {
        g_hash_table_insert (hash, g_strdup (_("avg_process_time (ms)")),
                             g_strdup_printf ("%lu",
                                              joblist->process_total_time / joblist->nb_process));
    }
    else {
        g_hash_table_insert (hash, g_strdup (_("avg_process_time (ms)")),
                             g_strdup ("0"));
    }

    /* If the main loop wants to hook up some more stats */
    g_signal_emit (joblist, joblist_signals[STATS], 0, hash, NULL);

    /* Calculating uptime */
    gettimeofday (&(current_time), NULL);

    g_hash_table_insert (hash, g_strdup (_("uptime (s)")),
                         g_strdup_printf ("%lu",
                                          current_time.tv_sec - joblist->startup_ts.tv_sec));

    return TRUE;
}

void
squale_joblist_startup (SqualeJobList *joblist)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    g_signal_emit (joblist, joblist_signals[STARTUP], 0, NULL);
}

void
squale_joblist_shutdown (SqualeJobList *joblist)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    g_signal_emit (joblist, joblist_signals[SHUTDOWN], 0, NULL);
}

char *
squale_joblist_get_name (SqualeJobList *joblist)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), NULL);

    if (joblist->name) {
        return g_strdup (joblist->name);
    }
    else {
        return NULL;
    }
}

void
squale_joblist_set_name (SqualeJobList *joblist, const char *name)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));
    g_return_if_fail (name != NULL);

    if (joblist->name)
        g_free (joblist->name);

    joblist->name = g_strdup (name);
}

char *
squale_joblist_get_backend (SqualeJobList *joblist)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), NULL);

    if (joblist->backend) {
        return g_strdup (joblist->backend);
    }
    else {
        return NULL;
    }
}

void
squale_joblist_set_backend (SqualeJobList *joblist, const char *backend)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));
    g_return_if_fail (backend != NULL);

    if (joblist->backend)
        g_free (joblist->backend);

    joblist->backend = g_strdup (backend);
}

gboolean
squale_joblist_add_job (SqualeJobList *joblist, SqualeJob *job, GError **error)
{
    GList *jobs = NULL, *workers = NULL;
    gboolean running_workers = FALSE;

    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    /* Check if we have some running workers to process that job */
    workers = joblist->workers;

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);
        if (SQUALE_IS_WORKER (worker)) {
            if (squale_worker_is_running (worker)) {
                running_workers = TRUE;
            }
        }
        workers = g_list_next (workers);
    }

    if ((joblist->status == SQUALE_JOBLIST_CLOSED) || (!running_workers)) {
        g_set_error (error, squale_joblist_error_quark (), 0,
                     _("Joblist %s is currently in closed state"), joblist->name);
        g_message (_("Declining addition of job %p to joblist %s because " \
        "it's closed"), job, joblist->name);
        return FALSE;
    }

    g_message (_("Adding job %p to joblist %s"), job, joblist->name);

    /* We steal the reference of that job */
    g_mutex_lock (joblist->list_mutex);

    /* If a warn/block level is defined we crawl in the list to look how many
     * pending job are hanging there */
    if (joblist->max_pending_warn || joblist->max_pending_block) {
        guint pending_jobs = 0;

        jobs = joblist->jobs;

        while (jobs) {
            SqualeJob *job = SQUALE_JOB (jobs->data);
            if (job->status == SQUALE_JOB_PENDING)
                pending_jobs++;
            jobs = g_list_next (jobs);
        }

        if (pending_jobs >= joblist->max_pending_block &&
            joblist->max_pending_block) {
            g_mutex_unlock (joblist->list_mutex);
            g_set_error (error, squale_joblist_error_quark (), 0,
                         _("Joblist %s is currently blocked, too many pending jobs (%d)"),
                         joblist->name, pending_jobs);
            g_warning (_("Declining addition of job %p to joblist %s because " \
          "it's blocked, too many pending jobs (%d)"), job, joblist->name,
                       pending_jobs);
            return FALSE;
        }
        if (pending_jobs >= joblist->max_pending_warn &&
            joblist->max_pending_warn) {
            g_warning ("we have %d pending jobs in joblist %s (%d allowed)",
                       pending_jobs, joblist->name, joblist->max_pending_warn);
        }
    }

    joblist->jobs = g_list_append (joblist->jobs, job);

    /* We signal that a job has been added to wake up the waiting worker
    threads */
    g_cond_signal (joblist->cond);

    g_mutex_unlock (joblist->list_mutex);

    return TRUE;
}

gboolean
squale_joblist_remove_job (SqualeJobList *joblist, SqualeJob *job)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    g_message (_("Removing job %p from joblist %s"), job, joblist->name);

    g_mutex_lock (joblist->list_mutex);

    joblist->jobs = g_list_remove (joblist->jobs, job);

    g_mutex_unlock (joblist->list_mutex);

    /* Compute stats */
    joblist->assign_total_time += squale_job_get_assignation_delay (job);
    joblist->nb_assign++;

    if (job->status == SQUALE_JOB_COMPLETE) {
        joblist->process_total_time += squale_job_get_processing_time (job);
        joblist->nb_process++;
        if (job->error) {
            joblist->nb_errors++;
        }
    }

    /* We unref that job as we have stolen the reference when adding */
    g_object_unref (job);

    return TRUE;
}

/* This function locks the joblist and crawls inside to find a pending job
   and assign it. If keep_locking is TRUE this function will not unlock the
   joblist mutex when no pending job is found. This is used to do an atomic
   switch to waiting mode in the worker being sure that the list is not touched
   meanwhile */
SqualeJob *
squale_joblist_assign_pending_job (SqualeJobList *joblist,
                                   gboolean keep_locking)
{
    GList *jobs = NULL;

    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), NULL);

    g_mutex_lock (joblist->list_mutex);

    jobs = joblist->jobs;

    while (jobs) {
        SqualeJob *job = SQUALE_JOB (jobs->data);

        if (SQUALE_IS_JOB (job)) {
            gboolean ret = FALSE;

            /* If this function returns TRUE we found a pending job and mark it as
               processing while locked so that no worker can steal it at the same
               time */
            ret = squale_job_set_status_if_match (job, SQUALE_JOB_PROCESSING,
                                                  SQUALE_JOB_PENDING);
            if (ret) {
                /* We return the job with an incremented reference count. Before the
                 * reference was incremented in the worker_run function but that can
                 * get us into a race if the job refcount is decremented because of
                 * client disconnection between that function returns and the ref is
                 * incremented. */
                g_object_ref (job);
                g_mutex_unlock (joblist->list_mutex);
                g_message (_("Found pending job %p in joblist %s"), job, joblist->name);
                return job;
            }
        }

        jobs = g_list_next (jobs);
    }

    /* We don't unlock the joblist as g_cond_wait will do that in the worker */
    if (!keep_locking) {
        g_mutex_unlock (joblist->list_mutex);
    }

    return NULL;
}

/* This function allows a worker to give up on a specific job. This will just
 * remove the job from the joblist, change its status back to pending and add
 * it again. */
gboolean
squale_joblist_giveup_job (SqualeJobList *joblist, SqualeJob *job)
{
    gboolean ret = FALSE;
    GError *error = NULL;

    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    /* We take an extra ref to the job, as removing will unref */
    g_object_ref (job);
    /* Remove job from the list */
    ret = squale_joblist_remove_job (joblist, job);
    if (G_UNLIKELY (!ret)) {
        g_warning ("failed removing job %p from joblist %p", job, joblist);
        g_object_unref (job);
        goto beach;
    }
    /* Change status back to pending */
    ret = squale_job_set_status_if_match (job, SQUALE_JOB_PENDING,
                                          SQUALE_JOB_PROCESSING);
    if (G_UNLIKELY (!ret)) {
        g_warning ("job %p was not in processing status", job);
        goto beach;
    }
    /* Add the job back, this will steal our reference */
    ret = squale_joblist_add_job (joblist, job, &error);
    if (G_UNLIKELY (!ret)) {
        g_warning ("failed adding job %p to joblist %p", job, joblist);
        if (error) {
            squale_job_set_error (job, error);
        }
        /* Declare the job as COMPLETE */
        squale_job_set_status_if_match (job, SQUALE_JOB_COMPLETE,
                                        SQUALE_JOB_PENDING);
    }

    beach:
    return ret;
}

void
squale_joblist_set_max_pending_warn_level (SqualeJobList *joblist,
                                           guint max_pending)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    joblist->max_pending_warn = max_pending;
}

void
squale_joblist_set_max_pending_block_level (SqualeJobList *joblist,
                                            guint max_pending)
{
    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    joblist->max_pending_block = max_pending;
}

gboolean
squale_joblist_clear (SqualeJobList *joblist)
{
    GList *jobs = NULL;

    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);

    g_message (_("Removing all jobs from joblist %s"), joblist->name);

    g_mutex_lock (joblist->list_mutex);

    jobs = joblist->jobs;

    while (jobs) {
        SqualeJob *job = SQUALE_JOB (jobs->data);

        if (SQUALE_IS_JOB (job))
            g_object_unref (job);

        jobs = g_list_next (jobs);
    }

    g_list_free (joblist->jobs);

    joblist->jobs = NULL;

    g_mutex_unlock (joblist->list_mutex);

    return TRUE;
}

gboolean
squale_joblist_add_worker (SqualeJobList *joblist, SqualeWorker *worker)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);

    g_object_ref (worker);
    joblist->workers = g_list_append (joblist->workers, worker);

    return TRUE;
}

gboolean
squale_joblist_remove_worker (SqualeJobList *joblist, SqualeWorker *worker)
{
    g_return_val_if_fail (SQUALE_IS_JOBLIST (joblist), FALSE);
    g_return_val_if_fail (SQUALE_IS_WORKER (worker), FALSE);

    joblist->workers = g_list_remove (joblist->workers, worker);
    g_object_unref (worker);

    return TRUE;
}

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_joblist_get_type (void)
{
    static GType joblist_type = 0;

    if (!joblist_type) {
        static const GTypeInfo joblist_info = {
                sizeof (SqualeJobListClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_joblist_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeJobList),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_joblist_init,
                NULL                    /* value_table */
        };

        joblist_type = g_type_register_static (G_TYPE_OBJECT, "SqualeJobList",
                                               &joblist_info, (GTypeFlags) 0);
    }
    return joblist_type;
}

SqualeJobList *
squale_joblist_new (void)
{
    SqualeJobList *joblist = g_object_new (SQUALE_TYPE_JOBLIST, NULL);

    return joblist;
}
