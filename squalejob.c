/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalejob.c : Source for SqualeJob object.
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
#include "squalejob.h"
#include "squale-i18n.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

static GObjectClass *parent_class = NULL;

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static void
squale_job_resultset_from_hash_foreach (gpointer key,
                                        gpointer value,
                                        gpointer user_data)
{
    gulong offset = 0;
    gint32 field_length = 0;
    SqualeResultSet *resultset = (SqualeResultSet *) user_data;

    offset = resultset->data_size;

    /* We pack key and value and update data_size */
    field_length = strlen ((char *)key);
    squale_check_mem_block (&(resultset->data),
                            &(resultset->allocated_memory), offset,
                            sizeof (gint32) + field_length);
    *(gint32 *)(resultset->data + offset) = field_length;
    offset += sizeof (gint32);
    memcpy (resultset->data + offset, key, field_length);
    offset += field_length;

    field_length = strlen ((char *)value);
    squale_check_mem_block (&(resultset->data),
                            &(resultset->allocated_memory), offset,
                            sizeof (gint32) + field_length);
    *(gint32 *)(resultset->data + offset) = field_length;
    offset += sizeof (gint32);
    memcpy (resultset->data + offset, value, field_length);
    offset += field_length;

    resultset->data_size = offset;
}

static gint32
squale_job_diff_millitime (struct timeval begin, struct timeval end)
{
    return ((end.tv_sec - begin.tv_sec) * 1000) + ((end.tv_usec - begin.tv_usec) / 1000);
}

/* =========================================== */
/*                                             */
/*              Init & Class init              */
/*                                             */
/* =========================================== */

static void
squale_job_dispose (GObject *object)
{
    SqualeJob *job = NULL;

    job = SQUALE_JOB (object);

    if (job->query) {
        g_free (job->query);
        job->query = NULL;
    }

    if (job->resultset) {
        g_message (_("Freeing resultset memory of job %p"), job);
        g_free (job->resultset);
        job->resultset = NULL;
    }

    if (job->error) {
        g_error_free (job->error);
        job->error = NULL;
    }

    if (job->warning) {
        g_error_free (job->warning);
        job->warning = NULL;
    }

    if (job->status_mutex) {
        g_mutex_free (job->status_mutex);
        job->status_mutex = NULL;
    }

    /* Closing our part of the socketpair because g_io_channel_shutdown closed
       the other one. */
    if (job->control_socket[1]) {
        close (job->control_socket[1]);
        job->control_socket[1] = 0;
    }

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_job_init (SqualeJob *job)
{
    job->status_mutex = g_mutex_new ();
    job->status = SQUALE_JOB_PENDING;

    if (socketpair (PF_UNIX, SOCK_STREAM, 0, job->control_socket) < 0) {
        g_warning ("cannot create io channel for job (%p)", job);
    }

    job->query = NULL;

    job->resultset = g_new0 (SqualeResultSet, 1);
    job->resultset->data = NULL;
    job->resultset->data_size = 0;
    job->resultset->allocated_memory = 0;

    job->error = NULL;
    job->warning = NULL;
    job->affected_rows = -1;

    gettimeofday (&(job->creation_ts), NULL);
    gettimeofday (&(job->assign_ts), NULL);
    gettimeofday (&(job->complete_ts), NULL);
}

static void
squale_job_class_init (SqualeJobClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->dispose = squale_job_dispose;
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

gboolean
squale_job_complete_from_hashtable (SqualeJob *job, GHashTable *hash)
{
    gint32 num_fields = 2, field_length;
    gulong offset = 0, num_rows = 0;

    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);
    g_return_val_if_fail (hash != NULL, FALSE);

    g_message (_("Generating resultset from hashtable %p for job %p"),
               hash, job);

    num_rows = (gulong) g_hash_table_size (hash);

    /* Let's allocate the needed memory in the resultset data buffer.
       We need 2 gint32 and a char for the header then we can start packing
       our data */
    job->resultset->allocated_memory = 1024;
    job->resultset->data = g_malloc0 (job->resultset->allocated_memory);

    /* Packing number of fields */
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            3 * sizeof (gint32) + sizeof (char));

    offset = 2 * sizeof (gint32) + sizeof (char);

    *(gint32 *)(job->resultset->data + offset) = num_fields;
    offset += sizeof (gint32);

    /* Packing columns names */
    field_length = strlen ("Name");
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            sizeof (gint32) + field_length);
    *(gint32 *)(job->resultset->data + offset) = field_length;
    offset += sizeof (gint32);
    memcpy (job->resultset->data + offset, "Name", field_length);
    offset += field_length;
    field_length = strlen ("Value");
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            sizeof (gint32) + field_length);
    *(gint32 *)(job->resultset->data + offset) = field_length;
    offset += sizeof (gint32);
    memcpy (job->resultset->data + offset, "Value", field_length);
    offset += field_length;

    /* Packing number of rows */
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            sizeof (gulong));
    *(gulong *)(job->resultset->data + offset) = num_rows;
    offset += sizeof (gulong);

    job->resultset->data_size = offset;

    /* Foreach row of hash table insert in data table  */
    g_hash_table_foreach (hash, squale_job_resultset_from_hash_foreach,
                          job->resultset);

    squale_job_set_status_if_match (job, SQUALE_JOB_COMPLETE,
                                    SQUALE_JOB_PENDING);

    return TRUE;
}

gint32
squale_job_get_assignation_delay (SqualeJob *job) {
    g_return_val_if_fail (SQUALE_IS_JOB (job), 0);

    return squale_job_diff_millitime (job->creation_ts, job->assign_ts);
}

gint32
squale_job_get_processing_time (SqualeJob *job) {
    g_return_val_if_fail (SQUALE_IS_JOB (job), 0);

    return squale_job_diff_millitime (job->assign_ts, job->complete_ts);
}

gboolean
squale_job_set_query (SqualeJob *job, const char *query)
{
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);
    g_return_val_if_fail (query != NULL, FALSE);

    if (job->query) {
        g_free (job->query);
        job->query = NULL;
    }

    g_message (_("Job %p received query %s"), job, query);

    job->query = g_strdup (query);

    if (g_str_has_prefix (query, SQUALE_GLOBAL_STATS_ORDER)) {
        job->job_type = SQUALE_JOB_GLOBAL_STATS;
    }
    else if (g_str_has_prefix (query, SQUALE_LOCAL_STATS_ORDER)) {
        job->job_type = SQUALE_JOB_LOCAL_STATS;
    }
    else if (g_str_has_prefix (query, SQUALE_SHUTDOWN_ORDER)) {
        job->job_type = SQUALE_JOB_SHUTDOWN;
    }
    else if (g_str_has_prefix (query, SQUALE_GLOBAL_SHUTDOWN_ORDER)) {
        job->job_type = SQUALE_JOB_GLOBAL_SHUTDOWN;
    }
    else if (g_str_has_prefix (query, SQUALE_STARTUP_ORDER)) {
        job->job_type = SQUALE_JOB_STARTUP;
    }
    else {
        job->job_type = SQUALE_JOB_NORMAL;
    }

    return TRUE;
}

gboolean
squale_job_set_error (SqualeJob *job, GError *error)
{
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    if (job->error) {
        g_error_free (job->error);
    }

    job->error = error;

    return TRUE;
}

gboolean
squale_job_set_warning (SqualeJob *job, GError *warning)
{
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    if (job->warning) {
        g_error_free (job->warning);
    }

    job->warning = warning;

    return TRUE;
}

gboolean
squale_job_set_status_if_match (SqualeJob *job, SqualeJobStatus status,
                                SqualeJobStatus match)
{
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    g_mutex_lock (job->status_mutex);

    if (job->status == match) {
        /* It's important to set the job status before updating timers or writing
           to the socketpair. Indeed there can be a race here */
        job->status = status;
        switch (status) {
            case SQUALE_JOB_PROCESSING:
                gettimeofday (&(job->assign_ts), NULL);
                break;
            case SQUALE_JOB_COMPLETE:
            {
                gchar c = 'c';
                gettimeofday (&(job->complete_ts), NULL);
                /* Let the main thread now we are complete */
                write (job->control_socket[1], &c, 1);
            }
                break;
            default:
                break;
        }
        g_mutex_unlock (job->status_mutex);
        return TRUE;
    }
    else {
        g_mutex_unlock (job->status_mutex);
        return FALSE;
    }

    return FALSE;
}

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_job_get_type (void)
{
    static GType job_type = 0;

    if (!job_type)
    {
        static const GTypeInfo job_info = {
                sizeof (SqualeJobClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_job_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeJob),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_job_init,
                NULL                    /* value_table */
        };

        job_type =
                g_type_register_static (G_TYPE_OBJECT, "SqualeJob",
                                        &job_info, (GTypeFlags) 0);
    }
    return job_type;
}

SqualeJob *
squale_job_new (void)
{
    SqualeJob *job = g_object_new (SQUALE_TYPE_JOB, NULL);

    return job;
}
