/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalemysqlworker.c : Source for SqualeMysqlWorker object.
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
#include "squalemysqlworker.h"
#include "squale-i18n.h"
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

enum
{
    PROP_0,
    PROP_HOST,
    PROP_PORT,
    PROP_USER,
    PROP_PASSWD,
    PROP_DBNAME
};

static SqualeWorkerClass *parent_class = NULL;

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static GQuark
squale_mysql_worker_error_quark (void)
{
    static GQuark quark = 0;
    if (quark == 0)
        quark = g_quark_from_static_string ("SQuaLe-MySQL");
    return quark;
}

static gboolean
squale_mysql_worker_store_resultset (SqualeMysqlWorker *my_worker,
                                     SqualeJob *job, MYSQL_RES *result)
{
    MYSQL_FIELD *fields;
    MYSQL_ROW row;
    gint32 num_fields = 0, i;
    gulong offset = 0, num_rows = 0, j;

    g_return_val_if_fail (SQUALE_IS_MYSQL_WORKER (my_worker), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);
    g_return_val_if_fail (result != NULL, FALSE);

    num_fields = (gint32) mysql_num_fields (result);
    num_rows = (gulong) mysql_num_rows (result);

    /* Let's allocate the needed memory in the resultset data buffer.
       We need 2 gint32 and a char for the header then we can start packing
       our data */
    job->resultset->allocated_memory = SQUALE_PAGE_SIZE;
    job->resultset->data = g_malloc0 (job->resultset->allocated_memory);

    /* Packing number of fields */
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            3 * sizeof (gint32) + sizeof (char));

    offset = 2 * sizeof (gint32) + sizeof (char);

    *(gint32 *)(job->resultset->data + offset) = num_fields;
    offset += sizeof (gint32);

    /* Getting columns names */
    fields = mysql_fetch_fields (result);

    /* Packing columns names */
    for (i = 0; i < num_fields; i++) {
        gint32 field_length = 0;
        if (fields[i].name) {
            field_length = strlen (fields[i].name);
        }
        squale_check_mem_block (&(job->resultset->data),
                                &(job->resultset->allocated_memory), offset,
                                sizeof (gint32) + field_length);
        *(gint32 *)(job->resultset->data + offset) = field_length;
        offset += sizeof (gint32);
        memcpy (job->resultset->data + offset, fields[i].name, field_length);
        offset += field_length;
    }

    /* Packing number of rows */
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            sizeof (gulong));
    *(gulong *)(job->resultset->data + offset) = num_rows;
    offset += sizeof (gulong);

    /* Packing data row by row */
    for (j = 0; j < num_rows; j++) {
        row = mysql_fetch_row (result);
        for (i = 0; i < num_fields; i++) {
            gint32 field_length = 0;
            if (row[i]) {
                field_length = strlen (row[i]);
            }
            squale_check_mem_block (&(job->resultset->data),
                                    &(job->resultset->allocated_memory),
                                    offset, sizeof (gint32) + field_length);
            *(gint32 *)(job->resultset->data + offset) = field_length;
            offset += sizeof (gint32);
            memcpy (job->resultset->data + offset, row[i], field_length);
            offset += field_length;
        }
    }

    job->resultset->data_size = offset;

    return TRUE;
}

static gboolean
squale_mysql_worker_connect (SqualeWorker *worker)
{
    SqualeMysqlWorker *my_worker = NULL;
    SqualeJobList *worker_joblist = NULL;
    char *joblist_name = NULL;
    guint nb_tries = 1;
    gboolean connected = FALSE;

    g_return_val_if_fail (SQUALE_IS_MYSQL_WORKER (worker), FALSE);

    my_worker = SQUALE_MYSQL_WORKER (worker);
    worker_joblist = squale_worker_get_joblist (worker);
    if (SQUALE_IS_JOBLIST (worker_joblist)) {
        joblist_name = squale_joblist_get_name (worker_joblist);
        g_object_unref (worker_joblist);
        worker_joblist = NULL;
    }

    squale_worker_set_status (worker, _("Connecting"));

    while (!connected && !squale_worker_check_shutdown (worker)) {

        g_message (_("Joblist '%s': MySQL worker (%p) trying to establish " \
               "connection to %s (attempt %d)"), joblist_name, worker,
                   my_worker->dbname, nb_tries);

        if (!mysql_real_connect (&(my_worker->mysql), my_worker->host,
                                 my_worker->user, my_worker->passwd,
                                 my_worker->dbname, my_worker->port, NULL, 0)) {
            g_warning (_("Joblist '%s': Connection attempt %d to MySQL database %s " \
                 "failed for worker (%p): %s"), joblist_name, nb_tries,
                       my_worker->dbname, worker, mysql_error (&(my_worker->mysql)));
            /* Wait a little before trying again */
            sleep (1);
        }
        else {
            g_message (_("Joblist '%s': MySQL worker (%p) successfully connected " \
                 "to %s"), joblist_name, worker, my_worker->dbname);
            /* And we are connected */
            connected = TRUE;
#if MYSQL_VERSION_ID >= 50000
            /* As of MySQL 5.0.3 the default behaviour of mysql_ping is to not
         reconnect automatically. Force the old behaviour */
      mysql_options (&(my_worker->mysql), MYSQL_OPT_RECONNECT,
          (const char *) &connected);
#endif
        }
        nb_tries++;
    }

    if (joblist_name)
        g_free (joblist_name);

    return connected;
}

static gboolean
squale_mysql_worker_disconnect (SqualeWorker *worker)
{
    SqualeMysqlWorker *my_worker = NULL;
    SqualeJobList *worker_joblist = NULL;
    char *joblist_name = NULL;

    g_return_val_if_fail (SQUALE_IS_MYSQL_WORKER (worker), FALSE);

    my_worker = SQUALE_MYSQL_WORKER (worker);
    worker_joblist = squale_worker_get_joblist (worker);
    if (SQUALE_IS_JOBLIST (worker_joblist)) {
        joblist_name = squale_joblist_get_name (worker_joblist);
        g_object_unref (worker_joblist);
        worker_joblist = NULL;
    }

    squale_worker_set_status (worker, _("Disconnecting"));

    g_message (_("Joblist '%s': MySQL worker (%p) shutting down connection to " \
             "%s"), joblist_name, worker, my_worker->dbname);

    mysql_close (&(my_worker->mysql));

    if (joblist_name)
        g_free (joblist_name);

    return TRUE;
}

static gpointer
squale_mysql_worker_run (gpointer worker)
{
    SqualeMysqlWorker *my_worker = NULL;
    SqualeJobList *joblist = NULL;
    SqualeJob *job = NULL;

    g_return_val_if_fail (SQUALE_IS_MYSQL_WORKER (worker), FALSE);

    my_worker = SQUALE_MYSQL_WORKER (worker);
    joblist = SQUALE_WORKER (my_worker)->joblist;

    squale_worker_set_running (SQUALE_WORKER (my_worker), TRUE);

    if (!squale_worker_connect (SQUALE_WORKER (my_worker))) {
        goto beach;
    }

    squale_worker_set_status (SQUALE_WORKER (my_worker), _("Sleeping"));

    /* The worker looping forever until shutdown is requested */
    while (!squale_worker_check_shutdown (SQUALE_WORKER (my_worker))) {

        if (job == NULL) {
            job = squale_joblist_assign_pending_job (joblist, FALSE);
        }

        if (SQUALE_IS_JOB (job)) {
            MYSQL_RES *result;

            squale_worker_set_status (SQUALE_WORKER (my_worker), job->query);

            if (mysql_ping (&(my_worker->mysql)) != 0) {
                /* Our connection died, release the reference to that job and make it
                 * available again on the job list */
                g_warning (_("MySQL worker (%p) lost connection to %s, giving up " \
                   "on job %p and reconnecting"), worker, my_worker->dbname,
                           job);
                squale_joblist_giveup_job (joblist, job);
                /* The job has been added back to the list, we still have our reference,
                 * so release it */
                g_object_unref (job);
                job = NULL;
                /* Make sure we clean connection correctly */
                squale_worker_disconnect (SQUALE_WORKER (my_worker));
                /* And start the connection loop */
                squale_worker_connect (SQUALE_WORKER (my_worker));
                /* Count our reconnections */
                SQUALE_WORKER (my_worker)->nb_db_conn_cycles++;
                /* Restart the infinite loop */
                continue;
            }

            /* We have a job assigned to us */
            if (mysql_query (&(my_worker->mysql), job->query)) {
                /* Error: Query failed */
                GError *error = g_error_new (squale_mysql_worker_error_quark (), 0,
                                             "%s", mysql_error (&(my_worker->mysql)));
                squale_job_set_error (job, error);
                SQUALE_WORKER (my_worker)->nb_errors++;
            }
            else {
                result = mysql_store_result (&(my_worker->mysql));
                if (result) {
                    /* Storing resultset */
                    squale_mysql_worker_store_resultset (my_worker, job, result);
                    mysql_free_result (result);
                }
                else {
                    if (mysql_field_count (&(my_worker->mysql)) == 0) {
                        /* The query was not supposed to return data */
                        job->affected_rows = mysql_affected_rows (&(my_worker->mysql));
                    }
                    else {
                        /* Error: Query succeeded but no fields returned when we were
                           expecting some */
                        GError *error = g_error_new (squale_mysql_worker_error_quark (), 0,
                                                     "%s",
                                                     mysql_error (&(my_worker->mysql)));
                        squale_job_set_error (job, error);
                        SQUALE_WORKER (my_worker)->nb_errors++;
                    }
                }
            }

            squale_job_set_status_if_match (job, SQUALE_JOB_COMPLETE,
                                            SQUALE_JOB_PROCESSING);

            SQUALE_WORKER (my_worker)->nb_jobs_processed++;

            g_object_unref (job);
            job = NULL;

            squale_worker_cycle_connection (SQUALE_WORKER (my_worker));

            squale_worker_set_status (SQUALE_WORKER (my_worker), _("Sleeping"));
        }
        else {
            /* No pending job in the joblist let's wait for a new job, while switching
               to waiting state we check the joblist for a pending job. If there is
               one we return it and we don't wait */
            job = squale_worker_wait (SQUALE_WORKER (my_worker));
        }
    }

    squale_worker_disconnect (SQUALE_WORKER (my_worker));

    beach:
    squale_worker_shutdown_complete (SQUALE_WORKER (my_worker));
    squale_worker_set_running (SQUALE_WORKER (my_worker), FALSE);

    return NULL;
}

static void
squale_mysql_worker_set_property (GObject *object, guint prop_id,
                                  const GValue *value, GParamSpec *pspec)
{
    SqualeMysqlWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_MYSQL_WORKER (object));

    worker = SQUALE_MYSQL_WORKER (object);

    switch (prop_id)
    {
        case PROP_HOST:
            if (worker->host)
                g_free (worker->host);
            worker->host = g_strdup (g_value_get_string (value));
            break;
        case PROP_PORT:
            worker->port = atoi (g_value_get_string (value));
            break;
        case PROP_DBNAME:
            if (worker->dbname)
                g_free (worker->dbname);
            worker->dbname = g_strdup (g_value_get_string (value));
            break;
        case PROP_USER:
            if (worker->user)
                g_free (worker->user);
            worker->user = g_strdup (g_value_get_string (value));
            break;
        case PROP_PASSWD:
            if (worker->passwd)
                g_free (worker->passwd);
            worker->passwd = g_strdup (g_value_get_string (value));
            break;
        default :
            G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
            break;
    }
}

static void
squale_mysql_worker_get_property (GObject *object, guint prop_id,
                                  GValue *value, GParamSpec *pspec)
{
    SqualeMysqlWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_MYSQL_WORKER (object));

    worker = SQUALE_MYSQL_WORKER (object);

    switch (prop_id)
    {
        case PROP_HOST:
            g_value_set_string (value, g_strdup (worker->host));
            break;
        case PROP_PORT:
            g_value_set_string (value, g_strdup_printf ("%d", worker->port));
            break;
        case PROP_DBNAME:
            g_value_set_string (value, g_strdup (worker->dbname));
            break;
        case PROP_USER:
            g_value_set_string (value, g_strdup (worker->user));
            break;
        case PROP_PASSWD:
            g_value_set_string (value, g_strdup (worker->passwd));
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
squale_mysql_worker_dispose (GObject *object)
{
    SqualeMysqlWorker *worker = NULL;

    worker = SQUALE_MYSQL_WORKER (object);

    if (worker->host)
        g_free (worker->host);

    worker->host = NULL;

    worker->port = 0;

    if (worker->user)
        g_free (worker->user);

    worker->user = NULL;

    if (worker->passwd)
        g_free (worker->passwd);

    worker->passwd = NULL;

    if (worker->dbname)
        g_free (worker->dbname);

    worker->dbname = NULL;

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_mysql_worker_init (SqualeMysqlWorker *worker)
{
    worker->host = NULL;
    worker->port = 0;
    worker->user = NULL;
    worker->passwd = NULL;
    worker->dbname = NULL;
    mysql_init (&(worker->mysql));
}

static void
squale_mysql_worker_class_init (SqualeMysqlWorkerClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
    SqualeWorkerClass *worker_class = SQUALE_WORKER_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->set_property = squale_mysql_worker_set_property;
    gobject_class->get_property = squale_mysql_worker_get_property;
    gobject_class->dispose = squale_mysql_worker_dispose;

    worker_class->connect = squale_mysql_worker_connect;
    worker_class->disconnect = squale_mysql_worker_disconnect;
    worker_class->run = squale_mysql_worker_run;

    g_object_class_install_property (gobject_class,
                                     PROP_HOST,
                                     g_param_spec_string ("host",
                                                          "Database hostname",
                                                          "The hostname to access that database",
                                                          NULL,
                                                          G_PARAM_READWRITE));
    g_object_class_install_property (gobject_class,
                                     PROP_PORT,
                                     g_param_spec_string ("port",
                                                          "Database port",
                                                          "The port to access that database",
                                                          NULL,
                                                          G_PARAM_READWRITE));
    g_object_class_install_property (gobject_class,
                                     PROP_DBNAME,
                                     g_param_spec_string ("dbname",
                                                          "Database name",
                                                          "The name of that database",
                                                          NULL,
                                                          G_PARAM_READWRITE));
    g_object_class_install_property (gobject_class,
                                     PROP_USER,
                                     g_param_spec_string ("user",
                                                          "Username",
                                                          "The username to access that database",
                                                          NULL,
                                                          G_PARAM_READWRITE));
    g_object_class_install_property (gobject_class,
                                     PROP_PASSWD,
                                     g_param_spec_string ("passwd",
                                                          "Password",
                                                          "The password to access that database",
                                                          NULL,
                                                          G_PARAM_READWRITE));
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_mysql_worker_get_type (void)
{
    static GType worker_type = 0;

    if (!worker_type)
    {
        static const GTypeInfo worker_info = {
                sizeof (SqualeMysqlWorkerClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_mysql_worker_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeMysqlWorker),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_mysql_worker_init,
                NULL                    /* value_table */
        };

        worker_type =
                g_type_register_static (SQUALE_TYPE_WORKER, "SqualeMysqlWorker",
                                        &worker_info, (GTypeFlags) 0);
    }
    return worker_type;
}

SqualeMysqlWorker *
squale_mysql_worker_new (void)
{
    SqualeMysqlWorker *worker = g_object_new (SQUALE_TYPE_MYSQL_WORKER, NULL);

    return worker;
}
