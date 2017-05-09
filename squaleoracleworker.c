/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squaleoracleworker.c : Source for SqualeOracleWorker object.
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
#include "squaleoracleworker.h"
#include "squale-i18n.h"
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

#define ORACLE_TEST_QUERY   "select 1 from dual"
#define MAX_ERRORS_IN_A_ROW 5

enum
{
    PROP_0,
    PROP_TNSNAME,
    PROP_USER,
    PROP_PASSWD,
    PROP_COMMIT_EVERY
};

static SqualeWorkerClass *parent_class = NULL;

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static GQuark
squale_oracle_worker_error_quark (void)
{
    static GQuark quark = 0;
    if (quark == 0)
        quark = g_quark_from_static_string ("SQuaLe-Oracle");
    return quark;
}

static gboolean
squale_oracle_worker_store_resultset (SqualeOracleWorker *ora_worker,
                                      SqualeJob *job, sqlo_stmt_handle_t sth)
{
    gint32 num_fields = 0, i, status;
    gulong offset = 0, num_rows_offset, j = 0;
    const char **col_names = NULL;

    g_return_val_if_fail (SQUALE_IS_ORACLE_WORKER (ora_worker), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (job), FALSE);

    col_names = sqlo_ocol_names (sth, &num_fields);

    /* That should never happen as we checked it before, but it does'nt harm */
    if (!col_names) {
        if (sqlo_fetch (sth, 1) < 0) {
            GError *error = g_error_new (squale_oracle_worker_error_quark (), 0, "%s",
                                         sqlo_geterror (ora_worker->dbh));
            squale_job_set_error (job, error);
            SQUALE_WORKER (ora_worker)->nb_errors++;
        }
        return FALSE;
    }

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

    /* Packing columns names */
    for (i = 0; i < num_fields; i++) {
        gint32 field_length = strlen (col_names[i]);
        squale_check_mem_block (&(job->resultset->data),
                                &(job->resultset->allocated_memory), offset,
                                sizeof (gint32) + field_length);
        *(gint32 *)(job->resultset->data + offset) = field_length;
        offset += sizeof (gint32);
        memcpy (job->resultset->data + offset, col_names[i], field_length);
        offset += field_length;
    }

    /* Storing the offset of number of rows */
    num_rows_offset = offset;
    squale_check_mem_block (&(job->resultset->data),
                            &(job->resultset->allocated_memory), offset,
                            sizeof (gulong));
    offset += sizeof (gulong);

    /* For each row */
    while (SQLO_SUCCESS == (status = (sqlo_fetch (sth, 1))) ||
           status == SQLO_SUCCESS_WITH_INFO) {
        const char **v = sqlo_values (sth, NULL, 1);
        for (i = 0; i < num_fields; i++) {
            gint32 field_length = strlen (v[i]);
            squale_check_mem_block (&(job->resultset->data),
                                    &(job->resultset->allocated_memory),
                                    offset, sizeof (gint32) + field_length);
            *(gint32 *)(job->resultset->data + offset) = field_length;
            offset += sizeof (gint32);
            memcpy (job->resultset->data + offset, v[i], field_length);
            offset += field_length;
        }
        if (status == SQLO_SUCCESS_WITH_INFO) {
            GError *warning = g_error_new (squale_oracle_worker_error_quark (), 0,
                                           "%s", sqlo_geterror (ora_worker->dbh));
            squale_job_set_warning (job, warning);
        }
        j++;
    }

    /* If status is different from SQLO_NO_DATA that means an error occured
       in that case we free the fetched data and gather the error message. */
    if (status != SQLO_NO_DATA) {
        GError *error = g_error_new (squale_oracle_worker_error_quark (), 0, "%s",
                                     sqlo_geterror (ora_worker->dbh));
        squale_job_set_error (job, error);
        SQUALE_WORKER (ora_worker)->nb_errors++;
        g_free (job->resultset->data);
        job->resultset->data = NULL;
        return FALSE;
    }

    /* Finally packing the number of rows */
    *(gulong *)(job->resultset->data + num_rows_offset) = (gulong) j;

    job->resultset->data_size = offset;

    return TRUE;
}

static gboolean
squale_oracle_worker_connect (SqualeWorker *worker)
{
    SqualeOracleWorker *ora_worker = NULL;
    SqualeJobList *worker_joblist = NULL;
    char *conn_string = NULL, *joblist_name = NULL;
    guint nb_tries = 1;
    gboolean connected = FALSE;

    g_return_val_if_fail (SQUALE_IS_ORACLE_WORKER (worker), FALSE);

    ora_worker = SQUALE_ORACLE_WORKER (worker);
    worker_joblist = squale_worker_get_joblist (worker);
    if (SQUALE_IS_JOBLIST (worker_joblist)) {
        joblist_name = squale_joblist_get_name (worker_joblist);
        g_object_unref (worker_joblist);
        worker_joblist = NULL;
    }

    squale_worker_set_status (worker, _("Connecting"));

    conn_string = g_strdup_printf ("%s/%s@%s", ora_worker->user,
                                   ora_worker->passwd, ora_worker->tnsname);

    while (!connected && !squale_worker_check_shutdown (worker)) {

        g_message (_("Joblist '%s': Oracle worker (%p) trying to establish " \
               "connection to %s (attempt %d)"), joblist_name, worker,
                   ora_worker->tnsname, nb_tries);

        if (SQLO_SUCCESS != sqlo_connect (&(ora_worker->dbh), conn_string)) {
            g_warning (_("Joblist '%s': Connection attempt %d to Oracle database %s" \
                 " failed for worker (%p): %s"), joblist_name, nb_tries,
                       ora_worker->tnsname, worker, sqlo_geterror (ora_worker->dbh));
            /* Wait a little before trying again */
            sleep (1);
        }
        else {
            g_message (_("Joblist '%s': Oracle worker (%p) successfully connected " \
                 "to %s"), joblist_name, worker, ora_worker->tnsname);
            connected = TRUE;
        }
        nb_tries++;
    }

    if (conn_string) {
        g_free (conn_string);
    }
    if (joblist_name) {
        g_free (joblist_name);
    }

    return connected;
}

static gboolean
squale_oracle_worker_disconnect (SqualeWorker *worker)
{
    SqualeOracleWorker *ora_worker = NULL;
    SqualeJobList *worker_joblist = NULL;
    char *joblist_name = NULL;

    g_return_val_if_fail (SQUALE_IS_ORACLE_WORKER (worker), FALSE);

    ora_worker = SQUALE_ORACLE_WORKER (worker);

    /* Trying to commit any pending transaction */
    if (ora_worker->since_commit) {
        sqlo_commit (ora_worker->dbh);
        ora_worker->since_commit = 0;
    }

    worker_joblist = squale_worker_get_joblist (worker);

    if (SQUALE_IS_JOBLIST (worker_joblist)) {
        joblist_name = squale_joblist_get_name (worker_joblist);
        g_object_unref (worker_joblist);
        worker_joblist = NULL;
    }

    squale_worker_set_status (worker, _("Disconnecting"));

    g_message (_("Joblist '%s': Oracle worker (%p) shutting down connection " \
             "to %s"), joblist_name, worker, ora_worker->tnsname);

    sqlo_finish (ora_worker->dbh);

    if (joblist_name) {
        g_free (joblist_name);
    }

    return TRUE;
}

static gboolean
squale_oracle_worker_test (SqualeOracleWorker *ora_worker)
{
    sqlo_stmt_handle_t sth = SQLO_STH_INIT;

    g_return_val_if_fail (SQUALE_IS_ORACLE_WORKER (ora_worker), FALSE);

    if (sqlo_open2 (&sth, ora_worker->dbh, ORACLE_TEST_QUERY, 0, NULL) < 0) {
        return FALSE;
    }
    else {
        if (sqlo_close (sth) != SQLO_SUCCESS) {
            return FALSE;
        }
        return TRUE;
    }
}

static gpointer
squale_oracle_worker_run (gpointer worker)
{
    SqualeOracleWorker *ora_worker = NULL;
    SqualeJobList *joblist = NULL;
    gboolean test_connection = FALSE;
    char *joblist_name = NULL;
    SqualeJob *job = NULL;

    g_return_val_if_fail (SQUALE_IS_ORACLE_WORKER (worker), NULL);

    ora_worker = SQUALE_ORACLE_WORKER (worker);
    joblist = squale_worker_get_joblist (worker);
    if (SQUALE_IS_JOBLIST (joblist)) {
        joblist_name = squale_joblist_get_name (joblist);
    }

    squale_worker_set_running (SQUALE_WORKER (ora_worker), TRUE);

    if (!squale_worker_connect (SQUALE_WORKER (ora_worker))) {
        goto beach;
    }

    squale_worker_set_status (SQUALE_WORKER (ora_worker), _("Sleeping"));

    /* The worker looping forever until shutdown is requested */
    while (!squale_worker_check_shutdown (SQUALE_WORKER (ora_worker))) {

        if (test_connection) {
            /* We make a test query to check if the connection is guilty */
            if (!squale_oracle_worker_test (ora_worker)) {
                /* Connection appears to be broken cycling... */
                g_warning (_("Joblist '%s': Oracle worker (%p)'s connection to %s " \
                   "went down, trying to cycle"), joblist_name, worker,
                           ora_worker->tnsname);
                sqlo_server_free (ora_worker->dbh);
                squale_worker_connect (SQUALE_WORKER (ora_worker));
                SQUALE_WORKER (ora_worker)->nb_db_conn_cycles++;
            }

            test_connection = FALSE;
        }

        if (job == NULL) {
            job = squale_joblist_assign_pending_job (joblist, FALSE);
        }

        if (SQUALE_IS_JOB (job)) {
            sqlo_stmt_handle_t sth = SQLO_STH_INIT;

            squale_worker_set_status (SQUALE_WORKER (ora_worker), job->query);

            /* We have a job assigned to us */
            if (sqlo_open2 (&sth, ora_worker->dbh, job->query, 0, NULL) < 0) {
                /* Error: Query failed */
                GError *error = g_error_new (squale_oracle_worker_error_quark (), 0,
                                             "%s", sqlo_geterror (ora_worker->dbh));
                squale_job_set_error (job, error);
                SQUALE_WORKER (ora_worker)->nb_errors++;

                test_connection = TRUE;
            }
            else {
                const char **col_names = NULL;
                gint num_fields;

                /* We try to get the number of columns */
                col_names = sqlo_ocol_names (sth, &num_fields);

                /* No columns. That means the query is not returning a resultset */
                if (!col_names) {
                    /* Try to execute the query */
                    if (sqlo_fetch (sth, 1) < 0) {
                        GError *error = g_error_new (squale_oracle_worker_error_quark (), 0,
                                                     "%s", sqlo_geterror (ora_worker->dbh));
                        squale_job_set_error (job, error);
                        SQUALE_WORKER (ora_worker)->nb_errors++;

                        test_connection = TRUE;
                    }
                    else {
                        gint affected_rows = sqlo_prows (sth);
                        if (affected_rows < 0) {
                            GError *error = g_error_new (squale_oracle_worker_error_quark (),
                                                         0, "%s",
                                                         sqlo_geterror (ora_worker->dbh));
                            squale_job_set_error (job, error);
                            SQUALE_WORKER (ora_worker)->nb_errors++;

                            test_connection = TRUE;
                        }
                        else {
                            job->affected_rows = (gint32) affected_rows;

                            ora_worker->since_commit++;

                            /* Try to commit only if commit grouping is disabled or we've
                             * reached our commit threshold */
                            if (ora_worker->commit_every == 0 ||
                                ora_worker->since_commit >= ora_worker->commit_every) {
                                g_message ("Joblist '%s': Oracle worker (%p) commiting %" \
                    G_GUINT64_FORMAT " transactions (Threshold %" \
                    G_GUINT64_FORMAT ")", joblist_name, worker,
                                           ora_worker->since_commit, ora_worker->commit_every);
                                /* Trying to commit */
                                if (sqlo_commit (ora_worker->dbh) < 0) {
                                    GError *error = g_error_new (squale_oracle_worker_error_quark (),
                                                                 0, "%s",
                                                                 sqlo_geterror (ora_worker->dbh));
                                    squale_job_set_error (job, error);
                                    SQUALE_WORKER (ora_worker)->nb_errors++;

                                    test_connection = TRUE;
                                }
                                else {
                                    /* Commit worked, reinitialize our counter */
                                    ora_worker->since_commit = 0;
                                }
                            }
                        }
                    }
                }
                else {
                    /* We probably have a resultset */
                    squale_oracle_worker_store_resultset (ora_worker, job, sth);
                }

                /* Closing cursor */
                if (sqlo_close (sth) != SQLO_SUCCESS) {
                    GError *error = g_error_new (squale_oracle_worker_error_quark (), 0,
                                                 "%s", sqlo_geterror (ora_worker->dbh));
                    squale_job_set_error (job, error);
                    SQUALE_WORKER (ora_worker)->nb_errors++;

                    test_connection = TRUE;
                }
            }

            squale_job_set_status_if_match (job, SQUALE_JOB_COMPLETE,
                                            SQUALE_JOB_PROCESSING);

            SQUALE_WORKER (ora_worker)->nb_jobs_processed++;

            g_object_unref (job);
            job = NULL;

            squale_worker_cycle_connection (SQUALE_WORKER (ora_worker));

            squale_worker_set_status (SQUALE_WORKER (ora_worker), _("Sleeping"));
        }
        else {
            /* No pending job in the joblist let's wait for a new job, while switching
               to waiting state we check the joblist for a pending job. If there is
               one we return it and we don't wait */
            job = squale_worker_wait (SQUALE_WORKER (ora_worker));
        }
    }

    squale_worker_disconnect (SQUALE_WORKER (ora_worker));

    beach:
    squale_worker_shutdown_complete (SQUALE_WORKER (ora_worker));
    squale_worker_set_running (SQUALE_WORKER (ora_worker), FALSE);

    if (SQUALE_IS_JOBLIST (joblist)) {
        g_object_unref (joblist);
        joblist = NULL;
    }

    if (joblist_name) {
        g_free (joblist_name);
    }

    return NULL;
}

static void
squale_oracle_worker_set_property (GObject *object, guint prop_id,
                                   const GValue *value, GParamSpec *pspec)
{
    SqualeOracleWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_ORACLE_WORKER (object));

    worker = SQUALE_ORACLE_WORKER (object);

    switch (prop_id)
    {
        case PROP_TNSNAME:
            if (worker->tnsname)
                g_free (worker->tnsname);
            worker->tnsname = g_strdup (g_value_get_string (value));
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
        case PROP_COMMIT_EVERY:
            worker->commit_every = atoll (g_value_get_string (value));
            /* We always reinitialize our since_commit counter */
            worker->since_commit = 0;
            break;
        default :
            G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
            break;
    }
}

static void
squale_oracle_worker_get_property (GObject *object, guint prop_id,
                                   GValue *value, GParamSpec *pspec)
{
    SqualeOracleWorker *worker = NULL;

    g_return_if_fail (SQUALE_IS_ORACLE_WORKER (object));

    worker = SQUALE_ORACLE_WORKER (object);

    switch (prop_id)
    {
        case PROP_TNSNAME:
            g_value_set_string (value, g_strdup (worker->tnsname));
            break;
        case PROP_USER:
            g_value_set_string (value, g_strdup (worker->user));
            break;
        case PROP_PASSWD:
            g_value_set_string (value, g_strdup (worker->passwd));
            break;
        case PROP_COMMIT_EVERY:
            g_value_set_string (value,
                                g_strdup_printf ("%" G_GUINT64_FORMAT, worker->commit_every));
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
squale_oracle_worker_dispose (GObject *object)
{
    SqualeOracleWorker *worker = NULL;

    worker = SQUALE_ORACLE_WORKER (object);

    if (worker->tnsname)
        g_free (worker->tnsname);

    worker->tnsname = NULL;

    if (worker->user)
        g_free (worker->user);

    worker->user = NULL;

    if (worker->passwd)
        g_free (worker->passwd);

    worker->passwd = NULL;

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_oracle_worker_init (SqualeOracleWorker *worker)
{
    worker->tnsname = NULL;
    worker->user = NULL;
    worker->passwd = NULL;
    worker->commit_every = worker->since_commit = 0;
}

static void
squale_oracle_worker_class_init (SqualeOracleWorkerClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
    SqualeWorkerClass *worker_class = SQUALE_WORKER_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->set_property = squale_oracle_worker_set_property;
    gobject_class->get_property = squale_oracle_worker_get_property;
    gobject_class->dispose = squale_oracle_worker_dispose;

    worker_class->connect = squale_oracle_worker_connect;
    worker_class->disconnect = squale_oracle_worker_disconnect;
    worker_class->run = squale_oracle_worker_run;

    g_object_class_install_property (gobject_class,
                                     PROP_TNSNAME,
                                     g_param_spec_string ("tnsname",
                                                          "Database TNSNAME",
                                                          "The TNSNAME to access that database",
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
    g_object_class_install_property (gobject_class,
                                     PROP_COMMIT_EVERY,
                                     g_param_spec_string ("commit-every",
                                                          "Commit every n transactions",
                                                          "When not 0 this tells the work to not auto-commit and do a commit after a certain number of transactions",
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
squale_oracle_worker_get_type (void)
{
    static GType worker_type = 0;

    if (!worker_type)
    {
        static const GTypeInfo worker_info = {
                sizeof (SqualeOracleWorkerClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_oracle_worker_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeOracleWorker),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_oracle_worker_init,
                NULL                    /* value_table */
        };

        worker_type =
                g_type_register_static (SQUALE_TYPE_WORKER, "SqualeOracleWorker",
                                        &worker_info, (GTypeFlags) 0);
    }
    return worker_type;
}

SqualeOracleWorker *
squale_oracle_worker_new (void)
{
    SqualeOracleWorker *worker = g_object_new (SQUALE_TYPE_ORACLE_WORKER, NULL);

    return worker;
}
