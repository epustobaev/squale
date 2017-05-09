/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalelistener.c : Source for SqualeClient object.
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
#include "squaleclient.h"
#include "squale-i18n.h"
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

enum
{
    DISCONNECTED,
    STATS,
    LAST_SIGNAL
};

typedef enum
{
    SQUALE_CLIENT_READ_OK,
    SQUALE_CLIENT_READ_PARTIAL,
    SQUALE_CLIENT_READ_DISCONNECTED
} SqualeClientReadReturn;

static GObjectClass *parent_class = NULL;
static guint client_signals[LAST_SIGNAL] = { 0 };

static gboolean squale_client_client_io_watch (GIOChannel *source,
                                               GIOCondition condition, gpointer user_data);
static gboolean squale_client_job_io_watch (GIOChannel *source,
                                            GIOCondition condition, gpointer user_data);

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static GQuark
squale_client_error_quark (void)
{
    static GQuark quark = 0;
    if (quark == 0)
        quark = g_quark_from_static_string ("SQuaLe-client");
    return quark;
}

static void
squale_client_disconnected (SqualeClient *client)
{
    g_return_if_fail (SQUALE_IS_CLIENT (client));

    /* If there's a source watching the job's control socket we remove it as
       the client will disappear */
    if (client->job_sourceid) {
        g_source_remove (client->job_sourceid);
        client->job_sourceid = 0;
    }

    if (client->job_io_channel) {
        g_io_channel_shutdown (client->job_io_channel, TRUE, NULL);
        g_io_channel_unref (client->job_io_channel);
        client->job_io_channel = NULL;
    }

    if (client->in_sourceid) {
        g_source_remove (client->in_sourceid);
        client->in_sourceid = 0;
    }

    if (client->client_io_channel) {
        /* That closes the fd as well */
        g_io_channel_shutdown (client->client_io_channel, TRUE, NULL);
        g_io_channel_unref (client->client_io_channel);
        client->client_io_channel = NULL;
    }
}

/* Here we prepare the output buffer that will be written to the socket from
   the GSource dispatch function when the socket becomes writable. */
static void
squale_client_send_result (SqualeClient *client)
{
    gint32 processing_time, assignation_time, buf_pos = 0;

    g_return_if_fail (SQUALE_IS_CLIENT (client));
    g_return_if_fail (SQUALE_IS_JOB (client->job));

    /* We don't process unfinished jobs */
    if (client->job->status != SQUALE_JOB_COMPLETE) {
        return;
    }

    g_message (_("Job %p is complete, sending result bytestream " \
      "to client %p"), client->job, client);

    /* Assignation and processing time */
    client->out_buf_size = 2 * sizeof (gint32);

    assignation_time = squale_job_get_assignation_delay (client->job);
    processing_time = squale_job_get_processing_time (client->job);

    if (client->job->error) {
        gint32 error_length = strlen (client->job->error->message);

        g_message (_("Job %p generated an error %s"), client->job,
                   client->job->error->message);

        /* Header char + number of affected rows */
        client->out_buf_size += sizeof (char) + sizeof (gint32) + error_length;

        client->out_buf = g_malloc0 (client->out_buf_size);

        *(gint32 *)(client->out_buf + buf_pos) = assignation_time;
        buf_pos += sizeof (gint32);
        *(gint32 *)(client->out_buf + buf_pos) = processing_time;
        buf_pos += sizeof (gint32);
        *(char *)(client->out_buf + buf_pos) = 'E';
        buf_pos += sizeof (char);
        *(gint32 *)(client->out_buf + buf_pos) = error_length;
        buf_pos += sizeof (gint32);
        memcpy (client->out_buf + buf_pos, client->job->error->message,
                error_length);
    }
    else if (client->job->affected_rows != -1) {
        g_message (_("Job %p has affected %d rows"), client->job,
                   client->job->affected_rows);

        /* Header char + number of affected rows */
        client->out_buf_size += sizeof (char) + sizeof (gint32);

        client->out_buf = g_malloc0 (client->out_buf_size);

        *(gint32 *)(client->out_buf + buf_pos) = assignation_time;
        buf_pos += sizeof (gint32);
        *(gint32 *)(client->out_buf + buf_pos) = processing_time;
        buf_pos += sizeof (gint32);
        *(char *)(client->out_buf + buf_pos) = 'A';
        buf_pos += sizeof (char);
        *(gint32 *)(client->out_buf + buf_pos) = client->job->affected_rows;
    }
    else if (client->job->resultset->data) {
        g_message (_("Job %p generated a resultset"), client->job);

        /* Here we will just put our integers and 'R' in the resultset buffer
           indeed the worker preallocated a space for us. This way we just use the
           data pointer of the resultset as out_buf and we save a lot of memory
           operations */
        *(gint32 *)(client->job->resultset->data + buf_pos) = assignation_time;
        buf_pos += sizeof (gint32);
        *(gint32 *)(client->job->resultset->data + buf_pos) = processing_time;
        buf_pos += sizeof (gint32);
        if (client->job->warning) {
            *(char *)(client->job->resultset->data + buf_pos) = 'W';
        }
        else {
            *(char *)(client->job->resultset->data + buf_pos) = 'R';
        }
        buf_pos += sizeof (char);

        client->out_buf_size = client->job->resultset->data_size;
        /* resultset->data won't be freed when the job will be disposed anyway,
           we will free that block of memory */
        client->out_buf = client->job->resultset->data;
        if (client->job->warning) {
            gulong offset = client->job->resultset->data_size;
            gint32 warning_length = strlen (client->job->warning->message);
            /* We add the warning message's length and the warning message itself */
            squale_check_mem_block (&(client->out_buf),
                                    &(client->job->resultset->allocated_memory), offset,
                                    sizeof (gint32) + warning_length);
            *(gint32 *)(client->out_buf + offset) = warning_length;
            offset += sizeof (gint32);
            memcpy (client->out_buf + offset, client->job->warning->message,
                    warning_length);
            offset += warning_length;
            client->out_buf_size = offset;
        }
    }
    else {
        g_critical ("Job is complete but not an error, no affected_rows and no " \
        "fields! That should never happen");
    }

    client->status = SQUALE_CLIENT_SEND_RESULT;

    /* We watch the client socket for output */
    if (client->in_sourceid) {
        /* We first remove the current source which is just looking for input */
        g_source_remove (client->in_sourceid);
        /* Then we add another source to the maincontext that will look for
           everything */
        client->in_sourceid = g_io_add_watch (client->client_io_channel,
                                              G_IO_IN | G_IO_ERR | G_IO_HUP | G_IO_OUT, squale_client_client_io_watch,
                                              client);
    }
}

static gboolean
squale_client_execute_system_order (SqualeClient *client)
{
    GHashTable *hash = NULL;
    GError *error = NULL;

    g_return_val_if_fail (SQUALE_IS_CLIENT (client), FALSE);
    g_return_val_if_fail (SQUALE_IS_JOB (client->job), FALSE);

    /* We use a hash table to store statistics pairs */
    hash = g_hash_table_new_full (g_str_hash , g_str_equal, g_free, g_free);

    switch (client->job->job_type) {
        case SQUALE_JOB_GLOBAL_STATS:
            g_signal_emit (client, client_signals[STATS], 0, hash, NULL);
            break;
        case SQUALE_JOB_LOCAL_STATS:
            if (SQUALE_IS_JOBLIST (client->joblist)) {
                squale_joblist_get_stats (client->joblist, hash);
            }
            else {
                error = g_error_new (squale_client_error_quark (), 0,
                                     _("Can't get stats from a non existing joblist %s"),
                                     client->order_joblist);
                squale_job_set_error (client->job, error);
                squale_job_set_status_if_match (client->job, SQUALE_JOB_COMPLETE,
                                                SQUALE_JOB_PENDING);
                g_hash_table_destroy (hash);
                return TRUE;
            }
            break;
        case SQUALE_JOB_GLOBAL_SHUTDOWN:
            g_hash_table_insert (hash, g_strdup ("Status"), g_strdup ("OK"));
            squale_job_complete_from_hashtable (client->job, hash);
            squale_client_send_result (client);
            g_hash_table_destroy (hash);
            squale_quit (SIGTERM);
            return TRUE;
            break;
        case SQUALE_JOB_SHUTDOWN:
            if (SQUALE_IS_JOBLIST (client->joblist)) {
                g_hash_table_insert (hash, g_strdup ("Status"), g_strdup ("OK"));
                if (client->joblist->status == SQUALE_JOBLIST_OPENED) {
                    squale_joblist_shutdown (client->joblist);
                }
            }
            else {
                error = g_error_new (squale_client_error_quark (), 0,
                                     _("Can't shutdown a non existing joblist %s"),
                                     client->order_joblist);
                squale_job_set_error (client->job, error);
                squale_job_set_status_if_match (client->job, SQUALE_JOB_COMPLETE,
                                                SQUALE_JOB_PENDING);
                g_hash_table_destroy (hash);
                return TRUE;
            }
            break;
        case SQUALE_JOB_STARTUP:
            if (SQUALE_IS_JOBLIST (client->joblist)) {
                g_hash_table_insert (hash, g_strdup ("Status"), g_strdup ("OK"));
                if (client->joblist->status == SQUALE_JOBLIST_CLOSED) {
                    squale_joblist_startup (client->joblist);
                }
            }
            else {
                error = g_error_new (squale_client_error_quark (), 0,
                                     _("Can't startup a non existing joblist %s"),
                                     client->order_joblist);
                squale_job_set_error (client->job, error);
                squale_job_set_status_if_match (client->job, SQUALE_JOB_COMPLETE,
                                                SQUALE_JOB_PENDING);
                g_hash_table_destroy (hash);
                return TRUE;
            }
            break;
        default:
            return FALSE;
            break;
    }

    squale_job_complete_from_hashtable (client->job, hash);

    g_hash_table_destroy (hash);

    return TRUE;
}

/* When we receive some data on the job's control socket that means something
   happened to our job. We check the status of the job and react accordingly */
static gboolean
squale_client_job_io_watch (GIOChannel *source, GIOCondition condition,
                            gpointer user_data)
{
    SqualeClient *client = NULL;
    gchar c;

    g_return_val_if_fail (SQUALE_IS_CLIENT (user_data), FALSE);

    client = SQUALE_CLIENT (user_data);

    read (client->job->control_socket[0], &c, 1);

    /* We only dispatch COMPLETE jobs */
    if (SQUALE_IS_JOB (client->job) &&
        client->job->status == SQUALE_JOB_COMPLETE) {
        squale_client_send_result (client);
        /* The source will be removed when we return so mark it as deleted */
        client->job_sourceid = 0;
        return FALSE;
    }
    else {
        return TRUE;
    }
}

/* This function searches for a matching joblist in the list we have been
   given by squale main loop. When a joblist is found we keep a ref to it,
   create a job with the query and put that job in the joblist */
static gboolean
squale_client_execute (SqualeClient *client)
{
    GList *joblists = NULL;

    g_return_val_if_fail (SQUALE_IS_CLIENT (client), FALSE);
    g_return_val_if_fail (client->joblists != NULL, FALSE);

    /* No order we return TRUE */
    if (!client->incoming_order) {
        return TRUE;
    }

    client->job = squale_job_new ();

    if (SQUALE_IS_JOB (client->job)) {
        g_message (_("Created job %p for client %p"), client->job, client);
    }
    else {
        g_warning ("Failed creating a new job for client %p", client);
        return FALSE;
    }

    /* We create an IOChannel to communicate with the worker thread. The thread
       will write a character on the job's socketpair when something happens.
       We just monitor that socketpair and dispatch the job according to the
       status */
    client->job_io_channel = g_io_channel_unix_new (
            client->job->control_socket[0]);
    if (client->job_io_channel) {
        client->job_sourceid = g_io_add_watch (client->job_io_channel, G_IO_IN,
                                               squale_client_job_io_watch, client);
    }
    else {
        g_warning ("Failed creating the GIOChannel for job %p", client->job);
    }

    /* Defining the query for that job */
    squale_job_set_query (client->job, client->incoming_order);

    /* Try to find a matching joblist to attach that job to */
    joblists = client->joblists;

    while (joblists) {
        SqualeJobList *joblist = SQUALE_JOBLIST (joblists->data);

        if (SQUALE_IS_JOBLIST (joblist)) {
            char *joblist_name = squale_joblist_get_name (joblist);

            if (!g_ascii_strcasecmp (joblist_name, client->order_joblist)) {
                /* We found the matching joblist */
                client->joblist = joblist;

                /* Paranoid ref: During all the client's life we keep a ref to the
                   joblist. This way we are sure the joblist can't disappear while
                   we still want to remove our job from it */
                g_object_ref (joblist);

                if (joblist_name) {
                    g_free (joblist_name);
                    joblist_name = NULL;
                }

                /* Check job type */
                if (client->job->job_type == SQUALE_JOB_NORMAL) {
                    GError *error = NULL;

                    if (!squale_joblist_add_job (client->joblist, client->job, &error)) {
                        if (error) {
                            squale_job_set_error (client->job, error);
                        }
                        /* Declare the job as COMPLETE */
                        squale_job_set_status_if_match (client->job, SQUALE_JOB_COMPLETE,
                                                        SQUALE_JOB_PENDING);
                    }

                    return TRUE;
                }
                else {
                    /* System job just run */
                    return squale_client_execute_system_order (client);
                }
            }

            if (joblist_name) {
                g_free (joblist_name);
                joblist_name = NULL;
            }
        }

        joblists = g_list_next (joblists);
    }

    if (client->job->job_type == SQUALE_JOB_NORMAL) {
        GError *error = g_error_new (squale_client_error_quark (), 0,
                                     _("Joblist %s does not exist in this SQuaLe instance"),
                                     client->order_joblist);
        /* No joblist found, returning an error */
        squale_job_set_error (client->job, error);
        squale_job_set_status_if_match (client->job, SQUALE_JOB_COMPLETE,
                                        SQUALE_JOB_PENDING);
        return TRUE;
    }
    else {
        /* No joblist was found but we have a system order. Trying to run */
        g_message (_("No matching joblist found, try running a  system job"));
        return squale_client_execute_system_order (client);
    }
}

/* That function get called when the client is taking too much time to send
   the order. It might be dead so we consider it as disconnected */
static gboolean
squale_client_timeout (gpointer user_data)
{
    SqualeClient *client = NULL;

    g_return_val_if_fail (SQUALE_IS_CLIENT (user_data), FALSE);

    client = SQUALE_CLIENT (user_data);

    client->client_timeout = 0;

    if (client->status < SQUALE_CLIENT_ORDER) {
        /* We still haven't finished order protocol communication */
        g_warning (_("Timeout while waiting for client %p"), client);
        g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
    }

    return FALSE;
}

/* This function reads a string's length and the string from a client's socket.
   It can be called multiple times for the same string to handle partial reads.
   Returns TRUE for a complete and successfull read or FALSE for an error or
   a partial read. It takes care of disconnecting the client on error */
static SqualeClientReadReturn
squale_client_read_string (SqualeClient *client, char **string)
{
    gint read_bytes = 0;

    g_return_val_if_fail (SQUALE_IS_CLIENT (client), FALSE);
    g_return_val_if_fail (string != NULL, FALSE);

    if (client->read_so_far == 0 && client->string_length == 0) {
        /* That's our first try on that string */
        read_bytes = read (client->client_fd, &(client->string_length),
                           sizeof (gint32));
        if (read_bytes == sizeof (gint32)) { /* Correctly read string's length */
            /* Allocating string memory space */
            *string = g_malloc0 (client->string_length + 1);
            /* Reading string */
            read_bytes = read (client->client_fd, *string, client->string_length);
            if (read_bytes == client->string_length) {
                /* We read the complete string */
                client->string_length = 0;
                return SQUALE_CLIENT_READ_OK;
            }
            else if (read_bytes > 0 && read_bytes < client->string_length) {
                /* We read part of the string */
                client->read_so_far = read_bytes;
                return SQUALE_CLIENT_READ_PARTIAL;
            }
            else { /* An error occured while reading string */
                if (read_bytes == 0) {
                    g_warning ("reached end of stream on socket but we " \
              "haven't read the string completely: read %d, expected %d",
                               read_bytes, client->string_length);
                }
                else {
                    if (errno == EAGAIN) {
                        return SQUALE_CLIENT_READ_PARTIAL;
                    }
                    g_warning ("error while reading from socket %s (read_bytes %d)",
                               strerror (errno), read_bytes);
                }
                g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
                client->string_length = 0;
                return SQUALE_CLIENT_READ_DISCONNECTED;
            }
        }
        else { /* Unable to read string length */
            if (read_bytes == 0) {
                g_warning ("reached end of stream on socket but we haven't read " \
            "string's length completely: read %d, expected %" G_GSIZE_FORMAT,
                        read_bytes, sizeof (gint32));
            }
            else {
                if (errno == EAGAIN) {
                    client->string_length = 0;
                    return SQUALE_CLIENT_READ_PARTIAL;
                }
                g_warning ("error while reading from socket %s (read_bytes %d)",
                           strerror (errno), read_bytes);
            }
            g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
            client->string_length = 0;
            return SQUALE_CLIENT_READ_DISCONNECTED;
        }
    }
    else { /* That's a second call for the same string */
        gint needed_bytes = client->string_length - client->read_so_far;
        read_bytes = read (client->client_fd, *string + client->read_so_far,
                           needed_bytes);
        if (read_bytes == needed_bytes) {
            /* We read the complete string */
            client->read_so_far = client->string_length = 0;
            return SQUALE_CLIENT_READ_OK;
        }
        else if (read_bytes > 0 && read_bytes < needed_bytes) {
            /* We read part of the string */
            client->read_so_far += read_bytes;
            return SQUALE_CLIENT_READ_PARTIAL;
        }
        else { /* An error occured while reading string */
            if (read_bytes == 0) {
                g_warning ("reached end of stream on socket but we " \
            "haven't read the string completely: read %d, expected %d",
                           read_bytes, needed_bytes);
            }
            else {
                if (errno == EAGAIN) {
                    return SQUALE_CLIENT_READ_PARTIAL;
                }
                g_warning ("error while reading from socket %s", strerror (errno));
            }
            g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
            client->read_so_far = client->string_length = 0;
            return SQUALE_CLIENT_READ_DISCONNECTED;
        }
    }
}

static gboolean
squale_client_client_io_watch (GIOChannel *source, GIOCondition condition,
                               gpointer user_data)
{
    SqualeClient *client = NULL;
    SqualeClientReadReturn ret;

    g_return_val_if_fail (SQUALE_IS_CLIENT (user_data), FALSE);

    client = SQUALE_CLIENT (user_data);

    /* The socket becomes readable */
    if (condition & G_IO_IN) {
        switch (client->status) {
            case SQUALE_CLIENT_STARTUP:
                ret = squale_client_read_string (client, &(client->order_joblist));
                switch (ret) {
                    case SQUALE_CLIENT_READ_OK:
                        client->status = SQUALE_CLIENT_CONNECTION;
                        break;
                    case SQUALE_CLIENT_READ_PARTIAL:
                        return TRUE;
                        break;
                    case SQUALE_CLIENT_READ_DISCONNECTED:
                        return FALSE;
                }
                break;
            case SQUALE_CLIENT_CONNECTION:
                ret = squale_client_read_string (client, &(client->incoming_order));
                switch (ret) {
                    case SQUALE_CLIENT_READ_OK:
                        if (client->client_timeout) {
                            g_source_remove (client->client_timeout);
                            client->client_timeout = 0;
                        }

                        client->status = SQUALE_CLIENT_ORDER;
                        squale_client_execute (client);
                        break;
                    case SQUALE_CLIENT_READ_PARTIAL:
                        return TRUE;
                    case SQUALE_CLIENT_READ_DISCONNECTED:
                        return FALSE;
                }
                break;
            default:
            {
                gint read_bytes = 0;
                gchar c;
                read_bytes = read (client->client_fd, &c, 1);
                if (read_bytes == 0) {
                    /* The remote client has closed the socket this client object will be
                       destroyed */
                    g_message ("remote client has closed the connection");
                }
                else {
                    /* That should not happen */
                    g_warning ("client is still sending data exceeding header size");
                }
                g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
                return FALSE;
            }
        }
    }

    /* The socket becomes writable */
    if (condition & G_IO_OUT) {
        gint32 wrote_bytes;
        switch (client->status) {
            case SQUALE_CLIENT_SEND_RESULT:
                wrote_bytes = write (client->client_fd,
                                     client->out_buf + client->written_so_far,
                                     client->out_buf_size - client->written_so_far);
                if (wrote_bytes > 0) {
                    client->written_so_far += wrote_bytes;

                    if (client->written_so_far >= client->out_buf_size) {
                        /* We wrote everything we wanted to send */
                        client->status = SQUALE_CLIENT_RESULT_SENT;
                        g_message (_("Data transfer to client %p completed successfully"),
                                   client);
                        g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
                        return FALSE;
                    }
                }
                else {
                    g_warning ("error while writing to socket %s", strerror (errno));
                    g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
                    return FALSE;
                }
                break;
            default:
                /* We don't have anything to send yet */
                break;
        }
    }

    if (condition & G_IO_HUP) {
        g_message ("HUP remote client has closed the connection");
        g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
        return FALSE;
    }

    if (condition & G_IO_ERR) {
        g_message ("Errors received on client socket, disconnecting...");
        g_signal_emit (client, client_signals[DISCONNECTED], 0, NULL);
        return FALSE;
    }

    return TRUE;
}

/* =========================================== */
/*                                             */
/*              Init & Class init              */
/*                                             */
/* =========================================== */

static void
squale_client_dispose (GObject *object)
{
    SqualeClient *client = NULL;

    client = SQUALE_CLIENT (object);

    if (client->client_timeout) {
        g_source_remove (client->client_timeout);
        client->client_timeout = 0;
    }

    if (client->out_buf) {
        g_free (client->out_buf);
        client->out_buf = NULL;
        client->out_buf_size = 0;
    }

    if (client->job_sourceid) {
        /* Remove the IOChannel from the main loop */
        g_source_remove (client->job_sourceid);
        client->job_sourceid = 0;
    }

    if (client->in_sourceid) {
        /* Remove the IOChannel from the main loop */
        g_source_remove (client->in_sourceid);
        client->in_sourceid = 0;
    }

    if (client->job_io_channel) {
        g_io_channel_shutdown (client->job_io_channel, TRUE, NULL);
        /* Loose our reference */
        g_io_channel_unref (client->job_io_channel);
        client->job_io_channel = NULL;
    }

    if (client->client_io_channel) {
        g_io_channel_shutdown (client->client_io_channel, TRUE, NULL);
        /* Loose our reference */
        g_io_channel_unref (client->client_io_channel);
        client->client_io_channel = NULL;
    }

    /* We remove normal jobs from the joblist */
    if (SQUALE_IS_JOB (client->job) &&
        SQUALE_IS_JOBLIST (client->joblist) &&
        client->job->job_type == SQUALE_JOB_NORMAL) {
        squale_joblist_remove_job (client->joblist, client->job);
        /* We don't touch the job as the joblist stole our ref */
        client->job = NULL;
    }

    if (SQUALE_IS_JOBLIST (client->joblist)) {
        g_object_unref (client->joblist);
        client->joblist = NULL;
    }

    /* That was an internal job, unrefing it */
    if (SQUALE_IS_JOB (client->job)) {
        g_object_unref (client->job);
        client->job = NULL;
    }

    if (client->order_joblist) {
        g_free (client->order_joblist);
        client->order_joblist = NULL;
    }

    if (client->incoming_order) {
        g_free (client->incoming_order);
        client->incoming_order = NULL;
    }

#ifdef HAVE_DMALLOC
    dmalloc_log_changed (client->dmalloc_mark,
      1 /* log unfreed pointers */,
      0 /* do not log freed pointers */,
      1 /* log individual pointers otherwise a summary */);
#endif

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_client_init (SqualeClient *client)
{
    /* Initial status */
    client->status = SQUALE_CLIENT_STARTUP;

    client->job_io_channel = NULL;
    client->client_io_channel = NULL;

    client->client_timeout = client->in_sourceid = client->job_sourceid = 0;

    /* Structure to receive order */
    client->order_joblist = NULL;
    client->incoming_order = NULL;
    client->out_buf = NULL;
    client->out_buf_size = 0;
    client->written_so_far = 0;
    client->read_so_far = 0;
    client->string_length = 0;

    /* Joblists and our job */
    client->joblists = NULL;
    client->joblist = NULL;
    client->job = NULL;

#ifdef HAVE_DMALLOC
    /* get the current dmalloc position */
  client->dmalloc_mark = dmalloc_mark () ;
#endif
}

static void
squale_client_class_init (SqualeClientClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->dispose = squale_client_dispose;

    klass->disconnected = squale_client_disconnected;

    client_signals[DISCONNECTED] =
            g_signal_new ("disconnected",
                          G_TYPE_FROM_CLASS (klass), G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeClientClass, disconnected),
                          NULL, NULL, g_cclosure_marshal_VOID__VOID, G_TYPE_NONE, 0);
    client_signals[STATS] =
            g_signal_new ("stats",
                          G_TYPE_FROM_CLASS (klass), G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeClientClass, stats),
                          NULL, NULL, g_cclosure_marshal_VOID__POINTER, G_TYPE_NONE,
                          1, G_TYPE_POINTER);
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

void
squale_client_set_fd (SqualeClient *client, gint client_fd)
{
    gint val;

    g_return_if_fail (SQUALE_IS_CLIENT (client));

    /* Make it non blocking */
    val = fcntl (client_fd, F_GETFL, 0);
    if (val < 0 ) {
        g_warning ("Failed to get flags from socket fd : %s", strerror (errno));
    }
    if (fcntl (client_fd, F_SETFL, val | O_NONBLOCK) < 0) {
        g_warning ("Failed to set non blocking flags on socket fd : %s",
                   strerror (errno));
    }

    client->client_io_channel = g_io_channel_unix_new (client_fd);
    client->client_fd = client_fd;
}

void
squale_client_handle (SqualeClient *client, GList *joblists)
{
    g_return_if_fail (SQUALE_IS_CLIENT (client));
    g_return_if_fail (joblists != NULL);

    /* We will use that list of joblist to find the appropriate joblist for our
       client's job */
    client->joblists = joblists;

    /* We set a timeout for the input communication protocol. That means
       that if the client has not completed the order protocol correctly
       before the timeout is triggered we will simply close the socket and
       report that client as disconnected */
    if (client->client_timeout == 0) {
        client->client_timeout = g_timeout_add (1000, squale_client_timeout,
                                                client);
    }

    /* We watch the client socket for input only */
    client->in_sourceid = g_io_add_watch (client->client_io_channel,
                                          G_IO_IN | G_IO_ERR | G_IO_HUP, squale_client_client_io_watch, client);
}

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_client_get_type (void)
{
    static GType client_type = 0;

    if (!client_type) {
        static const GTypeInfo client_info = {
                sizeof (SqualeClientClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_client_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeClient),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_client_init,
                NULL                    /* value_table */
        };

        client_type = g_type_register_static (G_TYPE_OBJECT, "SqualeClient",
                                              &client_info, (GTypeFlags) 0);
    }
    return client_type;
}

SqualeClient *
squale_client_new (void)
{
    SqualeClient *client = g_object_new (SQUALE_TYPE_CLIENT, NULL);

    return client;
}
