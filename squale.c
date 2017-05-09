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

#include <locale.h>
#include <unistd.h>
#include <sys/stat.h>

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

static GMainLoop *loop = NULL;
static Squale *squale = NULL;

char *dmalloc_logpath = NULL;

static void
squale_launch_workers (Squale *squale)
{
    GList *workers = NULL;

    g_return_if_fail (squale != NULL);
    g_return_if_fail (squale->xml != NULL);

    workers = squale->xml->workers;

    g_message (_("Launching workers"));

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);

        if (SQUALE_IS_WORKER (worker)) {
            worker->thread = g_thread_create (squale_worker_run, worker, FALSE, NULL);
        }

        workers = g_list_next (workers);
    }
}

static void
squale_shutdown_workers (Squale *squale)
{
    GList *workers = NULL;

    g_return_if_fail (squale != NULL);
    g_return_if_fail (squale->xml != NULL);

    workers = squale->xml->workers;

    g_message (_("Shutting down workers"));

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);

        if (SQUALE_IS_WORKER (worker))
            squale_worker_shutdown (worker);

        workers = g_list_next (workers);
    }
}

static void
squale_shutdown_joblist (SqualeJobList *joblist, Squale *squale)
{
    GList *workers = NULL;

    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    workers = squale->xml->workers;

    g_message (_("Shutting down joblist '%s'"), joblist->name);

    /* We mark the list as closed */
    squale_joblist_set_status (joblist, SQUALE_JOBLIST_CLOSED);

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);

        /* Check if this worker belongs to our joblist */
        if (SQUALE_IS_WORKER (worker) && worker->joblist == joblist) {
            /* Shut it down */
            squale_worker_shutdown (worker);
        }

        workers = g_list_next (workers);
    }

    /* Clearing all jobs */
    squale_joblist_clear (joblist);
}

static void
squale_startup_joblist (SqualeJobList *joblist, Squale *squale)
{
    GList *workers = NULL;

    g_return_if_fail (SQUALE_IS_JOBLIST (joblist));

    workers = squale->xml->workers;

    g_message (_("Starting up joblist '%s'"), joblist->name);

    /* Clearing the joblist */
    squale_joblist_clear (joblist);

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);

        /* Check if this worker belongs to our joblist */
        if (SQUALE_IS_WORKER (worker) && worker->joblist == joblist) {
            /* Launching worker */
            worker->thread = g_thread_create (squale_worker_run, worker, FALSE, NULL);
        }

        workers = g_list_next (workers);
    }

    /* Mark the joblist as opened */
    squale_joblist_set_status (joblist, SQUALE_JOBLIST_OPENED);
}

/* When a client loses connection this signal is triggered */
static void
squale_disconnected_client (SqualeClient *client, Squale *squale)
{
    g_return_if_fail (SQUALE_IS_CLIENT (client));

    g_message (_("Remote client %p has been disconnected"), client);

    squale_listener_remove_client (squale->listener, client);
}

static void
squale_stats_client (SqualeClient *client, gpointer data, Squale *squale)
{
    GHashTable *hash = (GHashTable *) data;
    struct timeval current_time;
    GList *joblists = NULL;
    char *joblists_string = NULL;

    g_return_if_fail (SQUALE_IS_CLIENT (client));

    g_message (_("Generating global stats"));

    gettimeofday (&(current_time), NULL);

    g_hash_table_insert (hash, g_strdup (_("uptime (s)")),
                         g_strdup_printf ("%lu", current_time.tv_sec - squale->startup_ts.tv_sec));

    g_hash_table_insert (hash, g_strdup (_("version")), g_strdup (VERSION));

    g_hash_table_insert (hash, g_strdup (_("connected_clients")),
                         g_strdup_printf ("%d", squale_listener_count_clients (squale->listener)));

    joblists = squale->xml->joblists;

    while (joblists) {
        SqualeJobList *joblist = SQUALE_JOBLIST (joblists->data);

        if (SQUALE_IS_JOBLIST (joblist)) {
            char *buf = NULL;
            char *joblist_name = squale_joblist_get_name (joblist);
            if (joblists_string) {
                buf = g_strdup (joblists_string);
                joblists_string = g_strjoin (", ", buf, joblist_name, NULL);
                if (buf) {
                    g_free (buf);
                }
            }
            else {
                joblists_string = g_strdup (joblist_name);
            }
            if (joblist_name) {
                g_free (joblist_name);
            }
        }

        joblists = g_list_next (joblists);
    }

    g_hash_table_insert (hash, g_strdup ("connections"),
                         g_strdup (joblists_string));

    if (joblists_string) {
        g_free (joblists_string);
    }
}

static void
squale_accept_new_client (SqualeListener *listener, gint client_fd,
                          Squale *squale)
{
    SqualeClient *client = squale_client_new ();

    g_return_if_fail (SQUALE_IS_CLIENT (client));
    g_return_if_fail (SQUALE_IS_LISTENER (listener));
    g_return_if_fail (squale != NULL);

    g_message (_("Accepting a new client"));

    squale_client_set_fd (client, client_fd);

    squale_listener_add_client (listener, client);

    g_signal_connect_after (client, "disconnected",
                            G_CALLBACK (squale_disconnected_client), squale);
    g_signal_connect (client, "stats",
                      G_CALLBACK (squale_stats_client), squale);

    squale_client_handle (client, squale->xml->joblists);
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

/* This function receives a reference to a pointer, the number of allocated
   bytes in that pointer, the current offset where we are supposed to write
   and the number of bytes we will have to write. It then takes care of checking
   that at least 10% of the allocated space will be available after needed_bytes
   are written. If that's not the case it's doubling the allocation until there
   is enough space to write the block of data */
gboolean
squale_check_mem_block (char **data_pointer, gulong *allocated_bytes,
                        gulong current_offset, gulong needed_bytes)
{
    glong remaining_bytes = 0, minimum_space = 0;

    g_return_val_if_fail (data_pointer != NULL, FALSE);
    g_return_val_if_fail (*data_pointer != NULL, FALSE);
    g_return_val_if_fail (allocated_bytes != NULL, FALSE);
    g_return_val_if_fail (current_offset < *allocated_bytes, FALSE);

    remaining_bytes = *allocated_bytes - current_offset - needed_bytes;
    minimum_space = *allocated_bytes / 10;

    /* We want 10% space available at least */
    if (remaining_bytes < minimum_space) {
        gpointer new_pointer = NULL;
        gulong new_allocation = *allocated_bytes * 2;

        /* Doubling allocation size until the data can fit */
        while ( (new_allocation - current_offset) < needed_bytes ) {
            new_allocation *= 2;
        }

        new_pointer = g_try_realloc (*data_pointer, new_allocation);
        if (new_pointer == NULL) {
            g_error ("Failed reallocating %lu bytes, that's very bad",
                     new_allocation);
            return FALSE;
        }
        else {
            g_message ("successfully reallocated %ld bytes to fit %ld bytes " \
          "from %ld offset",new_allocation,needed_bytes, current_offset );
            *data_pointer = new_pointer;
            *allocated_bytes = new_allocation;
        }
    }

    return TRUE;
}

void
squale_set_log_level (Squale *squale, const char *log_level, gboolean override)
{
    static gboolean forced = FALSE;

    g_return_if_fail (squale != NULL);
    g_return_if_fail (log_level != NULL);

    if (override) {
        forced = TRUE;
    }
    else {
        if (forced) {
            /* If log level has been enforced we don't accept new changes */
            return;
        }
    }

    g_message (_("Setting log level to '%s'"), log_level);

    if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_ERROR,
                             strlen (SQUALE_LOG_LEVEL_ERROR)) == 0) {
        squale->log_level = G_LOG_LEVEL_ERROR;
    }
    else if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_CRITICAL,
                                  strlen (SQUALE_LOG_LEVEL_CRITICAL)) == 0) {
        squale->log_level = G_LOG_LEVEL_CRITICAL;
    }
    else if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_WARNING,
                                  strlen (SQUALE_LOG_LEVEL_WARNING)) == 0) {
        squale->log_level = G_LOG_LEVEL_WARNING;
    }
    else if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_MESSAGE,
                                  strlen (SQUALE_LOG_LEVEL_MESSAGE)) == 0) {
        squale->log_level = G_LOG_LEVEL_MESSAGE;
    }
    else if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_INFO,
                                  strlen (SQUALE_LOG_LEVEL_INFO)) == 0) {
        squale->log_level = G_LOG_LEVEL_INFO;
    }
    else if (g_ascii_strncasecmp (log_level, SQUALE_LOG_LEVEL_DEBUG,
                                  strlen (SQUALE_LOG_LEVEL_DEBUG)) == 0) {
        squale->log_level = G_LOG_LEVEL_DEBUG;
    }
    else {
        g_warning ("Unknown log level '%s', using WARNING", log_level);
    }
}

void
squale_set_log_file (Squale *squale, const char *log_file, gboolean override)
{
    static gboolean forced = FALSE;

    g_return_if_fail (squale != NULL);

    if (squale->no_detach) {
        /* We refuse setting log file as we are in no_detach mode */
        return;
    }

    if (override) {
        forced = TRUE;
    }
    else {
        if (forced) {
            return;
        }
    }

    g_mutex_lock (squale->log_mutex);

    if (squale->log_fd) {
        fclose (squale->log_fd);
        squale->log_fd = NULL;
    }

    if (squale->log_file_name) {
        g_free (squale->log_file_name);
        squale->log_file_name = NULL;
    }

    if (log_file == NULL) {
        g_mutex_unlock (squale->log_mutex);
        return;
    }

    squale->log_file_name = g_strdup (log_file);

    /* Opening the new file */
    squale->log_fd = fopen (log_file, "a");

    g_mutex_unlock (squale->log_mutex);

#ifdef HAVE_DMALLOC
    dmalloc_logpath = g_strdup_printf ("%s.dmalloc", log_file);
#endif

    if (squale->log_fd == NULL) {
        g_warning ("Impossible to open log file '%s' in append mode", log_file);
    }
    else {
        g_warning (_("Setting log file to '%s'"), log_file);
    }
}

void
squale_set_socket_name (Squale *squale, const char *socket_name)
{
    g_return_if_fail (squale != NULL);

    g_message (_("Setting socket name to '%s'"), socket_name);

    if (squale->socket_name) {
        g_free (squale->socket_name);
        squale->socket_name = NULL;
    }

    squale->socket_name = g_strdup (socket_name);
}

static void
squale_daemonize ()
{
    switch (fork ()) {
        case -1:
            g_critical (_("Failed to fork as a daemon, not detaching."));
        case 0:
            if (setsid () == -1) {
                g_error ("Failed creating a new session.");
            }
            else {
                switch (fork ()) {
                    case -1:
                        g_critical (_("Failed to fork as a daemon, not detaching."));
                    case 0:
                        chdir ("/");
                        umask (0);
                        close (0);
                        close (1);
                        close (2);
                        break;
                    default:
                        exit (0);
                }
            }
            break;
        default:
            exit (0);
    }
}

static void
squale_log_rotate (int sig)
{
    char *log_file_name = g_strdup (squale->log_file_name);
    squale_set_log_file (squale, log_file_name, FALSE);
    if (log_file_name) {
        g_free (log_file_name);
    }
#ifdef HAVE_DMALLOC
    dmalloc_log_stats ();
#endif
}

void
squale_quit (int sig)
{
    g_warning (_("Exiting..."));
    g_main_loop_quit (loop);
}

int
main (int argc, const char **argv)
{
    guint log_handler_id;
    gint option_id;
    GList *joblists = NULL;
    char *log_file = NULL, *log_level = NULL, *config_file = NULL;
    gboolean no_detach = FALSE;
    poptContext popt_context;
    struct poptOption options_table[] =
            {
                    POPT_AUTOHELP
                    {"no-detach", 'd', POPT_ARG_NONE, &no_detach, 1,
                                N_("Don't detach as a daemon and log on stdout."), NULL},
                    {"config-file", 'c', POPT_ARG_STRING, &config_file, 2,
                                N_("Use a specific XML configuration file."), _("FILE")},
                    {"log-file", 'f', POPT_ARG_STRING, &log_file, 3,
                                N_("Write log output to a specific log file."), _("FILE")},
                    {"log-level", 'l', POPT_ARG_STRING, &log_level, 4,
                                N_("Define the level of log messages to be written to log file."),
                                "[CRITICAL, WARNING, MESSAGE]"},
                    POPT_TABLEEND
            };

#ifdef HAVE_DMALLOC
    /* options : debug=0x4e48503, */
  dmalloc_debug_setup ("debug=0x4f48d03,inter=100,lockon=20");
#endif

    g_type_init ();
    g_thread_init (NULL);

    /* i18n initialization */
    setlocale (LC_MESSAGES, "");
    bindtextdomain (GETTEXT_PACKAGE, SQUALELOCALEDIR);
    bind_textdomain_codeset (GETTEXT_PACKAGE, "UTF-8");
    textdomain (GETTEXT_PACKAGE);

    popt_context = poptGetContext (NULL, argc, argv, options_table, 0);

    while ((option_id = poptGetNextOpt (popt_context)) != -1) {
        if (option_id < 1 || option_id > 4) {
            poptPrintHelp (popt_context, stderr, 0);
            return 1;
        }
    }

    poptFreeContext (popt_context);

    squale = g_new0 (Squale, 1);

    /* If the no-detach command line option is not set we daemonize */
    if (no_detach == FALSE) {
        squale_daemonize ();
    }
    else {
        /* Otherwise we memorize that fact to keep log output on stdout */
        squale->no_detach = TRUE;
    }

    /* Connecting signals */
    if (signal (SIGTERM, squale_quit) == SIG_IGN)
        signal (SIGTERM, SIG_IGN);
    if (signal (SIGINT, squale_quit) == SIG_IGN)
        signal (SIGINT, SIG_IGN);
    if (signal (SIGHUP, squale_log_rotate) == SIG_IGN)
        signal (SIGHUP, SIG_IGN);

    signal (SIGPIPE, SIG_IGN);

    /* Try to ignore the file size exceeded signal, we don't want to get stopped
       because the sysadmin is not rotating log files ;-) */
    signal (SIGXFSZ, SIG_IGN);

    /* Store startup timestamp */
    gettimeofday (&(squale->startup_ts), NULL);

    /* Default socket if the parameter is omitted in configuration file */
    squale->socket_name = g_strdup ("/tmp/squale.sock");

    squale->xml = g_new0 (SqualeXML, 1);

    squale->log_mutex = g_mutex_new ();

    log_handler_id = g_log_set_handler (G_LOG_DOMAIN, G_LOG_LEVEL_WARNING |
                                                      G_LOG_LEVEL_CRITICAL | G_LOG_LEVEL_ERROR | G_LOG_LEVEL_MESSAGE |
                                                      G_LOG_LEVEL_INFO | G_LOG_LEVEL_DEBUG | G_LOG_FLAG_FATAL |
                                                      G_LOG_FLAG_RECURSION, squale_log_handler, squale);

    /* Overriding XML settings from command line options */
    if (log_file) {
        squale_set_log_file (squale, log_file, TRUE);
    }

    if (log_level) {
        squale_set_log_level (squale, log_level, TRUE);
    }
    else {
        squale->log_level = G_LOG_LEVEL_MESSAGE;
    }

    loop = g_main_loop_new (NULL, FALSE);

    if (config_file) {
        squale->xml = squale_xml_new (config_file, squale);
    }
    else {
        squale->xml = squale_xml_new (SQUALESYSCONFDIR"/squale.xml", squale);
    }

    if (squale->xml == NULL) {
        g_error (_("Failed to load XML configuration file, aborting."));
    }

#ifdef HAVE_DMALLOC
    g_warning (_("Starting up SQuaLe %s with DMALLOC support"), VERSION);
#else
    g_warning (_("Starting up SQuaLe %s"), VERSION);
#endif

    joblists = squale->xml->joblists;

    while (joblists) {
        SqualeJobList *joblist = SQUALE_JOBLIST (joblists->data);

        if (SQUALE_IS_JOBLIST (joblist)) {
            g_signal_connect (joblist, "startup",
                              G_CALLBACK (squale_startup_joblist), squale);
            g_signal_connect (joblist, "shutdown",
                              G_CALLBACK (squale_shutdown_joblist), squale);
        }

        joblists = g_list_next (joblists);
    }

    squale->listener = squale_listener_new ();

    if (SQUALE_IS_LISTENER (squale->listener)) {
        if (!squale_listener_open (squale->listener, squale->socket_name)) {
            g_critical (_("Failed opening listener socket '%s'"),
                        squale->socket_name);
            squale_xml_destroy (squale->xml);

            if (squale->xml) {
                g_free (squale->xml);
                squale->xml = NULL;
            }

            squale_set_log_file (squale, NULL, TRUE);

            /* Destroying our Squale structure. */
            if (squale) {
                g_mutex_free (squale->log_mutex);
                squale->log_mutex = NULL;

                if (squale->socket_name) {
                    g_free (squale->socket_name);
                    squale->socket_name = NULL;
                }

                g_free (squale);
                squale = NULL;
            }

            return -1;
        }
        else {
            g_message (_("Listening on unix socket '%s'"), squale->socket_name);
            g_signal_connect (squale->listener, "new-client",
                              G_CALLBACK (squale_accept_new_client), squale);
        }
    }
    else {
        g_critical ("Failed creating listener");

        squale_xml_destroy (squale->xml);

        if (squale->xml) {
            g_free (squale->xml);
            squale->xml = NULL;
        }

        squale_set_log_file (squale, NULL, TRUE);

        /* Destroying our Squale structure. */
        if (squale) {
            g_mutex_free (squale->log_mutex);
            squale->log_mutex = NULL;

            if (squale->socket_name) {
                g_free (squale->socket_name);
                squale->socket_name = NULL;
            }

            g_free (squale);
            squale = NULL;
        }

        return -1;
    }

    squale_launch_workers (squale);

    g_main_loop_run (loop);

    /* Stop accepting connections */
    squale_listener_close (squale->listener);

    /* Tell workers to shutdown after current task */
    squale_shutdown_workers (squale);

    /* Destroy listener and attached clients */
    g_object_unref (squale->listener);
    squale->listener = NULL;

    /* Destroying what we have created from XML file (workers, joblists) */
    squale_xml_destroy (squale->xml);

    if (squale->xml) {
        g_free (squale->xml);
        squale->xml = NULL;
    }

    /* Removing log handlers */
    g_message (_("Removing log handler"));

    squale_set_log_file (squale, NULL, TRUE);

    g_log_remove_handler (G_LOG_DOMAIN, log_handler_id);

    /* Destroying our Squale structure. */
    if (squale) {
        g_mutex_free (squale->log_mutex);
        squale->log_mutex = NULL;

        if (squale->socket_name) {
            g_free (squale->socket_name);
            squale->socket_name = NULL;
        }

        g_free (squale);
        squale = NULL;
    }

    g_main_loop_unref (loop);

#ifdef HAVE_DMALLOC
    dmalloc_shutdown ();
#endif

    return 0;
}
