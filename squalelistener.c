/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalelistener.c : Source for SqualeListener object.
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

#include "squalelistener.h"
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

enum
{
    NEW_CLIENT,
    LAST_SIGNAL
};

static GObjectClass *parent_class = NULL;
static guint listener_signals[LAST_SIGNAL] = { 0 };

/* ============================================================= */
/*                                                               */
/*                       Private Methods                         */
/*                                                               */
/* ============================================================= */

static void
squale_listener_accept (SqualeListener *listener)
{
    gint client_fd;
    struct sockaddr addr;
    socklen_t addrlen = 0;

    g_return_if_fail (SQUALE_IS_LISTENER (listener));

    g_message (_("Listener %p accepting a new connection"), listener);

    /* Clear the struct */
    memset (&(addr), '\0', sizeof (addr));

    retry:
    client_fd = accept (listener->sockfd, &addr, &addrlen);

    if (client_fd < 0) {
        if (errno == EINTR)
            goto retry;
        else {
            g_warning (_("Failed accepting client on socket '%s': %s"),
                       listener->filename, strerror (errno));
        }
    }
    else {
        g_signal_emit (listener, listener_signals[NEW_CLIENT], 0, client_fd, NULL);
    }
}

static gboolean
squale_listener_io_watch (GIOChannel *source,
                          GIOCondition condition,
                          gpointer user_data)
{
    SqualeListener *listener = NULL;

    g_return_val_if_fail (SQUALE_IS_LISTENER (user_data), FALSE);

    listener = SQUALE_LISTENER (user_data);

    if (condition & G_IO_IN) {
        squale_listener_accept (listener);
    }

    if (condition & G_IO_HUP) {
        g_warning ("Listener socket received HUP");
        return FALSE;
    }

    if (condition & G_IO_ERR) {
        g_warning ("Listener socket received an error");
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
squale_listener_dispose (GObject *object)
{
    SqualeListener *listener = SQUALE_LISTENER (object);

    if (listener->clients) {
        g_list_foreach (listener->clients, (GFunc) g_object_unref, NULL);
        g_list_free (listener->clients);
        listener->clients = NULL;
    }

    if (listener->io_channel) {
        g_io_channel_shutdown (listener->io_channel, TRUE, NULL);
        g_io_channel_unref (listener->io_channel);
        listener->io_channel = NULL;
    }

    if (listener->filename) {
        g_free (listener->filename);
        listener->filename = NULL;
    }

    if (G_OBJECT_CLASS (parent_class)->dispose)
        G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
squale_listener_init (SqualeListener *listener)
{
    listener->sockfd = 0;
    listener->filename = NULL;
    listener->io_channel = NULL;
    listener->clients = NULL;
}

static void
squale_listener_class_init (SqualeListenerClass *klass)
{
    GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

    parent_class = g_type_class_peek_parent (klass);

    gobject_class->dispose = squale_listener_dispose;

    listener_signals[NEW_CLIENT] =
            g_signal_new ("new-client",
                          G_TYPE_FROM_CLASS (klass),
                          G_SIGNAL_RUN_FIRST,
                          G_STRUCT_OFFSET (SqualeListenerClass, new_client),
                          NULL, NULL, g_cclosure_marshal_VOID__INT, G_TYPE_NONE,
                          1, G_TYPE_INT);
}

/* ============================================================= */
/*                                                               */
/*                       Public Methods                          */
/*                                                               */
/* ============================================================= */

guint
squale_listener_count_clients (SqualeListener *listener)
{
    guint nb_clients = 0;

    g_return_val_if_fail (SQUALE_IS_LISTENER (listener), 0);

    nb_clients = g_list_length (listener->clients);

    g_message (_("There are %u clients connected to listener %p"), nb_clients,
               listener);

    return nb_clients;
}

gboolean
squale_listener_add_client (SqualeListener *listener, gpointer client)
{
    g_return_val_if_fail (SQUALE_IS_LISTENER (listener), FALSE);

    g_message (_("Adding client %p to listener %p"), client, listener);

    /* We steal the reference */
    listener->clients = g_list_append (listener->clients, client);

    return TRUE;
}

gboolean
squale_listener_remove_client (SqualeListener *listener, gpointer client)
{
    g_return_val_if_fail (SQUALE_IS_LISTENER (listener), FALSE);

    g_message (_("Removing client %p from listener %p"), client, listener);

    listener->clients = g_list_remove (listener->clients, client);

    g_object_unref (client);

    return TRUE;
}

gboolean
squale_listener_open (SqualeListener *listener, const char *filename)
{
    struct sockaddr_un addr;
    gint val;
    struct stat sb;

    g_return_val_if_fail (SQUALE_IS_LISTENER (listener), FALSE);
    g_return_val_if_fail (filename != NULL, FALSE);

    if (listener->filename)
        g_free (listener->filename);

    listener->filename = g_strdup (filename);

    g_message (_("Opening listener socket '%s'"), listener->filename);

    /* If a socket already exist with that name we delete it */
    if (stat (filename, &sb) == 0 && S_ISSOCK (sb.st_mode))
        unlink (filename);

    /* Create socket */
    listener->sockfd = socket (PF_UNIX, SOCK_STREAM, 0);
    if (listener->sockfd < 0) {
        g_warning (_("Failed to create socket '%s': %s"), filename,
                   strerror (errno));
        return FALSE;
    }

    /* Clear the struct */
    memset (&(addr), '\0', sizeof (addr));
    /* Unix socket */
    addr.sun_family = AF_UNIX;
    /* Set filename */
    strncpy (addr.sun_path, listener->filename, sizeof (addr.sun_path) - 1);
    /* Binding socket */
    if (bind (listener->sockfd, (struct sockaddr*) &addr, sizeof (addr)) < 0) {
        g_warning (_("Failed to bind socket '%s': %s"), filename,
                   strerror (errno));
        close (listener->sockfd);
        return FALSE;
    }
    /* Listening on socket */
    if (listen (listener->sockfd, 30) < 0) {
        g_warning (_("Failed to listen on socket '%s': %s"), filename,
                   strerror (errno));
        close (listener->sockfd);
        return FALSE;
    }

    /* Make it non blocking */
    val = fcntl (listener->sockfd, F_GETFL, 0);
    if (val < 0 ) {
        g_warning (_("Failed to get flags from socket fd '%s': %s"), filename,
                   strerror (errno));
    }
    if (fcntl (listener->sockfd, F_SETFL, val | O_NONBLOCK) < 0) {
        g_warning (_("Failed to set non blocking flags on socket fd '%s': %s"),
                   filename, strerror (errno));
    }

    if (chmod (filename, 0777) < 0) {
        g_warning (_("Failed to set mode 0777 on socket '%s': %s"), filename,
                   strerror (errno));
    }

    listener->io_channel = g_io_channel_unix_new (listener->sockfd);
    g_io_add_watch (listener->io_channel, G_IO_IN | G_IO_ERR | G_IO_HUP,
                    squale_listener_io_watch, listener);

    return TRUE;
}

gboolean
squale_listener_close (SqualeListener *listener)
{
    g_return_val_if_fail (SQUALE_IS_LISTENER (listener), FALSE);

    g_message (_("Closing listener socket '%s'"), listener->filename);

    /* Removing the GIOChannel, this closes the fd */
    if (listener->io_channel) {
        g_io_channel_shutdown (listener->io_channel, TRUE, NULL);
        g_io_channel_unref (listener->io_channel);
        listener->io_channel = NULL;
    }

    /* Remove the socket */
    if (listener->filename) {
        struct stat sb;
        if (stat (listener->filename, &sb) == 0 && S_ISSOCK (sb.st_mode)) {
            unlink (listener->filename);
        }
        g_free (listener->filename);
        listener->filename = NULL;
    }

    return TRUE;
}

/* =========================================== */
/*                                             */
/*          Object typing & Creation           */
/*                                             */
/* =========================================== */

GType
squale_listener_get_type (void)
{
    static GType listener_type = 0;

    if (!listener_type)
    {
        static const GTypeInfo listener_info = {
                sizeof (SqualeListenerClass),
                NULL,                   /* base_init */
                NULL,                   /* base_finalize */
                (GClassInitFunc) squale_listener_class_init,
                NULL,                   /* class_finalize */
                NULL,                   /* class_data */
                sizeof (SqualeListener),
                0,                      /* n_preallocs */
                (GInstanceInitFunc) squale_listener_init,
                NULL                    /* value_table */
        };

        listener_type =
                g_type_register_static (G_TYPE_OBJECT, "SqualeListener",
                                        &listener_info, (GTypeFlags) 0);
    }
    return listener_type;
}

SqualeListener *
squale_listener_new (void)
{
    SqualeListener *listener = g_object_new (SQUALE_TYPE_LISTENER, NULL);

    return listener;
}
