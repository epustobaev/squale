/*  SQuaLe
 *
 *  Copyright (C) 2004 Julien Moutte <julien@moutte.net>
 *
 *  squalelistener.h : Header for SqualeListener object.
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

#ifndef __SQUALE_LISTENER_H__
#define __SQUALE_LISTENER_H__

#include <glib-object.h>

#define SQUALE_TYPE_LISTENER            (squale_listener_get_type ())
#define SQUALE_LISTENER(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_LISTENER, SqualeListener))
#define SQUALE_LISTENER_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_LISTENER, SqualeListenerClass))
#define SQUALE_IS_LISTENER(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_LISTENER))
#define SQUALE_IS_LISTENER_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_LISTENER))
#define SQUALE_LISTENER_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_LISTENER, SqualeListenerClass))

typedef struct _SqualeListener SqualeListener;
typedef struct _SqualeListenerClass SqualeListenerClass;

struct _SqualeListener
{
    GObject object;

    char *filename;
    gint sockfd;

    GIOChannel *io_channel;

    GList *clients;
};

struct _SqualeListenerClass
{
    GObjectClass parent_class;

    void (*new_client) (SqualeListener *listener, gint client_fd);
};

GType squale_listener_get_type (void);

SqualeListener *squale_listener_new (void);

gboolean squale_listener_open (SqualeListener *listener, const char *filename);
gboolean squale_listener_close (SqualeListener *listener);

gboolean squale_listener_add_client (SqualeListener *listener,
                                     gpointer client);
gboolean squale_listener_remove_client (SqualeListener *listener,
                                        gpointer client);

guint squale_listener_count_clients (SqualeListener *listener);

#endif /* __SQUALE_LISTENER_H__ */
