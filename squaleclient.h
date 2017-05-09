/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squaleclient.h : Header for SqualeClient object.
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

#ifndef __SQUALE_CLIENT_H__
#define __SQUALE_CLIENT_H__

#include <glib-object.h>

typedef struct _SqualeClient SqualeClient;
typedef struct _SqualeClientClass SqualeClientClass;

#include "squalejoblist.h"

#define SQUALE_TYPE_CLIENT            (squale_client_get_type ())
#define SQUALE_CLIENT(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_CLIENT, SqualeClient))
#define SQUALE_CLIENT_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_CLIENT, SqualeClientClass))
#define SQUALE_IS_CLIENT(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_CLIENT))
#define SQUALE_IS_CLIENT_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_CLIENT))
#define SQUALE_CLIENT_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_CLIENT, SqualeClientClass))

typedef enum {
    SQUALE_CLIENT_STARTUP,
    SQUALE_CLIENT_CONNECTION_HEADER,
    SQUALE_CLIENT_CONNECTION,
    SQUALE_CLIENT_ORDER_HEADER,
    SQUALE_CLIENT_ORDER,
    SQUALE_CLIENT_SEND_RESULT,
    SQUALE_CLIENT_RESULT_SENT
} SqualeClientStatus;

struct _SqualeClient
{
    GObject object;

    SqualeClientStatus status;

    GIOChannel *job_io_channel;
    GIOChannel *client_io_channel;

    gint client_fd;
    guint client_timeout;
    guint in_sourceid;
    guint job_sourceid;

    gint read_so_far;
    gint written_so_far;
    gint string_length;

    gint32 out_buf_size;

    char *order_joblist;
    char *incoming_order;
    char *out_buf;

    GList *joblists;
    SqualeJobList *joblist;
    SqualeJob *job;

    unsigned long dmalloc_mark;
};

struct _SqualeClientClass
{
    GObjectClass parent_class;

    void (*disconnected) (SqualeClient *client);
    void (*stats) (SqualeClient *client, GHashTable *hash);
};

GType squale_client_get_type (void);

SqualeClient *squale_client_new (void);

void squale_client_set_fd (SqualeClient *client, gint client_fd);

void squale_client_handle (SqualeClient *client, GList *joblists);

#endif /* __SQUALE_CLIENT_H__ */
