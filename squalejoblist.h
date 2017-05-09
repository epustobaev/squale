/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalejoblist.h : Header for SqualeJobList object.
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

#ifndef __SQUALE_JOBLIST_H__
#define __SQUALE_JOBLIST_H__

#include <glib-object.h>

typedef struct _SqualeJobList SqualeJobList;
typedef struct _SqualeJobListClass SqualeJobListClass;

#include "squalejob.h"
#include "squaleworker.h"

#define SQUALE_TYPE_JOBLIST            (squale_joblist_get_type ())
#define SQUALE_JOBLIST(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_JOBLIST, SqualeJobList))
#define SQUALE_JOBLIST_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_JOBLIST, SqualeJobListClass))
#define SQUALE_IS_JOBLIST(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_JOBLIST))
#define SQUALE_IS_JOBLIST_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_JOBLIST))
#define SQUALE_JOBLIST_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_JOBLIST, SqualeJobListClass))

typedef enum
{
    SQUALE_JOBLIST_OPENED,
    SQUALE_JOBLIST_CLOSED
} SqualeJobListStatus;

struct _SqualeJobList
{
    GObject object;

    SqualeJobListStatus status;

    GList *jobs;

    GMutex *list_mutex;
    GCond *cond;

    char *name;
    char *backend;
    guint max_pending_warn;
    guint max_pending_block;

    GList *workers;

    /* Statistics */
    gulong assign_total_time;
    gulong nb_assign;
    gulong process_total_time;
    gulong nb_process;
    gulong nb_errors;

    struct timeval startup_ts;
};

struct _SqualeJobListClass
{
    GObjectClass parent_class;

    /* Signals */
    void (*shutdown) (SqualeJobList *joblist);
    void (*startup) (SqualeJobList *joblist);
    void (*stats) (SqualeJobList *joblist, GHashTable *hash);
};

GType squale_joblist_get_type (void);

SqualeJobList *squale_joblist_new (void);

void squale_joblist_set_name (SqualeJobList *joblist, const char *name);
char *squale_joblist_get_name (SqualeJobList *joblist);
void squale_joblist_set_backend (SqualeJobList *joblist, const char *backend);
char * squale_joblist_get_backend (SqualeJobList *joblist);

gboolean squale_joblist_add_job (SqualeJobList *joblist, SqualeJob *job,
                                 GError **error);
gboolean squale_joblist_remove_job (SqualeJobList *joblist, SqualeJob *job);
SqualeJob *squale_joblist_assign_pending_job (SqualeJobList *joblist,
                                              gboolean keep_locking);
gboolean squale_joblist_giveup_job (SqualeJobList *joblist, SqualeJob *job);

void squale_joblist_set_max_pending_warn_level (SqualeJobList *joblist,
                                                guint max_pending);

void squale_joblist_set_max_pending_block_level (SqualeJobList *joblist,
                                                 guint max_pending);

gboolean squale_joblist_clear (SqualeJobList *joblist);

void squale_joblist_startup (SqualeJobList *joblist);
void squale_joblist_shutdown (SqualeJobList *joblist);

gboolean squale_joblist_get_stats (SqualeJobList *joblist, GHashTable *hash);

gboolean squale_joblist_set_status (SqualeJobList *joblist,
                                    SqualeJobListStatus status);

gboolean squale_joblist_add_worker (SqualeJobList *joblist,
                                    SqualeWorker *worker);
gboolean squale_joblist_remove_worker (SqualeJobList *joblist,
                                       SqualeWorker *worker);

#endif /* __SQUALE_JOBLIST_H__ */
