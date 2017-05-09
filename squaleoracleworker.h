/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squaleoracleworker.h : Header for SqualeOracleWorker object.
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

#ifndef __SQUALE_ORACLE_WORKER_H__
#define __SQUALE_ORACLE_WORKER_H__

#include "squaleworker.h"
#include <sqlora.h>

#define SQUALE_TYPE_ORACLE_WORKER            (squale_oracle_worker_get_type ())
#define SQUALE_ORACLE_WORKER(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_ORACLE_WORKER, SqualeOracleWorker))
#define SQUALE_ORACLE_WORKER_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_ORACLE_WORKER, SqualeOracleWorkerClass))
#define SQUALE_IS_ORACLE_WORKER(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_ORACLE_WORKER))
#define SQUALE_IS_ORACLE_WORKER_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_ORACLE_WORKER))
#define SQUALE_ORACLE_WORKER_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_ORACLE_WORKER, SqualeOracleWorkerClass))

typedef struct _SqualeOracleWorker SqualeOracleWorker;
typedef struct _SqualeOracleWorkerClass SqualeOracleWorkerClass;

struct _SqualeOracleWorker
{
    SqualeWorker worker;

    char *tnsname;
    char *user;
    char *passwd;
    guint64 commit_every;
    guint64 since_commit;

    sqlo_db_handle_t dbh;
};

struct _SqualeOracleWorkerClass
{
    SqualeWorkerClass parent_class;
};

GType squale_oracle_worker_get_type (void);

SqualeOracleWorker *squale_oracle_worker_new (void);

#endif /* __SQUALE_ORACLE_WORKER_H__ */
