/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalepgsqlworker.h : Header for SqualePgsqlWorker object.
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

#ifndef __SQUALE_PGSQL_WORKER_H__
#define __SQUALE_PGSQL_WORKER_H__

#include "squaleworker.h"
#include <libpq-fe.h>

#define SQUALE_TYPE_PGSQL_WORKER            (squale_pgsql_worker_get_type ())
#define SQUALE_PGSQL_WORKER(obj)            (G_TYPE_CHECK_INSTANCE_CAST ((obj), SQUALE_TYPE_PGSQL_WORKER, SqualePgsqlWorker))
#define SQUALE_PGSQL_WORKER_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), SQUALE_TYPE_PGSQL_WORKER, SqualePgsqlWorkerClass))
#define SQUALE_IS_PGSQL_WORKER(obj)         (G_TYPE_CHECK_INSTANCE_TYPE ((obj), SQUALE_TYPE_PGSQL_WORKER))
#define SQUALE_IS_PGSQL_WORKER_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), SQUALE_TYPE_PGSQL_WORKER))
#define SQUALE_PGSQL_WORKER_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), SQUALE_TYPE_PGSQL_WORKER, SqualePgsqlWorkerClass))

typedef struct _SqualePgsqlWorker SqualePgsqlWorker;
typedef struct _SqualePgsqlWorkerClass SqualePgsqlWorkerClass;

struct _SqualePgsqlWorker
{
    SqualeWorker worker;

    char *host;
    char *port;
    char *user;
    char *passwd;
    char *dbname;

    PGconn *conn;
};

struct _SqualePgsqlWorkerClass
{
    SqualeWorkerClass parent_class;
};

GType squale_pgsql_worker_get_type (void);

SqualePgsqlWorker *squale_pgsql_worker_new (void);

#endif /* __SQUALE_PGSQL_WORKER_H__ */
