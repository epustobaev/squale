/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalexml.h : Header for xml parsing routines in SQuaLe.
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

#ifndef __SQUALE_XML_H__
#define __SQUALE_XML_H__

#include <libxml/parser.h>

typedef struct _SqualeXML SqualeXML;

#include "squale.h"

typedef enum
{
    PARSER_START,
    PARSER_SQUALE,
    PARSER_SETTINGS,
    PARSER_SETTING,
    PARSER_CONNECTIONS,
    PARSER_CONNECTION,
    PARSER_WORKER,
    PARSER_FINISH,
    PARSER_UNKNOWN
} ParserState;

typedef enum
{
    SQUALE_BACKEND_UNSET,
    SQUALE_BACKEND_ORACLE,
    SQUALE_BACKEND_MYSQL,
    SQUALE_BACKEND_PGSQL,
    SQUALE_BACKEND_UNSUPPORTED
} Backend;

typedef enum
{
    SQUALE_SETTING_LOGLEVEL,
    SQUALE_SETTING_LOGFILE,
    SQUALE_SETTING_SOCKETNAME
} Setting;

struct _SqualeXML
{
    ParserState state;
    ParserState prev_state;

    Setting setting;

    SqualeJobList *joblist;
    Backend joblist_backend;
    SqualeWorker *worker;

    GList *joblists;
    GList *workers;

    GHashTable *properties;

    Squale *squale;
};

SqualeXML *squale_xml_new (const char *filename, Squale *squale);

void squale_xml_destroy (SqualeXML *xml);

#endif /* __SQUALE_XML_H__ */
