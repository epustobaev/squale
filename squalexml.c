/*  SQuaLe
 *
 *  Copyright (C) 2005 Julien Moutte <julien@moutte.net>
 *
 *  squalexml.c : SQuaLe XML parsing routines.
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

#include "squalexml.h"
#include "squale-i18n.h"

#ifdef HAVE_DMALLOC
#include <dmalloc.h>
#endif

#ifdef HAVE_ORACLE
#include "squaleoracleworker.h"
#endif
#ifdef HAVE_MYSQL
#include "squalemysqlworker.h"
#endif
#ifdef HAVE_PGSQL
#include "squalepgsqlworker.h"
#endif

#ifdef HAVE_ORACLE
static void
squale_xml_sigint_handler (void)
{
  squale_quit (SIGINT);
}
#endif /* HAVE_ORACLE */

static void
squale_xml_set_properties (gpointer key, gpointer value, gpointer user_data)
{
    SqualeXML *xml = (SqualeXML *) user_data;

    if (SQUALE_IS_WORKER (xml->worker))
        g_object_set (xml->worker, (char *) key, (char *) value, NULL);
}

static gboolean
squale_xml_dummy_true_func (gpointer key, gpointer value, gpointer user_data)
{
    return TRUE;
}

static void
squale_xml_start_document (SqualeXML *xml)
{
    g_return_if_fail (xml != NULL);

    xml->prev_state = PARSER_UNKNOWN;
    xml->state = PARSER_START;

    xml->joblists = NULL;
    xml->workers = NULL;

    xml->properties = g_hash_table_new_full (g_str_hash, g_str_equal,
                                             g_free, g_free);
}

static void
squale_xml_end_document (SqualeXML *xml)
{
    g_return_if_fail (xml != NULL);

    if (xml->properties) {
        g_hash_table_destroy (xml->properties);
        xml->properties = NULL;
    }

    xml->state = PARSER_FINISH;
}

static void
squale_xml_start_element (SqualeXML *xml, const xmlChar *xml_name,
                          const xmlChar **xml_attrs)
{
    gchar * name = (gchar *) xml_name;
    gchar ** attrs = (gchar **) xml_attrs;

    g_return_if_fail (xml != NULL);

    switch (xml->state) {
        case PARSER_START:
            if (!strcmp (name, "squale")) {
                xml->state = PARSER_SQUALE;
            }
            else {
                g_warning ("squale_xml_start_element : Expected <squale>.  Got <%s>.",
                           name);
                xml->prev_state = xml->state;
                xml->state = PARSER_UNKNOWN;
            }
            break;
        case PARSER_SQUALE:
            if (!strcmp (name, "connections")) {
                xml->state = PARSER_CONNECTIONS;
            }
            else if (!strcmp (name, "settings")) {
                xml->state = PARSER_SETTINGS;
            }
            else {
                g_warning ("squale_xml_start_element : Unexpected element <%s> " \
            "inside <squale>.", name);
                xml->prev_state = xml->state;
                xml->state = PARSER_UNKNOWN;
            }
            break;
        case PARSER_SETTINGS:
            if (!strcmp (name, "setting")) {
                guint i;
                xml->state = PARSER_SETTING;
                for (i = 0; attrs && attrs[i] != NULL; i += 2) {
                    if (!strcmp(attrs[i], "name")) {
                        if (!strcmp (attrs[i+1], "log_level")) {
                            xml->setting = SQUALE_SETTING_LOGLEVEL;
                        }
                        else if (!strcmp (attrs[i+1], "log_file")) {
                            xml->setting = SQUALE_SETTING_LOGFILE;
                        }
                        else if (!strcmp (attrs[i+1], "socket_name")) {
                            xml->setting = SQUALE_SETTING_SOCKETNAME;
                        }
                        else {
                            g_warning ("squale_xml_start_element : Unknown setting name %s",
                                       attrs[i+1]);
                        }
                    }
                    else if (!strcmp(attrs[i], "value")) {
                        switch (xml->setting) {
                            case SQUALE_SETTING_LOGLEVEL:
                                squale_set_log_level (xml->squale, attrs[i+1], FALSE);
                                break;
                            case SQUALE_SETTING_LOGFILE:
                                squale_set_log_file (xml->squale, attrs[i+1], FALSE);
                                break;
                            case SQUALE_SETTING_SOCKETNAME:
                                squale_set_socket_name (xml->squale, attrs[i+1]);
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
            else {
                g_warning ("squale_xml_start_element : Expected <setting>." \
              "Got <%s>.", name);
                xml->prev_state = xml->state;
                xml->state = PARSER_UNKNOWN;
            }
            break;
        case PARSER_CONNECTIONS:
            if (!strcmp (name, "connection")) {
                guint i;
                xml->state = PARSER_CONNECTION;
                xml->joblist = squale_joblist_new ();
                for (i = 0; attrs && attrs[i] != NULL; i += 2) {
                    if (!strcmp(attrs[i], "backend")) {
                        xml->joblist_backend = SQUALE_BACKEND_UNSET;
                        if (!strcmp (attrs[i+1], "oracle")) {
                            static gboolean backend_initialized = FALSE;
                            /* Creating a joblist and storing the fact that we will create
                               Oracle workers for that connection */
#ifdef HAVE_ORACLE
                            xml->joblist_backend = SQUALE_BACKEND_ORACLE;

              if (!backend_initialized) {
                /* One cursor per connection, max number connection allowed */
                if (SQLO_SUCCESS != sqlo_init (SQLO_ON, 32767, 1)) {
                  g_warning ("Failed initing libsqlora8");
                }
                else {
                  gint handle;
                  backend_initialized = TRUE;
                  sqlo_register_int_handler (&handle,
                      squale_xml_sigint_handler);
                }
              }
#else
                            g_warning (_("Oracle support is not built in SQuaLe"));
                            backend_initialized = FALSE;
#endif
                        }
                        else if (!strcmp (attrs[i+1], "mysql")) {
                            /* Creating a joblist and storing the fact that we will create
                               MySql workers for that connection */
#ifdef HAVE_MYSQL
                            xml->joblist_backend = SQUALE_BACKEND_MYSQL;
#else
                            g_warning (_("MySql support is not built in SQuaLe"));
#endif
                        }
                        else if (!strcmp (attrs[i+1], "pgsql")) {
                            /* Creating a joblist and storing the fact that we will create
                               PostgreSQL workers for that connection */
#ifdef HAVE_PGSQL
                            xml->joblist_backend = SQUALE_BACKEND_PGSQL;
#else
                            g_warning (_("PostgreSQL support is not built in SQuaLe"));
#endif
                        }
                        else {
                            g_warning (_("Unsupported connection backend '%s'"), attrs[i+1]);
                        }
                        squale_joblist_set_backend (xml->joblist, attrs[i+1]);
                    }
                    else if (!strcmp(attrs[i], "max-pending-warn-level")) {
                        if (SQUALE_IS_JOBLIST (xml->joblist)) {
                            squale_joblist_set_max_pending_warn_level (xml->joblist,
                                                                       atoi (attrs[i+1]));
                        }
                    }
                    else if (!strcmp(attrs[i], "max-pending-block-level")) {
                        if (SQUALE_IS_JOBLIST (xml->joblist)) {
                            squale_joblist_set_max_pending_block_level (xml->joblist,
                                                                        atoi (attrs[i+1]));
                        }
                    }
                    else if (!strcmp(attrs[i], "name")) {
                        if (SQUALE_IS_JOBLIST (xml->joblist)) {
                            squale_joblist_set_name (xml->joblist, attrs[i+1]);
                        }
                    }
                    else {
                        /* Storing worker properties for that connection */
                        g_hash_table_replace (xml->properties, g_strdup (attrs[i]),
                                              g_strdup (attrs[i+1]));
                    }
                }
            }
            else {
                g_warning ("squale_xml_start_element : Expected <connection>." \
              "Got <%s>.",  name);
                xml->prev_state = xml->state;
                xml->state = PARSER_UNKNOWN;
            }
            break;
        case PARSER_CONNECTION:
            if (!strcmp (name, "worker")) {
                guint i;
                xml->state = PARSER_WORKER;
                xml->worker = NULL;

                /* We create the correct worker */
                switch (xml->joblist_backend) {
                    case SQUALE_BACKEND_ORACLE:
#ifdef HAVE_ORACLE
                        xml->worker = SQUALE_WORKER (squale_oracle_worker_new ());
#endif
                        break;
                    case SQUALE_BACKEND_MYSQL:
#ifdef HAVE_MYSQL
                        xml->worker = SQUALE_WORKER (squale_mysql_worker_new ());
#endif
                        break;
                    case SQUALE_BACKEND_PGSQL:
#ifdef HAVE_PGSQL
                        xml->worker = SQUALE_WORKER (squale_pgsql_worker_new ());
#endif
                        break;
                    default:
                        g_warning (_("Unsupported backend format. Impossible to create " \
                "worker."));
                        break;
                }

                /* If not worker created we exit from that block */
                if (!SQUALE_IS_WORKER (xml->worker))
                    break;
                else /* If a worker is there we connect it to the current joblist */
                    squale_worker_set_joblist (xml->worker, xml->joblist);

                /* We set the default connection attributes on that worker */
                g_hash_table_foreach (xml->properties, squale_xml_set_properties, xml);

                for (i = 0; attrs && attrs[i] != NULL; i += 2) {
                    if (SQUALE_IS_WORKER (xml->worker)) {
                        /* We set all the attributes on the worker as properties to override
                           the connection's defaults */
                        g_object_set (xml->worker, attrs[i], attrs[i+1], NULL);
                    }
                }
            }
            else {
                g_warning ("squale_xml_start_element : Unexpected element <%s>" \
            " inside <connection>.", name);
                xml->prev_state = xml->state;
                xml->state = PARSER_UNKNOWN;
            }
            break;
        case PARSER_FINISH:
            break;
        case PARSER_UNKNOWN:
            break;
        default:
            break;
    }
}

static void
squale_xml_end_element (SqualeXML *xml, const xmlChar *xml_name)
{
    gchar * name = (gchar *) xml_name;

    g_return_if_fail (xml != NULL);

    switch (xml->state) {
        case PARSER_SQUALE:
            if (strcmp(name, "squale") != 0)
                g_warning("should find </squale> here.  Found </%s>", name);
            xml->state = PARSER_FINISH;
            break;
        case PARSER_SETTINGS:
            if (strcmp(name, "settings") != 0)
                g_warning("should find </settings> here.  Found </%s>", name);
            xml->state = PARSER_SQUALE;
            break;
        case PARSER_SETTING:
            if (strcmp(name, "setting") != 0)
                g_warning("should find </setting> here.  Found </%s>", name);
            xml->state = PARSER_SETTINGS;
            break;
        case PARSER_CONNECTIONS:
            if (strcmp(name, "connections") != 0)
                g_warning("should find </connections> here.  Found </%s>", name);
            xml->state = PARSER_SQUALE;
            break;
        case PARSER_CONNECTION:
            if (strcmp(name, "connection") != 0)
                g_warning("should find </connection> here.  Found </%s>", name);
            else {
                /* We clean our properties hash table for that connection */
                g_hash_table_foreach_remove (xml->properties,
                                             squale_xml_dummy_true_func, xml);

                if (SQUALE_IS_JOBLIST (xml->joblist)) {
                    if (xml->joblist_backend == SQUALE_BACKEND_UNSET) {
                        /* We created a joblist for an unsupported backend, destroy it */
                        g_object_unref (xml->joblist);
                    }
                    else {
                        /* We insert that joblist in our joblist list :-) */
                        xml->joblists = g_list_prepend (xml->joblists, xml->joblist);
                    }
                }

                xml->joblist_backend = SQUALE_BACKEND_UNSET;
                xml->joblist = NULL;
            }
            xml->state = PARSER_CONNECTIONS;
            break;
        case PARSER_WORKER:
            if (strcmp(name, "worker") != 0)
                g_warning("should find </worker> here.  Found </%s>", name);
            else {
                /* We finished a worker block, inserting that worker in the list */
                if (SQUALE_IS_WORKER (xml->worker))
                    xml->workers = g_list_prepend (xml->workers, xml->worker);
                xml->worker = NULL;
            }
            xml->state = PARSER_CONNECTION;
            break;
        case PARSER_START:
        case PARSER_FINISH:
        case PARSER_UNKNOWN:
            g_warning ("squale_xml_end_element : D'oh that should not happen !");
            break;
        default:
            break;
    }
}

static void
squale_xml_characters (SqualeXML *xml, const xmlChar *chars, int len)
{
    g_return_if_fail (xml != NULL);

    switch (xml->state)
    {
        default :
            /* don't care about content in any other states */
            break;
    }
}

static xmlEntityPtr
squale_xml_get_entity (SqualeXML *xml, const xmlChar *name)
{
    return xmlGetPredefinedEntity (name);
}

static void
squale_xml_warning (SqualeXML *xml, const char *msg, ...)
{
    va_list args;

    va_start (args, msg);
    g_logv ("XML", G_LOG_LEVEL_WARNING, msg, args);
    va_end (args);
}

static void
squale_xml_error (SqualeXML *xml, const char *msg, ...)
{
    va_list args;

    va_start (args, msg);
    g_logv ("XML", G_LOG_LEVEL_CRITICAL, msg, args);
    va_end (args);
}

static void
squale_xml_fatal_error (SqualeXML *xml, const char *msg, ...)
{
    va_list args;

    va_start (args, msg);
    g_logv ("XML", G_LOG_LEVEL_ERROR, msg, args);
    va_end (args);
}

static xmlSAXHandler squale_parser = {
        0,                                                 /* internalSubset */
        0,                                                 /* isStandalone */
        0,                                                 /* hasInternalSubset */
        0,                                                 /* hasExternalSubset */
        0,                                                 /* resolveEntity */
        (getEntitySAXFunc) squale_xml_get_entity,          /* getEntity */
        0,                                                 /* entityDecl */
        0,                                                 /* notationDecl */
        0,                                                 /* attributeDecl */
        0,                                                 /* elementDecl */
        0,                                                 /* unparsedEntityDecl */
        0,                                                 /* setDocumentLocator */
        (startDocumentSAXFunc) squale_xml_start_document,  /* startDocument */
        (endDocumentSAXFunc) squale_xml_end_document,      /* endDocument */
        (startElementSAXFunc) squale_xml_start_element,    /* startElement */
        (endElementSAXFunc) squale_xml_end_element,        /* endElement */
        0,                                                 /* reference */
        (charactersSAXFunc) squale_xml_characters,         /* characters */
        0,                                                 /* ignorableWhitespace */
        0,                                                 /* processingInstruction */
        (commentSAXFunc) 0,                                /* comment */
        (warningSAXFunc) squale_xml_warning,               /* warning */
        (errorSAXFunc) squale_xml_error,                   /* error */
        (fatalErrorSAXFunc) squale_xml_fatal_error,        /* fatalError */
        0,                                                 /* getParameterEntity */
        0,                                                 /* cdataBlock */
        0,                                                 /* externalSubset */
        0,                                                 /* initialized */
        0,                                                 /* private */
        0,                                                 /* startElementNs */
        0,                                                 /* endElementNs */
        0                                                  /* serror */
};

SqualeXML *
squale_xml_new (const char *filename, Squale *squale)
{
    SqualeXML *xml = NULL;

    g_return_val_if_fail (filename != NULL, NULL);

    if (!g_file_test (filename, G_FILE_TEST_IS_REGULAR)) {
        g_error (_("SQuaLe XML parser could not find xml file '%s'"),
                 filename);
        return NULL;
    }

    xml = g_new0 (SqualeXML, 1);

    xml->squale = squale;

    if (xmlSAXUserParseFile (&squale_parser, xml, filename) < 0) {
        g_error (_("SQuaLe XML parser reported that document was not well " \
        "formed"));
        return NULL;
    }

    g_message (_("Loaded XML configuration file '%s'"), filename);

    return xml;
}

void
squale_xml_destroy (SqualeXML *xml)
{
    GList *workers = NULL;
    GList *joblists = NULL;

    workers = xml->workers;

    while (workers) {
        SqualeWorker *worker = SQUALE_WORKER (workers->data);
        g_object_unref (worker);
        workers = g_list_next (workers);
    }

    g_list_free (xml->workers);

    xml->workers = NULL;

    joblists = xml->joblists;

    while (joblists) {
        SqualeJobList *joblist = SQUALE_JOBLIST (joblists->data);
        g_object_unref (joblist);
        joblists = g_list_next (joblists);
    }

    g_list_free (xml->joblists);

    xml->joblists = NULL;
}
