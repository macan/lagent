/**
 * Copyright (c) 2008 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * lagent(u) is the LAGENT(user space)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <getopt.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <ctype.h>
#include <time.h>
#include <stdarg.h>

#define GCTRL_RELEASE_VERSION   "0.0.1"
#define GCTRL_EPOLL_QUEUE_LEN   1024
struct _client_
{
    char *host;
    char *pgm;
    pid_t pid, sid;
    int rank;
};
typedef struct _client_ client_t;

struct _job_
{
    char *jobid;
    char *jobalias;
    char *username;
    int ranks;
    client_t *clients;
    struct _job_ *next;
};
typedef struct _job_ job_t;

#define NEW_JOB(job) do {                       \
        job = malloc(sizeof(job_t));            \
    } while (0)
#define INIT_JOB(job) do {                      \
        memset(job, 0, sizeof(job_t));          \
    } while (0)

#define JOB_TAIL(job) ({                        \
        job_t *__tmp = job;                     \
        while (__tmp && __tmp->next) {          \
            __tmp = __tmp->next;                \
        }                                       \
        __tmp;})

#define JOB_EMPTY(job) (job == NULL)
#define JOB_APPEND(new, job) do {                                       \
        if (job->next) {                                                \
            fprintf(stderr, "job->next is not NULL\n");                 \
            break;                                                      \
        }                                                               \
        job->next = new;                                                \
        new->next = NULL;                                               \
    } while (0)
#define JOB_DELETE(job, prev, next) do {                \
        if (!prev) {                                    \
            printf(stderr, "prev is NULL\n");           \
            break;                                      \
        }                                               \
        if (prev->next != job) {                        \
            printf(stderr, "prev->next is not job\n");  \
            break;                                      \
        }                                               \
        if (job->next != next) {                        \
            printf(stderr, "job->next is not next\n");  \
            break;                                      \
        }                                               \
        prev->next = next;                              \
    } while (0)
#define JOB_FREE(job) do {                      \
        int i;                                  \
        if (job->jobid)                         \
            free(job->jobid);                   \
        if (job->jobalias)                      \
            free(job->jobalias);                \
        if (job->username)                      \
            free(job->username);                \
        if (job->clients) {                     \
            for (i = 0; i < job->ranks; i++) {  \
                if (job->clients[i].host)       \
                    free(job->clients[i].host); \
                if (job->clients[i].pgm)        \
                    free(job->clients[i].pgm);  \
            }                                   \
            free(job->clients);                 \
        }                                       \
    } while (0)

#define JOB_MATCH(job, job_list) ({             \
        job_t *j = job_list;                    \
        int res = 0;                            \
        while (j) {                             \
            if (!strcmp(j->jobid, job->jobid)) {    \
                res = 1;                            \
                break;                              \
            }                                       \
            j = j->next;                            \
        }                                           \
        res ? j : NULL;                             \
        })

#define JOB_DUMP(job) do {                           \
        job_t *j = job;                              \
        while (j) {                                  \
            printf("jobid %s\n", j->jobid);          \
            printf("jobalais %s\n", j->jobalias);    \
            printf("username %s\n", j->username);    \
            printf("ranks %d\n", j->ranks);          \
            int i;                                   \
            for (i = 0; i < j->ranks && j->clients; i++) {  \
                printf("\thost %s\n",                \
                       j->clients[i].host);          \
                printf("\tpid %d\n",                 \
                       j->clients[i].pid);           \
                printf("\tsid %d\n",                 \
                       j->clients[i].sid);           \
                printf("\trank %d\n",                \
                       j->clients[i].rank);          \
                printf("\tpgm %s\n",                 \
                       j->clients[i].pgm);           \
            }                                        \
            j = j->next;                             \
        }                                            \
    } while (0)

/**
 * Every lister must supply the corresponding lister operations
 *
 * is_data_valid: check the data
 * scan_jobs: scan the jobs and append them to the job list
 * scan_clients: scan the clients and insert them to the job->clients
 */
struct lister_ops
{
    int (*is_data_valid)(char *data);
    int (*scan_jobs)(char *data, job_t *job_tail);
    int (*scan_clients)(char *data, job_t *job_head);
};

struct lister
{
    char *name;
    struct lister_ops ops;
    struct lister *next;
};

/**
 * Token operations
 */
#define TOKEN_START(p, blanks) do {             \
        p = strtok(p, blanks);                  \
    } while (0)

#define TOKEN_NEXT(p, blanks) do {              \
        p = strtok(NULL, blanks);               \
    } while (0)

#define TOKEN_MATCH(p, target) (!strncmp(p, target, strlen(target)))
        
struct nvlist
{
    char *name;
    char *value;
    struct nvlist *next;
};

struct gshell_internal
{
    int doecho;
    int display_clients;
    int sock;
    struct sockaddr_in addr;
    char *node;                 /* root node */
    char *port;                 /* root UDP port */
    struct nvlist *nv;
};

#define NVLIST_ALLOC() ({                       \
            struct nvlist *nv;                  \
            nv = malloc(sizeof(struct nvlist)); \
            if (!nv)                            \
                return;                         \
            nv;                                 \
})

#define NVLIST_FREE(nv) do {                    \
        if (nv) {                               \
            free(nv->name);                     \
            free(nv->value);                    \
            free(nv);                           \
        }                                       \
    } while (0)
