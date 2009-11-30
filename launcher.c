/**
 * Copyright (c) 2008 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * launcher: include all the launch helpers.
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

#include "lagent.h"

int last_fd = -1;
pid_t last_pid = -1;
static int __total = 0;
static struct io_redirections *__child = NULL;

char *color[] = {
    "\033[0;40;31m",
    "\033[0;40;32m",
    "\033[0;40;33m",
    "\033[0;40;34m",
    "\033[0;40;35m",
    "\033[0;40;36m",
    "\033[0;40;37m",
    "\033[0m",
};

char *ncolor[] = {
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
};

void abort_handler(int signr)
{
    int pid = getpid();
    printf("pis %d SHIT\n", pid);
}

void lagent_verify_child(struct io_redirections *child, int total)
{
    int i, j;

    if (!pattern)
        return;
    if (verbose)
        LAGENT_OUT(stderr, "> Verify the pattern '%s', unmatched list to (%s):\n", 
                   pattern, (ofilp ? ofile : "stderr"));
    for (j = 0; j < global_count; j++) {
        for (i = 0; i < total; i++) {
            if (!(child[i + total * j].flags & IO_RED_PATTERN_MATCH)) {
                char node[NI_MAXHOST];
                memset(node, 0, sizeof(node));
                inet_lookup_name((struct sockaddr *)(&(child[i + j * total].fw->addr)),
                                 node);
                if (verbose)
                    LAGENT_OUT((ofilp ? ofilp : stderr), "%s\n", node);
            }
        }
    }
}

void lagent_match_pattern(struct io_redirections *child, char *buf)
{
    char *p, *head;
    if (!pattern)
        return;
    if (!strlen(buf))
        return;
    p = malloc(strlen(buf) + 1);
    if (!p)
        return;
    memset(p, 0, strlen(buf) + 1);
    strncpy(p, buf, strlen(buf));

    head = p;
    p = strtok(p, "\n");
    if (!p)
        goto out;
    do {
        if (strstr(p, pattern) != NULL) {
            char node[NI_MAXHOST];
            child->flags |= IO_RED_PATTERN_MATCH;
            memset(node, 0, sizeof(node));
            inet_lookup_name((struct sockaddr *)(&child->fw->addr), node);
            LAGENT_DEBUG(stderr, "node %s match '%s'\n", node, pattern);
        }
    } while ((p = strtok(NULL, "\n")));
out:
    free(head);
}

void lagent_read_more(struct io_redirections *child)
{
    char buf[256 + 100], *p;
    char node[NI_MAXHOST];
    int br, fd = child->pipe[0];
    
    memset(buf, 0, sizeof(buf));
    if (last_fd != fd || last_pid != child->pid) {
        memset(node, 0, sizeof(node));
        inet_lookup_name((struct sockaddr *)&((child->fw)->addr), node);

        sprintf(buf, "%s%s%s\n",
                (no_color ? ncolor[fd % 7] : color[fd % 7]),
                (node[0] ? node : inet_ntoa((child->fw)->addr.sin_addr)),
                (no_color ? ncolor[7] : color[7]));
        if (ping_report) {
            ping_report = strlen(buf);
        }
        p = buf + strlen(buf);
    } else 
        p = buf;
    do {
        br = read(fd, p, 255);
        if (br) {
            if (!ping_report) {
                lagent_match_pattern(child, buf);
                LAGENT_OUT(stdout, "%s", buf);
            } else {
                if (!verbose)
                    buf[ping_report] = 0;
                LAGENT_OUT((ofilp ? ofilp : stdout), "%s", buf);
            }
            last_fd = fd;
            last_pid = child->pid;
        }
    } while (br == 255 && (p = buf) && memset(p, 0, sizeof(buf)));
}

void lagent_check_child(struct io_redirections *child, int *iter, int total)
{
    int status, i, j, res;

    for (j = 0; j < global_count; j++) {
        for (i = 0; i < total; i++) {
            if (child[i + total * j].pid) {
                res = waitpid(child[i + total * j].pid, &status, WNOHANG);
                
                if (res == child[i + total * j].pid) {
                    child[i + total * j].pid = 0;
                    
                    lagent_read_more(&child[i + total * j]);
                    if (lagent_poll_del(child[i + total * j].pipe[0]) < 0) {
                        LAGENT_ERR(stderr, "epoll del faild with %d\n", errno);
                    }
                    close(child[i + total * j].pipe[0]);
                    child[i + total * j].pipe[0] = -1;
                    if (repeater_mode) {
                        close(child[i + total * j].pin[1]);
                        child[i + total * j].pin[1] = -1;
                    }
                    *iter = *iter - 1;
                    if (file_transfer_mode)
                        lagent_message(stderr, "--> Transfer done on node", 
                                       child[i + total * j].fw);
                } else if (res < 0) {
                    child[i + total * j].pid = 0;
                    *iter = *iter - 1;
                    LAGENT_ERR(stderr, "lagent wait child %d failed\n", 
                               child[i + total * j].pid);
                }
            }
        }
    }
}

void lagent_start_poll(int total, struct io_redirections *child)
{
    char buf[256 + 100];
    char node[NI_MAXHOST];
    struct epoll_event ev[LAGENT_EPOLL_CHECK];
    int k, i, j, br, nfds;
    
    nfds = lagent_poll_wait(ev, 50);
    if (nfds < 0 && errno == EINTR) {
        nfds = 0;
    } else if (nfds < 0) {
        LAGENT_ERR(stderr, "poll wait failed with %d\n", errno);
        exit(errno);
    }
    for (k = 0; k < nfds; k++) {
        int fd = ev[k].data.fd;
        if (!fd) {
            /* stdin */
            stdin_handler();
            continue;
        }
        for (j = 0; j < global_count; j++) {
            for (i = 0; i < total; i++) {
                char *p;
                if (fd == child[i + j * total].pipe[0]) {
                    memset(buf, 0, sizeof(buf));
                    if (last_fd != fd || last_pid != child[i + j * total].pid) {
                        memset(node, 0, sizeof(node));
                        inet_lookup_name((struct sockaddr *)
                                         &((child[i + j * total].fw)->addr), node);
                        sprintf(buf, "%s%s%s\n",
                                (no_color ? ncolor[i % 7] : color[i % 7]),
                                (node[0] ? node :
                                 inet_ntoa((child[i + j * total].fw)->addr.sin_addr)),
                                (no_color ? ncolor[7] : color[7]));
                        if (ping_report) {
                            ping_report = strlen(buf);
                        }
                        p = buf + strlen(buf);
                    } else 
                        p = buf;
                    do {
                        br = read(fd, p, 255);
                        if (br) {
                            if (!ping_report) {
                                lagent_match_pattern(&child[i + j * total], buf);
                                LAGENT_OUT(stdout, "%s", buf);
                            } else {
                                /* it is ping report, whether should we be verbose? */
                                if (!verbose)
                                    buf[ping_report] = 0;
                                LAGENT_OUT((ofilp ? ofilp : stdout), "%s", buf);
                            }
                            last_fd = fd;
                            last_pid = child[i + j * total].pid;
                        }
                    } while (br == 255 && (p = buf) && memset(p, 0, sizeof(buf)));
                }
            }
        }
    }
}

int lagent_topo_from_pattern(phy_topo_t *topo, struct pattern_maker *pm)
{
    return 0;
}

int lagent_filter(struct filter *flsit)
{
    return 0;
}

void lagent_emit_children(char *line, ssize_t bl)
{
    int i, j;

    if (!line)
        return;
    if (global_debug)
        printf("> [STDIN]%s", line);
    for (j = 0; j < global_count; j++) {
        for (i = 0; i < __total; i++) {
            if (__child[i + __total * j].pin[1] > 0) {
                write(__child[i + __total * j].pin[1], line, bl);
            }
        }
    }
}

void lagent_close_children()
{
    int i, j;

    for (j = 0; j < global_count; j++) {
        for (i = 0; i < __total; i++) {
            if (__child[i + __total * j].pin[1] > 0) {
                close(__child[i + __total * j].pin[1]);
                __child[i + __total * j].pin[1] = -1;
            }
        }
    }
}

void stdin_handler()
{
    char line[256];
    ssize_t br;

    memset(line, 0, sizeof(line));
    while ((br = read(0, line, 256)) >= 0) {
        if (!br) {
            lagent_close_children();
            break;
        } else
            lagent_emit_children(line, br);
    }
}

int lagent_input_repeater(int fd)
{
    if (lagent_poll_add(0) == -1) {
        LAGENT_ERR(stderr, "poll add stdin failed with %d\n", errno);
        return EIO;
    }
    return 0;
}

int lagent_start_all(phy_topo_t *topo, struct io_redirections **child, int *total,
                     int *done)
{
    int res = 0;
    int i, j;

    *child = NULL;
    /* FIXME: set the listening ports ? */

    switch (topo->type) {
    case phy_topo_flat:
    {
        struct topo_flat *flat = &topo->flat;
        if (flat->elem_num <= 0)
            break;
        *total = flat->elem_num;
        __total = *total;
        *done = flat->elem_num * global_count;
        LAGENT_OUT(stderr, "> Execute on %d nodes(%d per node) ...\n", *total, 
                   global_count);
        *child = malloc(sizeof(struct io_redirections) * flat->elem_num * global_count);
        if (!(*child)) {
            res = ENOMEM;
            break;
        }
        __child = *child;
        memset(*child, 0, 
               sizeof(struct io_redirections) * flat->elem_num * global_count);
        if ((epfd = epoll_create((*total) * global_count + 1)) < 0) {
            perror("epoll_create");
            exit(errno);
        }
        if (repeater_mode)
            lagent_input_repeater(epfd);
        if (ping_report) {
            LAGENT_OUT(stderr, "%s", PING_HEADER);
            ping_report = 2;
        }
        for (j = 0; j < global_count; j++) {
        for (i = 0; i < flat->elem_num; i++) {
            /* slow down now */
            if (file_transfer_mode) {
                while ((flat->elem_num * j + i - 
                        (flat->elem_num * global_count - *done)) /* pending */
                       >= file_transfer_count) {
                    /* waiting for any pending transfer */
                    lagent_check_child(*child, done, *total);
                    lagent_start_poll(*total, *child);
                }
            } else if ((i && i % 100 == 0) || global_delay) {
                lagent_check_child(*child, done, *total);
                lagent_start_poll(*total, *child);
            }
            /* BEGIN stdin repeater */
            if (repeater_mode) {
                if (pipe((*child)[i + j * (*total)].pin) == -1) {
                    perror("stdin pipe");
                    exit(errno);
                }
            }
            /* END stdin repeater */
            if (pipe((*child)[i + j * (*total)].pipe) == -1) {
                perror("pipe");
                exit(errno);
            }
            if (lagent_poll_add((*child)[i + j * (*total)].pipe[0]) == -1) {
                LAGENT_ERR(stderr, "poll add failed with %d\n", errno);
                exit(errno);
            }
            (*child)[i + j * (*total)].fw = &flat->elem[i];
            /* global delay per fork */
            if ((i + j * global_count) && global_delay)
                sleep(global_delay);
            /* fork here */
            (*child)[i + j * (*total)].pid = fork();
            if ((*child)[i + j * (*total)].pid < 0) {
                res = errno;
                break;
            }
            if ((*child)[i + j * (*total)].pid == 0) {
                /* child process */
                /* do 'lagent -r root' */
                if (!cmd)
                    exit(0);
                char user_addr[64] = {0,};
                char *args[] = {"/usr/bin/ssh",
                                "-x",
                                noinput ? "-n" : "-x",
                                user_addr,
                                cmd,
                                (char *)0
                };
                if (set_user)
                    snprintf(user_addr, 63, "%s@%s", set_user, 
                             inet_ntoa(flat->elem[i].addr.sin_addr));
                else
                    snprintf(user_addr, 63, "%s", 
                             inet_ntoa(flat->elem[i].addr.sin_addr));
                close(1);
                close(2);
                close((*child)[i + j * (*total)].pipe[0]);
                dup2((*child)[i + j * (*total)].pipe[1], 1);
                dup2((*child)[i + j * (*total)].pipe[1], 2);
                if (repeater_mode) {
                    close(0);
                    close((*child)[i + j * (*total)].pin[1]);
                    dup2((*child)[i + j * (*total)].pin[0], 0);
                }
                                    
                signal(SIGINT, SIG_IGN);
                res = execv("/usr/bin/ssh", args);
                if (signal(SIGABRT, abort_handler) < 0) {
                    perror("signal");
                    exit(errno);
                }
                if (res == -1) {
                    LAGENT_ERR(stderr, "Execute '%s' failed\n", args[0]);
                } else if (WEXITSTATUS(res)) {
                    LAGENT_ERR(stderr, "Execute '%s' return %d\n", args[0], 
                               WEXITSTATUS(res));
                }
                printf("%s %s %s %s %s\n", args[0], args[1], args[2], args[3], args[4]);
                exit(-1);
                res = ELAGENTCHILDEXIT;
                break;
            } else {
                /* parent process */
                close((*child)[i + j * (*total)].pipe[1]);
                if (repeater_mode)
                    close((*child)[i + j * (*total)].pin[0]);
            }
        }
        }
        break;
    }
    case phy_topo_tree:
        break;
    case phy_topo_interim:
        LAGENT_ERR(stderr, "invalid physical topology type\n");
        res = EINVAL;
        goto out;
        break;
    default:
        LAGENT_ERR(stderr, "invalid physical topology type\n");
        res = EINVAL;
        goto out;
    }
out:
    return res;
}


