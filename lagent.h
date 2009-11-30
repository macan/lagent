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
#include <netdb.h>

#define LAGENT_RELEASE_VERSION          "0.4.0"
#define VERIFIER                        "sha1sum"

enum {
    phy_topo_flat = 1,
    phy_topo_interim,
    phy_topo_tree
};

typedef enum {
    FW_FLAT = 0,
    FW_TREE,
    FW_MULTICAST
} fw_type_t;

typedef struct 
{
    struct sockaddr_in addr;
    char *name;
    union
    {
        int cbi[4];             /* 0 is used for TOPO_INIT, 1 is used for
                                 * CHKPT, 2 is used for RSTRT, 3 is used for
                                 * ? */
        char cbs[32];
    };
} fw_addr_t;

/**
 * TYPE(fw_type_t):
 *      FW_FLAT with a inet nodes list
 *      FW_TREE with a subtree nodes list
 *      FW_MULTICAST with a group address
 *
 * These info. should be saved to the LOG
 */

#define INIT_MSG        0
#define TOPO_MSG        1
#define HB_MSG          2
#define CHKPT_MSG       3
#define RSTRT_MSG       4
#define CHKPT_REPLY_MSG 5
#define RSTRT_REPLY_MSG 6
#define ACK_MSG         100

#define ANY_MSG         -1

typedef struct
{
    int msg_type, len;
    int fw_addr_num;
    fw_type_t fw_type;
    fw_addr_t fw_addr[0];
} init_msg_t;

/**
 * For ACK_MSG, the cmd could be:
 */

#define MSG_CMD_INIT    0
#define MSG_CMD_TOPO    1
#define MSG_CMD_HB      2
#define MSG_CMD_CHKPT   3
#define MSG_CMD_RSTRT   4
#define MSG_CMD_CHKPT_REPLY     5
#define MSG_CMD_RSTRT_REPLY     6

/**
 * For CHKPT_MSG, the cmd could be: nothing or jobid or version number?
 */

typedef struct
{
    int msg_type, len;
    int cmd, bref;              /* cmd in CHKPT_MSG means the unique CSN ?;
                                 * bref only used for ack ? */
    char data[0];
} cmd_msg_t;

typedef struct
{
    int msg_type, len;
} msg_header_t;

/**
 * Note that the format of the configration file is:
 * #root
 * node_name
 * #block i
 * node_name
 * node_name
 * ...
 * #block i+1
 * ...
 */

#define MAX_LINE_LENGTH         256
#define BLOCK_ITEMS_DEFAULT     16

typedef struct
{
    char node[MAX_LINE_LENGTH];
} baddr_t;

typedef struct _block_
{
    int bn;                     /* block number */
    int alloc_num, elem_num;
    baddr_t *elem;
    struct _block_ * next;
} block_t;

typedef enum {
    PARSER_INIT,
    PARSER_EXPECT_ROOT,
    PARSER_EXPECT_BLOCK,
    PARSER_READING_BLOCK
} parser_state_t;

enum {
    PARSER_OK = 0,
    PARSER_NEED_RETRY,
    PARSER_CONTINUE,
    PARSER_FAILED = 1000
};

typedef struct tree_node
{
    fw_addr_t *addr;            /* address info. */
    struct tree_node *children; /* children list */
    int ch_num;                 /* children number */
} tree_node_t;

struct topo_flat
{
    fw_addr_t *elem;
    int elem_num;
};

struct topo_tree
{
    int elem_num;
    tree_node_t *node;
};

typedef struct
{
    int type;           /* phy_topo_tree/phy_topo_flat */
    union 
    {
        struct topo_flat flat;
        struct topo_tree tree;
        block_t *bhead;
    };
} phy_topo_t;

extern int parse_topo(FILE *file, phy_topo_t *topo);
extern int global_debug;
extern int global_delay;
extern int global_quiet;
extern int deep_quiet;
extern int pln;
extern int has_log;
extern FILE *logfp;
extern int lsock;               /* listening socket */
extern unsigned short lport;
extern int epfd;
extern int isroot;
extern phy_topo_t phy_topo;
extern char *root;
extern char *pattern;
extern FILE *ofilp;
extern char *ofile;
extern char *self;
extern char *cmd;
extern char *gbuf;

extern int chkpt_unix, rstrt_unix;

#define LAGENT_DEFAULT_PORT     4000
#define LAGENT_PID_FILE         "/tmp/.lagent.pid"
#define LAGENT_MAX_MSG_LEN      (1024 * 3)
#define LAGENT_EPOLL_QUEUE_LEN  4096
#define LAGENT_EPOLL_CHECK      64

#define LAGENT_DEBUG(file, fmt, args...) do {    \
        if (global_debug) {                      \
            fprintf(file, fmt, ##args);          \
        }                                        \
    } while (0)

#define ELAGENTCHILDEXIT        1000

#define LAGENT_SET(buf, fmt, args...) ({        \
            int len;                            \
            sprintf(buf, fmt, ##args);          \
            len = strlen(buf);                  \
            len;})

#define LAGENT_OUT(file, fmt, args...) do {         \
        if (!((global_quiet && file == stdout) || \
              (deep_quiet && file == stderr))) {    \
            fprintf(file, fmt, ##args);             \
            fflush(file);                           \
        }                                           \
    } while (0)

#define LAGENT_ERR(file, fmt, args...) do {                     \
        if (!(deep_quiet && file == stderr)) {                  \
            fprintf(file, "[%s] " fmt, __FUNCTION__, ##args);   \
        }                                                       \
    } while (0)

#define LAGENT_LOG(file, fmt, args...) do {                     \
        if (has_log) {                                          \
            fprintf(file, "[%s] " fmt, __FUNCTION__, ##args);   \
            fflush(file);                                       \
        }                                                       \
    } while (0)

typedef int (*handle_io_callback)(cmd_msg_t *msg, int type, int cmd, 
                                  struct sockaddr *from);
extern int match_root(parser_state_t *ps, char *line);
extern int get_root(parser_state_t *ps, char *line, char *out);
extern int get_node(parser_state_t *ps, char *line, char *out);
extern int match_block(parser_state_t *ps, char *line, block_t **block);
extern int parse_topo_flat(FILE *file, phy_topo_t *topo);
extern int parse_topo_tree(FILE *file, phy_topo_t *topo);
extern void lagent_poll_close(int epfd);
extern int lagent_bcast(phy_topo_t *topo, cmd_msg_t *msg);
extern int lagent_gather(phy_topo_t *topo, int type, int cmd, time_t expire);
extern int lagent_poll_wait(struct epoll_event *ev, int timeout);
extern int lagent_poll_create(void);
extern int lagent_poll_add(int fd);
extern int lagent_poll_del(int fd);
extern int lagent_recv(int fd, void *msg, struct sockaddr *from, socklen_t *len);
extern int handle_io_input(cmd_msg_t *msg, int type, int cmd, struct sockaddr *from,
                           handle_io_callback *cb);
extern int start_listening(int protocol, unsigned short port);
extern int lagent_rsend(int fd, void *msg, fw_addr_t *addr, unsigned short port);
extern phy_topo_t *select_children(int isroot, char *root, phy_topo_t *phy_topo);
extern int get_ack_cmd(int type);
extern int check_chkpt(cmd_msg_t *msg, struct sockaddr *from);
extern cmd_msg_t *dup_msg(cmd_msg_t *from);
extern int lagent_rbcast(int fd, phy_topo_t *topo, cmd_msg_t *msg);
extern int get_elem_num(phy_topo_t *topo);
extern int parse_chkpt_item(char **node, int *pid, int *is_local);
extern int parse_message_unix(char *str, int *pid, int *state, int *bref);
extern int lagent_unix_read(int fd);
extern void lagent_wait_children(void);
extern int is_my_child(char *node, phy_topo_t *topo, fw_addr_t **item);
extern void search_pb_list_for_local(int bref);
extern void inet_lookup_name(struct sockaddr *sa, char *name);
extern int lagent_checking_excludes(fw_addr_t *fw);
extern void dump_nodes_from_topo_flat(FILE *file, phy_topo_t *topo);

struct _persistent_block_;
typedef struct _persistent_block_ persistent_block_t;
typedef void (*pb_callback)(persistent_block_t *pb, int isroot, char *root, 
                            phy_topo_t *phy_topo, cmd_msg_t *msg);
extern int bref;
typedef struct 
{
    fw_addr_t *addr;
    int done, alloc, free;
} comp_t;

struct _persistent_block_
{
    time_t expire;              /* event expire */
    struct _persistent_block_ *next;
    struct _persistent_block_ *free;
    int bref;                   /* this is a global bref number */
    int flags;
#define PB_DONE         0x01
#define PB_ONE_TARGET   0x02
#define PB_MUL_TARGET   0x04
#define PB_HB           0x08
#define PB_IGNORE       0x80

    int retry, faults;
    int target, current;
    pb_callback func;
    void *private;
#define DEFAULT_COMP_ITEM       10
    comp_t completes;
    void *data;

    int match;                  /* this field be set to the unique request
                                 * number, greater than 0 */
    /* args */
    int arg0;
    char *arg1;
    phy_topo_t *arg2;
    cmd_msg_t *arg3;
};

extern persistent_block_t *pbl;

persistent_block_t *alloc_pb(void);
extern void insert_pb(persistent_block_t *pb);
extern void remove_pb(persistent_block_t *pb);
extern void search_pb_list(cmd_msg_t *msg, struct sockaddr *from);

extern void chkpt_cb(persistent_block_t *pb, int isroot, char *root, phy_topo_t *topo,
                     cmd_msg_t *msg);
extern void default_resend_cb(persistent_block_t *pb, int isroot, char *root, 
                              phy_topo_t *topo, cmd_msg_t *msg);
extern int do_local_chkpt(phy_topo_t *topo, cmd_msg_t *msg, persistent_block_t *pb);
extern void stdin_handler();

#define LAGENT_CHKPT_UNIX_PATH          "/tmp/lagent.chkpt"
#define LAGENT_RSTRT_UNIX_PATH          "/tmp/lagent.rstrt"

/* TEST BLOCKS */
void tb_cb(persistent_block_t *pb, int isroot, char *root, phy_topo_t *phy_topo, 
           cmd_msg_t *msg);

/* FOR launching */
struct io_redirections
{
    int pipe[2];                /* stdout & stderr */
    int pin[2];
    fw_addr_t *fw;
    pid_t pid;
    int flags;
#define IO_RED_PATTERN_MATCH    0x01
};

/* Excludes List */
struct excludes
{
    fw_addr_t fw;
    struct excludes *next;
};

#define PING_HEADER "Ping Report -- \033[0;40;31mBad Nodes Should Be REPAIRED\033[0m\n"

typedef void (*ce_cb_t)(void *a, char *b, int n);

extern ce_cb_t comb_emit_cb;
extern void comb(void *a, int n, int m, int index);
#define COMB_ELEM_SIZE  64
extern void lagent_message(FILE* stream, char *string, fw_addr_t *fw);
extern int lagent_start_all(phy_topo_t *topo, struct io_redirections **child, 
                            int *total,int *done);
extern void lagent_start_poll(int total, struct io_redirections *child);
extern void lagent_check_child(struct io_redirections *child, int *iter, int total);
extern void lagent_verify_child(struct io_redirections *child, int total);

extern char *color[];
extern int no_color;
extern int verbose;
extern int ping_report;
extern int global_count;
extern int repeater_mode;
extern int file_transfer_mode;
extern int file_transfer_count;
extern int noinput;
extern char *set_user;

/* Lagent Start Filters */
struct filter
{
    char *pattern;
    int flag;
#define FILTER_MATCH            0x00000001
#define FILTER_EXCLUDE          0x00000002
    struct filter *next;
};
extern int lagent_filter(struct filter *flsit);
extern struct filter *global_filter;

struct pacemaker
{
    int start, end, stride;
};

struct pattern_maker
{
    char *prefix, *suffix, *mid;
    struct pacemaker pm[2];
#define PATTERN_AD      0       /* prefix + pm[0] */
#define PATTERN_ADA     1       /* prefix + pm[0] + mid */
#define PATTERN_ADAD    2       /* prefix + pm[0] + mid + pm[1] */
#define PATTERN_ADADA   3       /* prefix + pm[0] + mid + pm[1] + suffix */
#define PATTERN_DA      4       /* pm[0] + mid */
#define PATTERN_DAD     5       /* pm[0] + mid + pm[1] */
#define PATTERN_DADA    6       /* pm[0] + mid + pm[1] + suffix */
#define PATTERN_A       7       /* prefix */
#define PATTERN_D       8       /* pm[0] */
    struct pattern_maker *next;
};
