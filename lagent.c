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

/**
 * TODO:
 * 1. support parallel local mode, e.x. local multiple pings
 * 2. support regular expresions, e.x. lagent -d ie8[1-5]*
*/

#include "lagent.h"

int global_debug = 0;
int global_count = 1;
int global_delay = 0;           /* in seconds */
int global_quiet = 0;           /* quiet mode, no stdout */
int deep_quiet = 0;
int repeater_mode = 0;
int no_color = 0;               /* default in colorfull mode */
int pln = 0;
FILE *logfp = NULL;
int has_log = 0;
int isroot = 0;
char *root = NULL;
char *pattern = NULL;
int bref = 0;
persistent_block_t *pbl = NULL; /* pb list */
phy_topo_t phy_topo = {.type = 0};
char *self = NULL;
FILE *ofilp = NULL;
char *ofile = NULL;
int chkpt_unix = 0;
int rstrt_unix = 0;
char *cmd = NULL;
char *excludes = NULL;
char *excludes_file = NULL;
struct excludes *global_excludes = NULL;
int pretend = 0;
int verbose = 0;
int dumplist = 0;
int ping_report = 0;

#define FILE_TRANSFER_DEFAULT   10
int file_transfer_mode = 0;
int file_transfer_count = FILE_TRANSFER_DEFAULT;

int total = 0, done = 0;
struct io_redirections *child = NULL;

int comb_elem = 0;              /* check for combination generator */
int comb_out = 0;
int comb_idx = 0;
int comb_echo = 0;
unsigned long comb_n = 1L;
char *specify_nodes = NULL;
char *gbuf = NULL;
int noinput = 0;
char *set_user = NULL;
ce_cb_t comb_emit_cb = NULL;
struct filter *global_filter = NULL;

void emit_phy_config_header()
{
    printf("# Configration file of the physical layout\n"
           "\n"
           "# This is the tree root\n"
           "#!root\n"
           "# Set your own root here\n"
           "localhost\n"
           "\n"
           "# These are the blocks\n"
           "#!block 0\n"
           "\n");
}

void lagent_dump_excludes(FILE *stream)
{
    struct excludes *ex = global_excludes;
    char node[NI_MAXHOST];
    
    if (!global_excludes)
        return;
    LAGENT_ERR(stream, "Dump global excludes list:\n");
    while (ex) {
        inet_lookup_name((struct sockaddr *)(&ex->fw.addr), node);
        LAGENT_ERR(stream, "%s\n", node);
        ex = ex->next;
    }
}

int lagent_create_excludes()
{
    struct hostent *host;
    struct excludes *ex;
    
    /* this is the command line list */
    if (excludes) {
        char *p = excludes;
        p = strtok(excludes, ",;: \n");
        do {
            if (!p) {
                LAGENT_ERR(stderr, "try to match token failed\n");
                return EINVAL;
            }
            if (strcmp(p, "self") == 0) {
                /* exclude myself */
                char self_name[256];
                if (gethostname(self_name, 256)) {
                    LAGENT_ERR(stderr, "gethostname failed with %d\n", errno);
                    return errno;
                }
                host = gethostbyname(self_name);
                if (!host) {
                    LAGENT_ERR(stderr, "gethostbyname (%s) failed with %d\n",
                               self_name, errno);
                    return errno;
                }
            } else {
                /* normal case */
                host = gethostbyname(p);
                if (!host) {
                    LAGENT_ERR(stderr, "gethostbyname (%s) failed with %d\n",
                               p, errno);
                    return errno;
                }
            }
            if ((ex = malloc(sizeof(struct excludes))) == NULL) {
                LAGENT_ERR(stderr, "malloc failed with %d\n", errno);
                return errno;
            }
            ex->fw.addr.sin_family = AF_INET;
            ex->fw.addr.sin_addr.s_addr = *((unsigned long *)host->h_addr_list[0]);
            /* add to the tail of the global excludes list */
            ex->next = global_excludes;
            global_excludes = ex;
        } while (p = strtok(NULL, ",;: \n"), p);
    }
    /* this is the excludes file */
    if (excludes_file) {
        /* open the file firse */
        FILE *f = fopen(excludes_file, "r");
        char *line = NULL;
        size_t len;
        if (!f) {
            LAGENT_ERR(stderr, "open file %s failed with %d\n", excludes_file, errno);
            return errno;
        }
        pln = 0;
        while ((getline(&line, &len, f)) != -1) {
            line[(strlen(line) - 1) ? : 0] = 0;
            host = gethostbyname(line);
            if (!host) {
                LAGENT_ERR(stderr, "gethostbyname (%s) failed with %d at line %d\n",
                           line, errno, pln);
                return errno;
            }
            if ((ex = malloc(sizeof(struct excludes))) == NULL) {
                LAGENT_ERR(stderr, "malloc failed with %d\n", errno);
                return errno;
            }
            ex->fw.addr.sin_family = AF_INET;
            ex->fw.addr.sin_addr.s_addr = *((unsigned long *)host->h_addr_list[0]);
            /* add to the tail of the global excludes list */
            ex->next = global_excludes;
            global_excludes = ex;
            pln++;
        }
    }
    return 0;
}

int lagent_checking_excludes(fw_addr_t *fw)
{
    struct excludes *ex = global_excludes;

    while (ex) {
        if (fw->addr.sin_addr.s_addr == ex->fw.addr.sin_addr.s_addr) {
            return 1;
        }
        ex = ex->next;
    }
    return 0;
}

int lagent_init_unix_socket(void)
{
    int res = 0;
    struct sockaddr_un addr = {.sun_family = AF_UNIX,};
    
    chkpt_unix = socket(AF_UNIX, SOCK_DGRAM, AF_UNIX);
    if (chkpt_unix == -1) {
        LAGENT_LOG(logfp, "create unix socket failed, %d\n", errno);
        res = errno;
        goto out;
    }
    sprintf(addr.sun_path, "%s", LAGENT_CHKPT_UNIX_PATH);
    unlink(addr.sun_path);
    if (bind(chkpt_unix, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        LAGENT_LOG(logfp, "bind CHKPT unix socket failed, %d\n", errno);
        res = errno;
        goto out;
    }
    rstrt_unix = socket(AF_UNIX, SOCK_DGRAM, AF_UNIX);
    if (rstrt_unix == -1) {
        LAGENT_LOG(logfp, "create unix socket failed, %d\n", errno);
        res = errno;
        goto out;
    }
    sprintf(addr.sun_path, "%s", LAGENT_RSTRT_UNIX_PATH);
    unlink(addr.sun_path);
    if (bind(rstrt_unix, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        LAGENT_LOG(logfp, "bind RSTRT unix socket failed, %d\n", errno);
        res = errno;
        goto out;
    }
out:
    return res;
}

/*
 * Message format is "pid:3669;links:2;state:0;bref:90"
 */
int lagent_unix_read(int fd)
{
    int bi, pid, state, bref, res;
    char data[256] = {0,};

    if (fd == chkpt_unix) {
        bi = recv(fd, data, 256, 0);
        if (bi == -1) {
            LAGENT_LOG(logfp, "chkpt_unix recv failed, %d\n", errno);
            return -errno;
        }
        LAGENT_LOG(logfp, "From CHKPT_UNIX: %s\n", data);
        res = parse_message_unix(data, &pid, &state, &bref);
        if (res < 0) {
            LAGENT_LOG(logfp, "parse unix message failed\n");
            return -EINVAL;
        }
        /* FIXME: search the pb list and increase the pb->current */
        search_pb_list_for_local(bref);
    } else if (fd == rstrt_unix) {
        bi = recv(fd, data, 256, 0);
        if (bi == -1) {
            LAGENT_LOG(logfp, "rstrt_unix recv failed, %d\n", errno);
            return -errno;
        }
        LAGENT_LOG(logfp, "From RSTRT_UNIX: %s\n", data);
        res = parse_message_unix(data, &pid, &state, &bref);
        if (res < 0) {
            LAGENT_LOG(logfp, "parse unix message failed\n");
            return -EINVAL;
        }
        /* FIXME: search the pb list and increase the pb->current */
    } else {
        /* need to search this fd further */
        return 1;
    }
    return 0;
}

persistent_block_t *alloc_pb()
{
    persistent_block_t *p = malloc(sizeof(persistent_block_t));
    if (!p) {
        LAGENT_LOG(logfp, "malloc persistent_block_t failed, no free memory");
    }
    memset(p, 0, sizeof(persistent_block_t));
    p->bref = bref++;
    return p;
}

void insert_pb(persistent_block_t *pb)
{
    persistent_block_t *p = pbl, *tail = pbl;

    while (p != NULL && p->next != NULL) {
        p = p->next;
        tail = p;
    }
    if (tail) {
        tail->next = pb;
        pb->next = NULL;
    } else {
        pbl = pb;
    }
    LAGENT_LOG(logfp, "insert block %d\n", pb->bref);
}

void remove_pb(persistent_block_t *pb)
{
    persistent_block_t *p = pbl, *prev = pbl;

    if (p->bref == pb->bref) {
        pbl = p->next;
        return;
    }
    p = p->next;
    while (p != NULL) {
        if (p->bref == pb->bref) {
            prev->next = p->next;
            break;
        }
        prev = p;
        p = p->next;
    }
}

static void usage(FILE *stream)
{
    fprintf(stream,
            "LAGENT version %s build @ %s on %s\n"
            "Usage: lagent [-sgqCiavRN] [-f file] [-t topo] [-c \"CMD\"]\n"
            "              [-x node_list] [-e exclude_file] [-d node_set]\n"
            "              [-b #] [-o #] [-n #] [-T <#>] [-D #] [-P pattern]\n"
            "    or lagent [-r root_node]\n"
            "\n"
            "General options:\n"
            " -s, --start       start lagent on all the node.\n"
            " -g, --debug       debug mode.\n"
            " -?, --help        print this help message.\n"
            " -V, --version     print version information.\n"
            " -p, --port        set the local port.\n"
            " -C, --no-color    unset the colorful mode.\n"
            " -q, --quiet       keep quiet on STDOUT.\n"
            " -Q, --deep-quiet  keep quiet on STDERR.\n"
            " -O, --ofile       output to the file.\n"
            " -L, --list        dump the node list.\n"
            " -H, --header      dump the header of the phy config file.\n"
            "\n"
            "Options for configrations:\n"
            " -f, --file        physical configration file.\n"
            "\n"
            "Options for physical topology:\n"
            " -t, --topo        physical topology [flat | tree(not implemented)].\n"
            "\n"
            "Options for start:\n"
            " -c cmd_str, --command     execute the commands on nodes.\n"
            " -x node_str, --exclude    exclude the following nodes.\n"
            " -e file, --exfile         exclude the nodes in the file.\n"
            " -i, --ping                ping the nodes to report the bad nodes.\n"
            " -a, --pretend             pretend to execute the command.\n"
            " -A, --load                seeing the load of the target nodes.\n"
            " -v, --verbose             verbose mode.\n"
            " -d node_str, --nodes      specify the nodes.\n"
            " -b #, --comb              enable the combination node, # is select num.\n"
            " -o #, --comb-out          output the ith combination number.\n"
            " -n #, --count             specify the number of processes start on each node.\n"
            " -D #, --delay             set the delay between every fork.\n"
            " -T #, --file-transfer     file transfer mode, # is max parallel count.\n"
            " -P str, --pattern         pattern for matching the output.\n"
            "                   [NOTE THAT] if it occurs 'stack smashing' error, \n"
            "                   please disable this option.\n"
            " -R, --repeater            repeater mode, send the STDIN to all the hosts.\n"
            "                   [NOTE THAT] this repeater will not repeat STDIN to\n"
            "                   the host which can't recv input at the moment.\n"
            " -N, --noinput             redirect host STDIN to /dev/null.\n"
            "\n"
            "Examples:\n"
            "0. lagent -r root_node\n"
            "   normal usage, start lagent daemon on this node with root node set to\n"
            "   root_node.\n"
            "1. lagent -sc \"date\"\n"
            "   execute 'date' on all the hosts.\n"
            "2. lagent -si\n"
            "   ping all the hosts and print the report.\n"
            "3. lagent -sc \"scp node1:file1 local_path\" -T10 -x self\n"
            "   copy file1 from node1 on each host exclude\n"
            "   self with MAX parallel factor to 10.\n"
            "4. lagent -sc \"date\" -D1 -n 5\n"
            "   execute 'date' 5 times on all the hosts with fork delay 1s.\n"
            "5. lagent -sc \"cat > file\" -R\n"
            "   cat the stdin to all the hosts to the file.\n"
            "6. lagent -sc \"ls\" -P \"XYZ\" -v\n"
            "   execute 'ls' on all the hosts and match the \n"
            "   pattern XYZ, report the unmatched.\n"
            "7. echo hello,world | lagent -sRc \"cat > file\"\n"
            "   repeat the STDIN to all the host, this will work well\n"
            "   with other shell commands.\n"
            "8. lagent -sb 4 -o 2\n"
            "   output the the 3rd(2+1) combination tuple of C(nodes,4).\n"
            "9. lagent -sA\n"
            "   seeing the load of the target nodes.\n"
            "Notes:\n"
            "1. How to use LAGENT with other BASH tools?\n"
            "1.1 use AWK in LAGENT cmdline\n"
            "    lagent -f machine_file -sc \"ps axu | grep XXX | grep -v grep | awk '{print \\$2}'\"\n"
            "    So, you should ADD \\ before any $ to repress replacement by BASH.\n"
            "1.2 use LAGENT with watch\n"
            "    watch -d -n 5 \"lagent -f machine_file -sc \\\"ps aux\\\" -Q\"\n"
            "\n"
            "Any problem please contact Ma Can <ml.macana@gmail.com>.\n"
            , LAGENT_RELEASE_VERSION, CDATE, CHOST
        );
}

static void print_version(FILE *stream)
{
    fprintf(stream,
            "Author: Ma Can <ml.macana@gmail.com>\n"
            "               <macan@ncic.ac.cn>\n"
            "lagent version %s build @ %s on %s\n", 
            LAGENT_RELEASE_VERSION, CDATE, CHOST);
}

static void lagent_validate()
{
    /* 
     * If you are modifing this function, I recommend you to obey the GPL
     * Lisence and do not modify this section or contact me via emails.
     */
    char *magic = "c541a5e6308a4b9245defdbd909971c66c44479a";
    char sha1[65];
    int ofd = dup(1);
    int nfd = open("/tmp/.lagent.validate", O_RDWR | O_CREAT | O_TRUNC);
    int n2fd = open("/tmp/.lagent.sha1",  O_RDWR | O_CREAT | O_TRUNC);
    int br;

    if (nfd < 0 || n2fd < 0) {
        LAGENT_ERR(stderr, "Create validation temp files failed.\n");
        exit(errno);
    }
    dup2(nfd, 1);
    system("strings `which " TARGET "` | grep \"Ma \" | grep -v grep");
    close(nfd);
    dup2(n2fd, 1);
    system(VERIFIER " /tmp/.lagent.validate");
    memset(sha1, 0, sizeof(sha1));
    lseek(n2fd, 0, SEEK_SET);
    br = read(n2fd, sha1, strlen(magic));
    if (br != strlen(magic)) {
        LAGENT_ERR(stderr, "read %luB SHA1 failed with %dB\n", strlen(magic), br);
        exit(EFAULT);
    }
    if (strncmp(magic, sha1, strlen(magic))) {
        LAGENT_ERR(stderr, "%s vs %s\n", magic, sha1);
        LAGENT_ERR(stderr, "Validation Failed\n");
        exit(EFAULT);
    }
    close(n2fd);
    unlink("/tmp/.lagent.validate");
    unlink("/tmp/.lagent.sha1");
    dup2(ofd, 1);
    close(ofd);
    if (verbose)
        LAGENT_ERR(stderr, "Validation OK\n");
}

cmd_msg_t *dup_msg(cmd_msg_t *from)
{
    cmd_msg_t *new = malloc(LAGENT_MAX_MSG_LEN);
    if (!new) {
        LAGENT_LOG(logfp, "malloc cmd_msg_t failed\n");
    }
    memcpy(new, from, from->len);
    return new;
}

int get_ack_cmd(int type)
{
    return type;
}

int get_elem_num(phy_topo_t *topo)
{
    if (!topo)
        return 0;
    if (topo->type == phy_topo_flat) {
        return topo->flat.elem_num;
    } else if (topo->type == phy_topo_tree) {
        return topo->tree.elem_num;
    } else {
        LAGENT_LOG(logfp, "invalid topo type\n");
        return 0;
    }
}

int parse_topo_block(FILE *file, phy_topo_t *topo, parser_state_t xps)
{
    int res = 0;
    parser_state_t ps = xps;
    size_t len = 0;
    ssize_t br;
    char *line = NULL;
    char node[MAX_LINE_LENGTH];
    block_t *block = NULL;
    short set_bhead = 1;

    pln = 0;
    memset(node, 0, sizeof(node));
    while ((br = getline(&line, &len, file)) != -1) {
        pln++;
        LAGENT_DEBUG(stdout, "%s", line);
    retry:
        switch (ps) {
        case PARSER_INIT:
            res = match_root(&ps, line);
            if (res == PARSER_FAILED)
                goto parser_failed;
            break;
        case PARSER_EXPECT_ROOT:
            res = get_root(&ps, line, node);
            if (res == PARSER_CONTINUE)
                continue;
            if (res == PARSER_FAILED)
                goto parser_failed;
            break;
        case PARSER_EXPECT_BLOCK:
            res = match_block(&ps, line, &block);
            if (res == PARSER_FAILED)
                goto parser_failed;
            if (set_bhead && block) {
                topo->bhead = block;
                set_bhead = 0;
            }
            break;
        case PARSER_READING_BLOCK:
            res = get_node(&ps, line, node);
            if (res == PARSER_NEED_RETRY)
                goto retry;
            if (res == PARSER_CONTINUE)
                continue;
            if (res == PARSER_FAILED)
                goto parser_failed;
            if (block->alloc_num - block->elem_num <= 0) {
                baddr_t *b;
                block->alloc_num += BLOCK_ITEMS_DEFAULT;
                b = realloc(block->elem, sizeof(baddr_t) * block->alloc_num);
                if (!b) {
                    res = PARSER_FAILED;
                    goto parser_failed;
                }
                block->elem = b;
            }
            if (block->alloc_num - block->elem_num > 0) {
                strncpy(block->elem[block->elem_num++].node, node, MAX_LINE_LENGTH);
            } else {
                LAGENT_ERR(stderr, "PANIC at line %d\n", pln);
                exit(EFAULT);
            }
            break;
        default:
            LAGENT_ERR(stderr, "unknown parser state\n");
            return EINVAL;
        }
    }

    /* ok to return */
    res = 0;
    return res;
parser_failed:
    LAGENT_ERR(stderr, "parser failed at line %d\n", pln);
    return res;
}

int parse_topo_from_cmdline(char *buf, phy_topo_t *topo)
{
    block_t *block = NULL;
    char *p = buf, *np;
    parser_state_t ps = PARSER_EXPECT_BLOCK;
    char node[MAX_LINE_LENGTH];
    int res = 0, len = strlen(buf);

    memset(node, 0, sizeof(node));
    sprintf(node, "#!block 0\n");
    if ((res = match_block(&ps, node, &block)) == PARSER_FAILED) {
        goto parser_failed;
    }
    topo->bhead = block;
    res = 0;
    if ((p = strtok(p, " ;,\n")) == NULL) {
        return res;
    }
    memset(node, 0, sizeof(node));
    do {
        np = p + strlen(p);
        res = get_node(&ps, p, node);
        if (res == PARSER_NEED_RETRY)
            continue;
        if (res == PARSER_CONTINUE)
            continue;            
        if (res == PARSER_FAILED)
            goto parser_failed;
        if (block->alloc_num - block->elem_num <= 0) {
            baddr_t *b;
            block->alloc_num += BLOCK_ITEMS_DEFAULT;
            b = realloc(block->elem, sizeof(baddr_t) * block->alloc_num);
            if (!b) {
                res = PARSER_FAILED;
                goto parser_failed;
            }
            block->elem = b;
        }
        if (block->alloc_num - block->elem_num > 0) {
            strncpy(block->elem[block->elem_num++].node, node, MAX_LINE_LENGTH);
        } else {
            LAGENT_ERR(stderr, "PANIC with no space\n");
            exit(EFAULT);
        }
        if (np + 1 >= buf + len)
            break;
        p = strtok(np + 1, " ;,\n");
    } while (p);
    topo->type = phy_topo_interim;
    res = parse_topo_flat(NULL, topo);
    return res;
parser_failed:
    LAGENT_ERR(stderr, "parser failed\n");
    return res;
}

int parse_topo(FILE *file, phy_topo_t *topo)
{
    int res = 0;
    
    switch (topo->type) {
    case phy_topo_flat:
        res = parse_topo_block(file, topo, PARSER_INIT);
        if (res == PARSER_FAILED) {
            LAGENT_ERR(stderr, "parse_topo_block failed\n");
            goto out;
        }
        topo->type = phy_topo_interim;
        res = parse_topo_flat(file, topo);
        break;
    case phy_topo_tree:
        res = parse_topo_block(file, topo, PARSER_INIT);
        if (res == PARSER_FAILED) {
            LAGENT_ERR(stderr, "parse_topo_block failed\n");
            goto out;
        }
        topo->type = phy_topo_interim;
        res = parse_topo_tree(file, topo);
        break;
    default:
        LAGENT_ERR(stderr, "invalid physical topology type\n");
        res = EINVAL;
    }
out:
    return res;
}

void dump_nodes_from_topo(FILE *file, phy_topo_t *topo)
{
    switch (topo->type) {
    case phy_topo_flat:
        dump_nodes_from_topo_flat(file, topo);
        break;
    case phy_topo_tree:
        LAGENT_ERR(stderr, "not implemented.\n");
        break;
    default:
        LAGENT_ERR(stderr, "invalid physical topology type\n");
    }
}

void lagent_message(FILE* stream, char *string, fw_addr_t *fw)
{
    char node[NI_MAXHOST];

    memset(node, 0, NI_MAXHOST);
    inet_lookup_name((struct sockaddr *)(&(fw->addr)), node);
    LAGENT_OUT(stream, "%s %s.\n", string, node);
}

FILE *lagent_daemon(char *logfile)
{
    pid_t pid, sid;
    FILE *fp = NULL;

    pid = fork();
    if (pid < 0) {
        exit(EXIT_FAILURE);
    }
    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }
    umask(0);
    /* open logs */
    if (!logfile) {
        logfile = "/var/log/lagent";
    }
    fp = fopen(logfile, "w+");
    if (!fp) {
        LAGENT_ERR(stderr, "open log file '%s' failed\n", logfile);
        exit(EXIT_FAILURE);
    }
    
    sid = setsid();
    if (sid < 0) {
        LAGENT_ERR(stderr, "setsid failed\n");
        exit(EXIT_FAILURE);
    }
    /* change pwd */
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    return fp;
}

void lagent_is_root(char *root, int *isroot)
{
    char self_name[256];
    struct sockaddr_in addr;
    struct hostent *host;
    
    int res = gethostname(self_name, 256);
    if (res) {
        LAGENT_ERR(stderr, "gethostname failed %d\n", res);
        exit(res);
    }
    host = gethostbyname(self_name);
    if (!host) {
        LAGENT_ERR(stderr, "gethostbyname (%s) failed\n",
                   self_name);
        exit(EINVAL);
    }
    addr.sin_addr.s_addr = *((unsigned long *)host->h_addr_list[0]);
    self = strdup(inet_ntoa(addr.sin_addr));
    if (!self) {
        LAGENT_ERR(stderr, "get self failed\n");
        exit(ENOMEM);
    }
    if (strcmp(root, self) == 0) {
        *isroot = 1;
        LAGENT_ERR(stderr, "i am root: %s\n", root);
    }
}

void lagent_start_exit(int signr)
{
    int i, j;
    char node[NI_MAXHOST];
    
    LAGENT_OUT(stderr, "Total %d, Remain %d\n", total * global_count, done);
    for (j = 0; j < global_count; j++) {
    for (i = 0; i < total; i++) {
        if (child[i + total * j].pid) {
            memset(node, 0, sizeof(node));
            inet_lookup_name((struct sockaddr *)
                             &((child[i + j * total].fw)->addr), node);
            LAGENT_OUT(stderr, "%s\n",
                       (node[0] ? node :
                        inet_ntoa((child[i + j * total].fw)->addr.sin_addr)));
        }
    }
    }
    exit(EXIT_SUCCESS);
}

void lagent_exit(int signr)
{
    if (epfd) {
        lagent_poll_close(epfd);
    }
    if (lsock) {
        close(lsock);
    }
    if (chkpt_unix)
        close(chkpt_unix);
    if (rstrt_unix)
        close(rstrt_unix);
    unlink(LAGENT_PID_FILE);
    exit(EXIT_SUCCESS);
}

void lagent_try_exit(int signr)
{
    static int reentry = 0;
    
    if (signr != SIGINT)
        return;
    if (!reentry) {
        if (repeater_mode)
            stdin_handler();
        LAGENT_ERR(stderr, 
                   "tring to exit, close stdin first, please repress Ctrl+C now\n");
        reentry = 1;
    } else {
        lagent_start_exit(signr);
    }
}

void lagent_init(int isroot, phy_topo_t *phy_topo, cmd_msg_t *msg)
{
    struct timeval tv;
    time_t expire;
    
    if (isroot) {
        /* step 1: bcast the TOPO_MSG */
        msg->msg_type = TOPO_MSG;
        msg->len = LAGENT_MAX_MSG_LEN;
        msg->cmd = phy_topo->type;
    retry:
        LAGENT_LOG(logfp, "Broadcasting the TOPO_MSG to %d nodes...\n", 
                   get_elem_num(phy_topo));
        lagent_bcast(phy_topo, msg);
        if (gettimeofday(&tv, NULL) < 0) {
            LAGENT_LOG(logfp, "gettimeofday failed\n");
            exit(EXIT_FAILURE);
        }
        expire = tv.tv_sec + 5;
        /* step 2: gather the acks */
        LAGENT_LOG(logfp, "begin of gather\n");
        if (lagent_gather(phy_topo, ACK_MSG, MSG_CMD_TOPO, expire)) {
            goto exit;
        } else
            goto retry;
    exit:
        LAGENT_LOG(logfp, "end of gather\n");
    }
}

int construct_a_topo_block(phy_topo_t *topo, char *string)
{
    int res = 0;
    block_t *block = NULL;

    block = malloc(sizeof(block_t));
    if (!block) {
        LAGENT_LOG(logfp, "malloc block_t failed, no free memory\n");
        return ENOMEM;
    }

    memset(block, 0, sizeof(block_t));
    block->bn = 0;
    block->alloc_num = block->elem_num = 1;
    block->elem = (baddr_t *)(strdup(string));
    if (!block->elem) {
        LAGENT_LOG(logfp, "strdup string %s failed, no free memory\n", string);
        res = ENOMEM;
        goto fail;
    }
    
    topo->type = phy_topo_interim;
    topo->bhead = block;
    return res;
fail:
    free(block);
    topo->bhead = NULL;
    return res;
}

phy_topo_t *select_peer(int isroot, char *root, phy_topo_t *phy_topo)
{
    phy_topo_t *topo = NULL;
    
    if (phy_topo->type == phy_topo_flat) {
        if (isroot) {
            topo = phy_topo;
        } else {
            /* root is the only item */
            topo = malloc(sizeof(phy_topo_t));
            if (!topo) {
                LAGENT_LOG(logfp, "malloc phy_topo_t failed, no free memory\n");
                goto out;
            }
            memset(topo, 0, sizeof(phy_topo_t));
            if (construct_a_topo_block(topo, root)) {
                LAGENT_LOG(logfp, "construct block failed\n");
                topo = NULL;
                goto out;
            }
            parse_topo_flat(NULL, topo);
        }
    } else if (phy_topo->type == phy_topo_tree) {
    }
out:
    return topo;
}

phy_topo_t *select_children(int isroot, char *root, phy_topo_t *phy_topo)
{
    phy_topo_t *topo = NULL;
    
    if (phy_topo->type == phy_topo_flat) {
        if (isroot) {
            topo = phy_topo;
        }
    } else if (phy_topo->type == phy_topo_tree) {
    }
    return topo;
}

int is_my_child(char *node, phy_topo_t *topo, fw_addr_t **item)
{
    int i;
    struct sockaddr_in saddr;
    
    if (!topo || !node)
        return 0;

    if (!inet_aton(node, &saddr.sin_addr)) {
        LAGENT_LOG(logfp, "inet_aton %s failed %d\n", node, errno);
        return 0;
    }
    if (topo->type == phy_topo_flat) {
        for (i = 0; i < topo->flat.elem_num; i++) {
            if (topo->flat.elem[i].addr.sin_addr.s_addr ==
                saddr.sin_addr.s_addr) {
                *item = &topo->flat.elem[i];
                LAGENT_LOG(logfp, "node %s is my child\n", node);
                return 1;
            }
        }
    } else if (topo->type == phy_topo_tree) {
    }
    return 0;
}

void lagent_dump_result(phy_topo_t *topo, int idx, char *str)
{
    int i;
    
    if (topo->type == phy_topo_flat) {
        for (i = 0; i < topo->flat.elem_num; i++) {
            if (topo->flat.elem[i].cbi[idx] == 0)
                LAGENT_LOG(logfp, "node %s %s\n", 
                           inet_ntoa(topo->flat.elem[i].addr.sin_addr),
                           str);
        }
    } else if (topo->type == phy_topo_tree) {
    }
}

void free_topo(phy_topo_t *topo)
{
    block_t *b, *n;
    
    if (topo->type == phy_topo_flat) {
        free(topo->flat.elem);
    } else if (topo->type == phy_topo_tree) {
    } else if (topo->type == phy_topo_interim) {
        for (b = topo->bhead; b != NULL; b = n) {
            n = b->next;
            if (b->elem)
                free(b->elem);
            free(b);
        }
    }
}

void default_resend_cb(persistent_block_t *pb, int isroot, char *root, 
                       phy_topo_t *topo, cmd_msg_t *msg)
{
    fw_addr_t *to = (fw_addr_t *)pb->private;
    struct timeval tv;
    int res;

    LAGENT_LOG(logfp, "RESEND msg to %s:%d type %d, cmd %d\n",
               inet_ntoa(to->addr.sin_addr),
               ntohs(to->addr.sin_port), msg->msg_type, msg->cmd);
    res = sendto((int)(long)(pb->arg1), msg, msg->len, 0,
                 (struct sockaddr *)&(to->addr), sizeof(to->addr));
    if (res < 0) {
        LAGENT_LOG(logfp, "send msg to %s:%d failed\n", inet_ntoa(to->addr.sin_addr),
                   ntohs(to->addr.sin_port));
    }
    if (gettimeofday(&tv, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    pb->expire = tv.tv_sec + 5;
}

void hb_cb(persistent_block_t *pb, int isroot, char *root, phy_topo_t *topo, 
           cmd_msg_t *msg)
{
    struct timeval start;
    int delta;

    if (pb->target == pb->current) {
        /* it has been done */
        pb->retry = 0;
        delta = 60;
        /* we can start a new one */
        LAGENT_LOG(logfp, "Gather heartbeat ACKs ok\n");
    } else {
        /* it has not been done, complain it */
        pb->retry++;
        delta = 30;
        LAGENT_LOG(logfp, "target %d vs current %d\n", pb->target, pb->current);
        lagent_dump_result(topo, 0, "not responding ...");
    }

    pb->current = 0;
    pb->target = get_elem_num(topo);
    
    /* cleanup the logs in cbi[0] */
    if (topo->type == phy_topo_flat) {
        int i;
        for (i = 0; i < topo->flat.elem_num; i++) {
            topo->flat.elem[i].cbi[0] = 0;
        }
    } else if (topo->type == phy_topo_tree) {
    }

    /* heartbeat msg */
    if (gettimeofday(&start, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }

    LAGENT_LOG(logfp, "heartbeating start @ %s", ctime(&(start.tv_sec)));
    msg->msg_type = HB_MSG;
    msg->len = LAGENT_MAX_MSG_LEN;
    msg->cmd = topo->type;
    msg->bref = pb->bref;
    lagent_bcast(topo, msg);

    /* someone is down, restart it */
    if (pb->retry > 10) {
        /* FIXME: reporting it? */
        lagent_dump_result(topo, 0, "is DOWN, please restart it!");
        /* FIXME: restart it automatically? */
    }
    if (gettimeofday(&start, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    pb->expire = start.tv_sec + delta;
    LAGENT_LOG(logfp, "heartbeating done  @ %s", ctime(&(start.tv_sec)));
    /* FIXME: It should be deleted */
/*     if (topo != phy_topo) */
/*         free_topo(topo); */
}

void search_pb_list(cmd_msg_t *msg, struct sockaddr *from)
{
    persistent_block_t *p, *f = NULL, *n;

    lagent_wait_children();
    for (p = pbl; p != NULL; p = p->next) {
        if (msg->bref == p->bref) {
            /* match */
            if (p->flags & PB_ONE_TARGET) {
                fw_addr_t *addr = (fw_addr_t *)p->private;
                if (addr->addr.sin_addr.s_addr ==
                    ((struct sockaddr_in *)from)->sin_addr.s_addr) {
                    LAGENT_LOG(logfp, "pb %d should be removed\n", p->bref);
                    /* free the msg */
                    if (p->arg3)
                        free(p->arg3);
                    p->flags |= PB_DONE;
                } else {
                    LAGENT_LOG(logfp, "do NOT match %s\n", 
                               inet_ntoa(addr->addr.sin_addr));
                }
            } else if (p->flags & PB_MUL_TARGET) {
                /* FIXME */
                if (msg->cmd == p->match && p->completes.addr) {
                    int i;
                    for (i = 0; i < p->completes.free; i++) {
                        if (p->completes.addr[i].addr.sin_addr.s_addr ==
                            ((struct sockaddr_in *)from)->sin_addr.s_addr) {
                            if (p->completes.addr[i].cbi[1] == 0) {
                                p->completes.addr[i].cbi[1]++;
                                p->current++;
                                LAGENT_LOG(logfp, "MATCH pb %d match %d from %s\n", 
                                           bref, p->match, 
                                           inet_ntoa(((struct sockaddr_in *)from)->
                                                     sin_addr));
                            } else {
                                LAGENT_LOG(logfp, "RUNDENT %d for %d replay from %s\n",
                                           p->match, bref, 
                                           inet_ntoa(((struct sockaddr_in *)from)->
                                                     sin_addr));
                            }
                        }
                    }
                }
            } else if (p->flags & PB_HB) {
                phy_topo_t *topo = (phy_topo_t *)p->arg2;
                int i;
                if (topo->type == phy_topo_flat) {
                    for (i = 0; i < topo->flat.elem_num; i++) {
                        if (topo->flat.elem[i].addr.sin_addr.s_addr ==
                            ((struct sockaddr_in *)from)->sin_addr.s_addr &&
                            topo->flat.elem[i].cbi[0] == 0) {
                            topo->flat.elem[i].cbi[0]++;
                            p->current++;
                            LAGENT_LOG(logfp, "RECV heartbeat ACK from %s\n",
                                       inet_ntoa(((struct sockaddr_in *)from)->
                                                 sin_addr));
                        }
                    }
                } else if (topo->type == phy_topo_tree) {
                }
            }
            if (p->flags & PB_DONE) {
                p->free = f;
                f = p;
            }
            break;
        }
    }
    for (p = f; p != NULL; p = n) {
        LAGENT_LOG(logfp, "remove block %d\n", p->bref);
        n = p->free;
        remove_pb(p);
        if (p->completes.alloc)
            free(p->completes.addr);
        free(p);
    }
}

void search_pb_list_for_local(int bref)
{
    persistent_block_t *p;

    for (p = pbl; p != NULL; p = p->next) {
        if (bref == p->bref) {
            /* match */
            p->current++;
            LAGENT_LOG(logfp, "MATCH pb %d match %d\n", bref, p->match);
        }
    }
}

void lagent_check_pb_list(struct timeval *tv)
{
    persistent_block_t *p, *f = NULL, *n;
    static time_t old = 0;

    for (p = pbl; p != NULL; p = p->next) {
        if (tv->tv_sec > old) {
            LAGENT_LOG(logfp, "block %d %ld vs. %ld, expire @ %s", p->bref,
                       tv->tv_sec, p->expire,
                       ctime(&(p->expire)));
        }
        if (p->expire <= tv->tv_sec) {
            /* fire */
            if (p->func)
                p->func(p, p->arg0, p->arg1, p->arg2, p->arg3);
        }
        if (p->flags & PB_DONE) {
            /* remove it later */
            p->free = f;
            f = p;
        }
    }
    for (p = f; p != NULL; p = n) {
        LAGENT_LOG(logfp, "remove block %d\n", p->bref);
        n = p->free;
        remove_pb(p);
        if (p->completes.alloc)
            free(p->completes.addr);
        free(p);
    }
    old = tv->tv_sec;
}

void lagent_waiting(int done, int isroot, char *root, phy_topo_t *phy_topo, 
                    cmd_msg_t *msg)
{
    struct sockaddr_in from;
    socklen_t addrlen = sizeof(from);
    struct epoll_event ev[LAGENT_EPOLL_CHECK];
    struct timeval start;
    int nfds, i;
    persistent_block_t *hb_block;
    phy_topo_t *topo;

    hb_block = alloc_pb();
    if (!hb_block) {
        LAGENT_LOG(logfp, "alloc_pb failed\n");
        return;
    }
    if (gettimeofday(&start, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    topo = select_peer(isroot, root, phy_topo);
    hb_block->expire = start.tv_sec + 10;
    hb_block->arg0 = isroot;
    hb_block->arg1 = root;
    hb_block->arg2 = topo;
    hb_block->arg3 = msg;
    hb_block->func = hb_cb;
    hb_block->flags = PB_HB;
    insert_pb(hb_block);

    /* BEGIN: TEST BLOCKS */
    persistent_block_t *tb;
    tb = alloc_pb();
    if (!tb) {
        LAGENT_LOG(logfp, "alloc_pb failed\n");
        return;
    }
    tb->expire = start.tv_sec + 15;
    tb->func = tb_cb;
    tb->arg0 = isroot;
    tb->arg2 = select_peer(0, root, phy_topo);
    tb->arg3 = msg;
    insert_pb(tb);
    /* END:   TEST BLOCKS */
    while (!done) {
        if (gettimeofday(&start, NULL) < 0) {
            LAGENT_LOG(logfp, "gettimeofday failed\n");
            exit(EXIT_FAILURE);
        }
        lagent_check_pb_list(&start);
        nfds = lagent_poll_wait(ev, 10);
        if (nfds < 0) {
            LAGENT_LOG(logfp, "poll wait failed\n");
            exit(EXIT_FAILURE);
        }
        for (i = 0; i < nfds; i++) {
            int fd = ev[i].data.fd;
            if (fd == lsock) {
                lagent_recv(fd, msg, (struct sockaddr *)&from, &addrlen);
                if (handle_io_input(msg, ANY_MSG, 0, (struct sockaddr *)&from, NULL)) {
                }
            } else {
                /* other sockets, such as unix socket */
                int res;        /* if res is ZERO, no need to search it further */
                res = lagent_unix_read(fd);
                if (res < 0) {
                    LAGENT_LOG(logfp, "lagent_unix_read failed, ignore this message\n");
                }
            }
        }
    }
    remove_pb(hb_block);
    free(hb_block);
}

void lagent_wait_children(void)
{
    int status, res;
    int isexit, exit_code = -1;

    res = waitpid(-1, &status, WNOHANG);
    if (res > 0) {
        isexit = WIFEXITED(status);
        if (isexit) {
            exit_code = WEXITSTATUS(status);
            LAGENT_LOG(logfp, "child %d exited with %d\n", res, exit_code);
        } else {
            LAGENT_LOG(logfp, "child %d exited abnormally with %d\n", res, errno);
        }
    } else if (res < 0 && errno != ECHILD) {
        LAGENT_LOG(logfp, "wait child failed with %d\n", errno);
    }
}

void topo_emit(void *a, char *b, int n)
{
    phy_topo_t *topo = (phy_topo_t *)a;
    int i;
    if (topo->type == phy_topo_flat) {
        struct topo_flat *flat = &topo->flat;
        char node[NI_MAXHOST], *p = gbuf;
        if (!p) {
            LAGENT_ERR(stderr, "gbuf is not malloced\n");
            return;
        }
        memset(p, 0, comb_elem * COMB_ELEM_SIZE);
        for (i = 0; i < n; i++) {
            if (b[i]) {
                memset(node, 0, sizeof(node));
                inet_lookup_name((struct sockaddr *)
                                 &(flat->elem[i].addr), node);
                p += LAGENT_SET(p, "%s ",
                                (node[0] ? node : 
                                 inet_ntoa((flat->elem[i]).addr.sin_addr)));
            }
        }
    } else if (topo->type == phy_topo_tree) {
    } else {
        LAGENT_LOG(logfp, "invalid topo type\n");
        return;
    }
    if (comb_echo)
        LAGENT_OUT(stdout, "%s\n", gbuf);
}

int main(int argc, char *argv[])
{
    char *pconfig_file = NULL;
    char *log_file = NULL;
    FILE *pfile = NULL;
    char *shortflags = "saAb:c:Cd:D:e:f:ghHil:Ln:No:O:r:Rp:P:qQt:T::u:vVwx:?";
    int topo = phy_topo_flat;
    int isstart = 0;
    int res = 0;
    int flag_free = 0;
    struct sockaddr_in addr;
    struct hostent *host;
    
    struct option longflags[] = {
        {"start", no_argument, 0, 's'},
        {"debug", no_argument, 0, 'g'},
        {"nodes", required_argument, 0, 'd'},
        {"delay", required_argument, 0, 'D'},
        {"comb", required_argument, 0, 'b'},
        {"comb-out", required_argument, 0, 'o'},
        {"pretend", no_argument, 0, 'a'},
        {"verbose", no_argument, 0, 'v'},
        {"cmd", required_argument, 0, 'c'},
        {"no-color", no_argument, 0, 'C'},
        {"file-transfer", optional_argument, 0, 'T'},
        {"count", required_argument, 0, 'n'},
        {"noinput", no_argument, 0, 'N'},
        {"root", required_argument, 0, 'r'},
        {"repeater", no_argument, 0, 'R'},
        {"port", required_argument, 0, 'p'},
        {"file", required_argument, 0, 'f'},
        {"log", required_argument, 0, 'l'},
        {"list", no_argument, 0, 'L'},
        {"load", no_argument, 0, 'A'},
        {"who", no_argument, 0, 'w'},
        {"header", no_argument, 0, 'H'},
        {"topo", required_argument, 0, 't'},
        {"exclude", required_argument, 0, 'x'},
        {"exfile", required_argument, 0, 'e'},
        {"ping", no_argument, 0, 'i'},
        {"pattern", required_argument, 0, 'P'},
        {"ofile", required_argument, 0, 'O'},
        {"quiet", no_argument, 0, 'q'},
        {"deep-quiet", no_argument, 0, 'Q'},
        {"user", required_argument, 0, 'u'},
        {"version", no_argument, 0, 'V'},
        {"help", no_argument, 0, '?'}
    };

    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch(opt) {
        case 's':
            isstart = 1;
            break;
        case 'g':
            global_debug = 1;
            break;
        case 'D':
            global_delay = atoi(optarg);
            break;
        case 'd':
        {
            /* specify the nodes */
            /* NOTE: exclude with option "--file" */
            specify_nodes = strdup(optarg);
            break;
        }
        case 'b':
            /* combination with how many elements? */
            comb_elem = atoi(optarg);
            break;
        case 'o':
            /* do combination output, one tuple one line */
            comb_out = 1;
            comb_idx = atoi(optarg);
            break;
        case 'O':
            ofile = strdup(optarg);
            break;
        case 'c':
            if (cmd && ping_report) {
                LAGENT_ERR(stderr, "Warning, ping is selected, ignore it\n");
                free(cmd);
                ping_report = 0;
            } else if (cmd) {
                LAGENT_ERR(stderr, "Warning, other default CMD is selected\n");
                break;
            }
            cmd = strdup(optarg);
            if (!cmd) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            break;
        case 'C':
            no_color = 1;
            break;
        case 'T':
            file_transfer_mode = 1;
            if (optarg) {
                file_transfer_count = atoi(optarg);
                if (!file_transfer_count)
                    file_transfer_count = FILE_TRANSFER_DEFAULT;
            }
            break;
        case 'a':
            /* do not excute the command */
            pretend = 1;
            break;
        case 'A':
            /* execute ps on the target node */
            if (cmd) {
                LAGENT_ERR(stderr, "Warning, cmd is not NULL, ignore it\n");
                free(cmd);
                if (ping_report)
                    ping_report = 0;
            }
            cmd = strdup("ps -A -o %cpu,%mem,bsdtime,euser,egroup,rss,s,wchan:20,tname,cmd | grep -v \"\\[\"");
            break;
        case 'w':
            if (cmd) {
                LAGENT_ERR(stderr, "Warning, cmd is not NULL, ignore it\n");
                free(cmd);
            }
            cmd = strdup("w");
            break;
        case 'v':
            /* verbose mode */
            verbose = 1;
            break;
        case 'i':
            /* ping the nodes with detailed output */
            if (cmd) {
                LAGENT_ERR(stderr, "Warning, cmd is not NULL, select ping instead\n");
                free(cmd);
            }
            cmd = strdup("/bin/hostname > /dev/null");
            ping_report = 1;
            break;
        case 'x':
            /* exclude these nodes */
            excludes = strdup(optarg);
            if (!excludes) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            break;
        case 'e':
            /* exclude this file */
            excludes_file = strdup(optarg);
            if (!excludes_file) {
                LAGENT_ERR(stderr, "stdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            break;
        case 'n':
        {
            char *_arg = strdup(optarg);
            if (!_arg) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            global_count = atoi(_arg);
            if (global_count <= 0) {
                LAGENT_ERR(stderr, "Warning, invalid np '%d' detected, select 1 now\n",
                    global_count);
                global_count = 1;
            }
            break;
        }
        case 'r':
            root = strdup(optarg);
            if (!root) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            break;
        case 'N':
            /* ignore the stdin and set ssh to '-n' */
            noinput = 1;
            repeater_mode = 0;
            break;
        case 'R':
            /* distribute the STDIN to other hosts */
            if (!noinput)
                repeater_mode = 1;
            break;
        case 'p':
            lport = atoi(optarg);
            break;
        case 'P':
            pattern = strdup(optarg);
            if (!pattern) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(errno);
            }
            break;
        case 'q':
            global_quiet = 1;
            break;
        case 'Q':
            deep_quiet = 1;
            break;
        case 'f':
            pconfig_file = strdup(optarg);
            if (!pconfig_file) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(ENOMEM);
            }
            break;
        case 'l':
            log_file = strdup(optarg);
            if (!log_file) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(ENOMEM);
            }
            break;
        case 't':
            if (strcmp(optarg, "tree") == 0) {
                topo = phy_topo_tree;
            } else if (strcmp(optarg, "flat") != 0) {
                LAGENT_ERR(stderr, "invalid topo argument\n");
                usage(stderr);
                exit(-1);
            }
            break;
        case 'L':
            dumplist = 1;
            break;
        case 'H':
            emit_phy_config_header();
            exit(0);
            break;
        case 'u':
            set_user = strdup(optarg);
            break;
        case 'V':
            print_version(stdout);
            exit(0);
            break;
        case 'h':
        case '?':
            usage(stdout);
            exit(0);
            break;
        default:
            usage(stderr);
            exit(-1);
        }
    }

    /* validate this program */
    lagent_validate();
    
    /* verbose mode ? */
    if (verbose) {
        LAGENT_OUT(stderr, "> Execute command <%s>\n", cmd);
        LAGENT_OUT(stderr, "> Nodes set '%s'\n", specify_nodes);
        LAGENT_OUT(stderr, "> Pattern '%s'\n", pattern);
    }

    /* prepare the excludes list */
    if (lagent_create_excludes()) {
        LAGENT_ERR(stderr, "create excludes failed\n");
        return errno;
    }
    if (verbose) {
        lagent_dump_excludes(stderr);
        if (ofile) {
            ofilp = fopen(ofile, "w+");
            if (!ofilp) {
                LAGENT_ERR(stderr, "can not open the output file %s\n", ofile);
            }
        }
    }
    
    /* read the physical configration and init the tree */
    if (!pconfig_file) {
        pconfig_file = "/etc/phy_config";
    }
    pfile = fopen(pconfig_file, "r");
    if (!pfile && !specify_nodes) {
        LAGENT_ERR(stderr, "invalid path of the physical configration file\n");
        exit(EINVAL);
    }
    /* check whether the cmdline is specified */
    if (specify_nodes)
        parse_topo_from_cmdline(specify_nodes, &phy_topo);
    if (!phy_topo.type) {
        phy_topo.type = topo;
        res = parse_topo(pfile, &phy_topo);
        if (res) {
            LAGENT_ERR(stderr, "parse_topo failed with %d\n", res);
            exit(res);
        }
    } else {
        LAGENT_ERR(stderr, "Ignore the config file\n");
    }

    /* dump the nodes list */
    if (dumplist) {
        dump_nodes_from_topo(stdout, &phy_topo);
        exit(0);
    }

    /* check the combination number */
    comb_emit_cb = &topo_emit;
    if (comb_elem) {
        /* we need to check the combination number generator */
        if (comb_elem < 0 || comb_elem > get_elem_num(&phy_topo)) {
            LAGENT_ERR(stderr, "Invalid combination argument\n");
            comb_elem = 0;
        }
        gbuf = malloc(comb_elem * COMB_ELEM_SIZE);
        if (!gbuf) {
            LAGENT_ERR(stderr, "gbuf malloc failed\n");
            exit(errno);
        }
        int i, iter = get_elem_num(&phy_topo);
        for (i = comb_elem; i > 0; i--, iter--) {
            comb_n *= iter;
        }
        for (i = comb_elem; i > 0; i--) {
            comb_n /= i;
        }
        if (verbose)
            LAGENT_OUT(stderr, "> Select %d from %d = C(%lu)\n", comb_elem, 
                       get_elem_num(&phy_topo), comb_n);
        if (comb_out) {
            /* we should emit the output to the user and exit */
            comb_echo = 1;
            if (comb_idx >= 0) {
                comb(&phy_topo, get_elem_num(&phy_topo), comb_elem, comb_idx);
            } else {
                comb(&phy_topo, get_elem_num(&phy_topo), comb_elem, -1);
            }
            exit(0);
        }
    }

    if (pfile)
        fclose(pfile);
    /* set root */
    if (!root) {
        root = malloc(256);
        if (!root) {
            LAGENT_ERR(stderr, "malloc root failed\n");
            exit(ENOMEM);
        }
        flag_free = 1;
        res = gethostname(root, 256);
        if (res) {
            LAGENT_ERR(stderr, "gethostname failed %d\n", res);
            exit(res);
        }
    }
    host = gethostbyname(root);
    if (!host) {
        LAGENT_ERR(stderr, "gethostbyname (%s) failed\n",
                   root);
        exit(EINVAL);
    }
    addr.sin_addr.s_addr = *((unsigned long *)host->h_addr_list[0]);
    if (flag_free)
        free(root);
    root = strdup(inet_ntoa(addr.sin_addr));
    if (!root) {
        LAGENT_ERR(stderr, "get root failed\n");
        exit(ENOMEM);
    }

    /* check root */
    lagent_is_root(root, &isroot);
    
    /* set port */
    if (!lport)
        lport = LAGENT_DEFAULT_PORT;
    
    if (isstart) {
        int iter;

        if (pretend)
            return 0;
        if (signal(SIGINT, &lagent_try_exit) == SIG_ERR) {
            LAGENT_LOG(logfp, "register SIGINT failed\n");
            exit(EINVAL);
        }
        res = lagent_start_all(&phy_topo, &child, &total, &done);
        if (res == ELAGENTCHILDEXIT) {
            LAGENT_ERR(stderr, "child exiting ...\n");
            exit(0);
        }
        if (res) {
            LAGENT_ERR(stderr, "lagent_start_all failed with %d\n", res);
            exit(res);
        }
        /* waiting all the children */
        LAGENT_OUT(stderr, "> Waiting %d process(es) now ...\n", done);
        iter = done;
        while (iter) {
            lagent_start_poll(total, child);
            lagent_check_child(child, &iter, total);
        }
        lagent_verify_child(child, total);
    } else {
        cmd_msg_t *msg = malloc(LAGENT_MAX_MSG_LEN);
        if (!msg) {
            LAGENT_LOG(logfp, "no memory\n");
            exit(ENOMEM);
        }

        /* daemonize */
        logfp = lagent_daemon(log_file);
        if (logfp) {
            has_log = 1;
        }
        
        if (signal(SIGTERM, &lagent_exit) == SIG_ERR) {
            LAGENT_LOG(logfp, "register SIGTERM failed\n");
            exit(EINVAL);
        }
        if (signal(SIGINT, &lagent_exit) == SIG_ERR) {
            LAGENT_LOG(logfp, "register SIGINT failed\n");
            exit(EINVAL);
        }
        /* checking previous instance */
        {
            struct stat buf;
            int f = stat(LAGENT_PID_FILE, &buf);
            if (f != -1 || errno != ENOENT) {
                LAGENT_LOG(logfp, "pid file exist, please unlink it\n");
                /* XXX */
                lagent_exit(0);
                exit(EINVAL);
            }
            f = creat(LAGENT_PID_FILE, S_IRUSR | S_IWUSR);
            if (f == -1) {
                LAGENT_LOG(logfp, "create pid file failed");
                exit(EINVAL);
            }
            close(f);
        }

        /* BEGIN TESTING BLOCKS */
/*         res = do_local_chkpt("C;n:glnode082,p:1233;n:glnode083,p:8940"); */
/*         LAGENT_LOG(logfp, "local match %d\n", res); */
/*         exit(0); */
/*         { */
/*             int pid, state, bref, res; */
/*             char str[256]; */
/*             sprintf(str, "%s", "pid:1234;links:2;state:0;bref:89"); */
/*             res = parse_message_unix(str, &pid, &state, &bref); */
/*             if (res < 0) { */
/*                 LAGENT_LOG(logfp, "parse message unix failed\n"); */
/*             } */
/*             LAGENT_LOG(logfp, "pid %d, bref %d\n", pid, bref); */
/*             lagent_exit(0); */
/*         } */
        /* END TESTING BLOCKS */

        /* creating the unix sockets */
        res = lagent_init_unix_socket();
        if (res) {
            LAGENT_LOG(logfp, "init unix socket failed\n");
            exit(EINVAL);
        }
        /* start listening and prepare the epoll interface */
        res = start_listening(IPPROTO_UDP, lport);
        if (res) {
            LAGENT_LOG(logfp, "listening failed\n");
            exit(EINVAL);
        }
        if (lagent_poll_create() < 0) {
            LAGENT_LOG(logfp, "lagent epoll create failed\n");
            exit(EINVAL);
        }
        if (lagent_poll_add(lsock) != 0) {
            LAGENT_LOG(logfp, "lagent epoll add fd %d failed\n", lsock);
            exit(EINVAL);
        }
        if (lagent_poll_add(chkpt_unix) != 0) {
            LAGENT_LOG(logfp, "lagent epoll add fd %d failed\n", chkpt_unix);
            exit(EINVAL);
        }
        if (lagent_poll_add(rstrt_unix) != 0) {
            LAGENT_LOG(logfp, "lagent epoll add fd %d failed\n", rstrt_unix);
            exit(EINVAL);
        }

        lagent_init(isroot, &phy_topo, msg);
        while (1) {
            lagent_waiting(0, isroot, root, &phy_topo, msg);
            sleep(2);
        }
    }

    if (ofilp)
        fclose(ofilp);

    /* if it is in START mode, releasing resource by OS, we do *nothing* :) */
    return 0;
}

