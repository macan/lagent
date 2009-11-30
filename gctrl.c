#include "gctrl.h"
#include "lagent.h"

job_t *job_list = NULL;
struct lister *lister_list = NULL;
char *input = NULL;
int epfd = -1;
int deep_quiet = 0;
struct gshell_internal gi = {
    .doecho = 0,
    .display_clients = 0,
    .sock = 0,
    .nv = NULL,
    .node = NULL,
    .port = NULL,
};

static void usage(FILE *stream)
{
    fprintf(stream,
            "Usage: gctrl [-l]\n"
            "\n"
            "General options:\n"
            " -l, --lister      external program to get MPI job information.\n"
            " -f, --file        read data from the file.\n"
            " -?, --help        print this help message.\n"
            " -v, --version     print version information.\n"
            "\n"
        );
}

static void print_version(FILE *stream)
{
    fprintf(stream,
            "gctrl version %s\n", GCTRL_RELEASE_VERSION);
}

static int is_in_set(char *p, char *blank)
{
    int blen = strlen(blank), i;
    for (i = 0; i < blen; i++) {
        if (*p == blank[i])
            return 1;
    }
    return 0;
}

static char *eat_blanks(char *data, char *blank)
{
    char *p = data;
    int len;

    len = strlen(data);
    while (is_in_set(p, blank)) {
        if (*p == '\0' || (p - data) > len)
            break;
        p++;
    }
    return p;
}

static int read_job(char **data, job_t **xjob, int start)
{
    char *p = *data;
    job_t *job = NULL, *j;
    int res = 0;
    
    NEW_JOB(job);
    if (!job) {
        perror("NEW_JOB()");
        return ENOMEM;
    }
    INIT_JOB(job);

    /* read in the block */
    if (start)
        TOKEN_START(p, " =\n");
    if (!TOKEN_MATCH(p, "jobid")) {
        LAGENT_ERR(stderr, "match 'jobid' failed\n");
        res = EINVAL;
        goto out;
    }
    TOKEN_NEXT(p, " =\n");
    job->jobid = strdup(p);
    TOKEN_NEXT(p, " =\n");
    if (!TOKEN_MATCH(p, "jobalias")) {
        LAGENT_ERR(stderr, "match 'jobalias' failed\n");
        res = EINVAL;
        goto out;
    }
    TOKEN_NEXT(p, " =\n");
    if (!TOKEN_MATCH(p, "username")) {
        job->jobalias = strdup(p);
        TOKEN_NEXT(p, " =\n");
    }
    TOKEN_NEXT(p, " =\n");
    job->username = strdup(p);
    if ((j = JOB_MATCH(job, job_list)) == NULL)
        job->ranks++;
    else {
        JOB_FREE(job);
        j->ranks++;
        job = NULL;
    }

    /* IGNORE */
    TOKEN_NEXT(p, " =\n");      /* host */
    TOKEN_NEXT(p, " =\n");
    TOKEN_NEXT(p, " =\n");      /* pid */
    TOKEN_NEXT(p, " =\n");
    TOKEN_NEXT(p, " =\n");      /* sid */
    TOKEN_NEXT(p, " =\n");
    TOKEN_NEXT(p, " =\n");      /* rank */
    TOKEN_NEXT(p, " =\n");
    TOKEN_NEXT(p, " =\n");      /* pgm */
    TOKEN_NEXT(p, " =\n");
out:
    *data = p;
    *xjob = job;
    return res;
}

static int read_client(char **data, int start)
{
    char *p = *data;
    client_t *cli = NULL;
    job_t job, *t;
    int res = 0;

    /* read in the block */
    /* jobid */
    if (start)
        TOKEN_START(p, " =\n");
    if (!TOKEN_MATCH(p, "jobid")) {
        LAGENT_ERR(stderr, "match 'jobid' failed\n");
        res = EINVAL;
        goto out;
    }
    TOKEN_NEXT(p, " =\n");
    INIT_JOB((&job));
    job.jobid = strdup(p);
    TOKEN_NEXT(p, " =\n");      /* jobalias */
    TOKEN_NEXT(p, " =\n");
    if (!TOKEN_MATCH(p, "username")) {
        TOKEN_NEXT(p, " =\n");  /* username */
    }
    TOKEN_NEXT(p, " =\n");
    /* find the job_t */
    if ((t = JOB_MATCH((&job), job_list)) != NULL) {
        if (!t->clients)
            t->clients = malloc(sizeof(client_t) * t->ranks);
        cli = t->clients;
    } else {
        LAGENT_ERR(stderr, "can not find the job_t\n");
        res = EINVAL;
        goto out;
    }
    {
        char *host, *pid, *sid, *rank;
        int r;
        TOKEN_NEXT(p, " =\n");      /* host */
        if (!TOKEN_MATCH(p, "host")) {
            LAGENT_ERR(stderr, "match 'host' failed\n");
            res = EINVAL;
            goto out;
        }
        TOKEN_NEXT(p, " =\n");
        host = strdup(p);
        TOKEN_NEXT(p, " =\n");      /* pid */
        if (!TOKEN_MATCH(p, "pid")) {
            LAGENT_ERR(stderr, "match 'pid' failed\n");
            res = EINVAL;
            goto out;
        }
        TOKEN_NEXT(p, " =\n");
        pid = strdup(p);
        TOKEN_NEXT(p, " =\n");      /* sid */
        if (!TOKEN_MATCH(p, "sid")) {
            LAGENT_ERR(stderr, "match 'sid' failed\n");
            res = EINVAL;
            goto out;
        }
        TOKEN_NEXT(p, " =\n");
        sid = strdup(p);
        TOKEN_NEXT(p, " =\n");      /* rank */
        if (!TOKEN_MATCH(p, "rank")) {
            LAGENT_ERR(stderr, "match 'rank' failed\n");
            res = EINVAL;
            goto out;
        }
        TOKEN_NEXT(p, " =\n");
        rank = strdup(p);
        r = atoi(rank);
        cli[r].host = host;
        cli[r].pid = atoi(pid);
        cli[r].sid = atoi(sid);
        cli[r].rank = r;
        TOKEN_NEXT(p, " =\n");      /* pgm */
        if (!TOKEN_MATCH(p, "pgm")) {
            LAGENT_ERR(stderr, "match 'pgm' failed\n");
            res = EINVAL;
            goto out;
        }
        TOKEN_NEXT(p, " =\n");
        cli[r].pgm = strdup(p);
    }
out:
    *data = p;
    return res;
}

int mpd_is_data_valid(char *data)
{
    char *p;
    
    if (!data)
        return 0;
    p = eat_blanks(data, " \n");
    if (strncmp(p, "jobid", 5) == 0) {
        return 1;
    } else
        return 0;
}

int mpd_scan_jobs(char *data, job_t *job_tail)
{
    job_t *job;
    char *p, *m;
    int len, start = 1;

    if (!data)
        return 0;

    len = strlen(data);
    m = malloc(len);
    memcpy(m, data, len);
    p = m;
    while (p - m < len) {
        if (read_job(&p, &job, start)) {
            LAGENT_ERR(stderr, "read_block() failed\n");
            return EINVAL;
        }
        if (job) {
            job_tail = JOB_TAIL(job_tail);
            if (job_tail) {
                JOB_APPEND(job, job_tail);
            } else {
                job_list = job;
                job_tail = job_list;
            }
        }
        TOKEN_NEXT(p, " =\n");
        if (!p)
            break;
        start = 0;
    }
    
    JOB_DUMP(job_list);
    free(m);
    return 0;
}

int mpd_scan_clients(char *data, job_t *job_head)
{
    char *p, *m;
    int len, start = 1;

    if (!data)
        return 0;
    len = strlen(data);
    m = malloc(len);
    memcpy(m, data, len);
    p = m;
    while (p - m < len) {
        if (read_client(&p, start)) {
            LAGENT_ERR(stderr, "read_client() failed\n");
            return EINVAL;
        }
        TOKEN_NEXT(p, " =\n");
        if (!p)
            break;
        start = 0;
    }

    JOB_DUMP(job_list);
    return 0;
}

struct lister mpd_lister = {
    .name = "mpdlistjob",
    .ops.is_data_valid = mpd_is_data_valid,
    .ops.scan_jobs = mpd_scan_jobs,
    .ops.scan_clients = mpd_scan_clients,
    .next = NULL,    
};

int debug_scan_jobs(char *data, job_t *job_tail)
{
    if (input)
        data = input;
    return mpd_scan_jobs(data, job_tail);
}

int debug_scan_clients(char *data, job_t *job_head)
{
    if (input)
        data = input;
    return mpd_scan_clients(data, job_head);
}

struct lister debug_lister = {
    .name = "debug",
    .ops.is_data_valid = mpd_is_data_valid,
    .ops.scan_jobs = debug_scan_jobs,
    .ops.scan_clients = debug_scan_clients,
    .next = NULL,
};

struct lister *select_lister(char *lister_name)
{
    struct lister *l = lister_list;
    
    while (l) {
        if (!strcmp(lister_name, l->name)) {
            return l;
        }
        l = l->next;
    }

    return NULL;
}

void gshell_output(FILE *stream, char *fmt, ...)
{
    va_list arglist;

    va_start(arglist, fmt);
    vfprintf(stream, fmt, arglist);
    va_end(arglist);
    fflush(stream);
}

#define PROMPT  (prompt ? "\n" : (prompt = 0, ""))

void gshell_help(int prompt)
{
    gshell_output(stdout, "Commands List:\n"
                  "help\tprint this help list\n"
                  "list\tlist the jobs or nvlist\n"
                  "    \tjob\n"
                  "    \tnvlist\n"
                  "    \tinternal\n"
                  "set\tset name/value pairs\n"
        );
}

void gshell_invalid_cmd(int prompt)
{
    gshell_output(stderr, "%sInvalid commmand\n", PROMPT);
}

void gshell_display_gi(int prompt)
{
    gshell_output(stdout, "%sGI:\n", PROMPT);
    gshell_output(stdout, "\techo\t%d\n", gi.doecho);
    gshell_output(stdout, "\tdisplay_clients\t%d\n", gi.display_clients);
    gshell_output(stdout, "\tnode\t%s\n", gi.node);
    gshell_output(stdout, "\tport\t%s\n", gi.port);
    gshell_output(stdout, "\tsock\t%d\n", gi.sock);
}

void gshell_list(char *line, int prompt)
{
    job_t *j = job_list;
    int index = 1, jon = 1;     /* 0 is internal, 1 is for job, 2 is for nvlist */
    char *p = line;

    p = strtok(p, " \n");
    if (!p) {
        gshell_output(stderr, "%sInvalid command\n", PROMPT);
        return;
    }
    if (strcmp(p, "list") != 0) {
        gshell_output(stderr, "%sInvalid command '%s'\n", PROMPT, p);
        return;
    }
    if (!(p = strtok(NULL, " \n"))) {
        jon = 1;
    } else {
        if (strcmp(p, "job") == 0) {
            jon = 1;
        } else if (strcmp(p, "nvlist") == 0) {
            jon = 2;
        } else if (strcmp(p, "internal") == 0) {
            jon = 0;
            gshell_display_gi(prompt);
        } else {
            gshell_output(stderr, "%sInvalid list command argument %s\n", PROMPT, p);
            return;
        }
    }
    if (jon == 1) {
        while (j) {
            printf("JOB %d:\n", index++);
            printf("\tJOBID\t%s\n", j->jobid);
            printf("\tJOBALAIS\t%s\n", j->jobalias);
            printf("\tUSERNAME\t%s\n", j->username);
            printf("\tRANKS\t%d\n", j->ranks);
            if (!gi.display_clients)
                goto cont;
            int i;
            for (i = 0; i < j->ranks && j->clients; i++) {
                printf("\thost %s\n",
                       j->clients[i].host);
                printf("\tpid %d\n",
                       j->clients[i].pid);
                printf("\tsid %d\n",
                       j->clients[i].sid);
                printf("\trank %d\n",
                       j->clients[i].rank);
                printf("\tpgm %s\n",
                       j->clients[i].pgm);
            }
        cont:
            j = j->next;
        }
    } else if (jon == 2) {
        struct nvlist *nv = gi.nv;
        while (nv) {
            printf("NVLIST %d:\n", index++);
            printf("\tNAME\t%s\n", nv->name);
            printf("\tVALUE\t%s\n", nv->value);
            nv = nv->next;
        }
    }
}

void gshell_scan_nvlist()
{
    struct nvlist *nv = gi.nv, *prev = gi.nv;
    int f = 0;

    while (nv) {
        if (strcmp(nv->name, "display_clients") == 0) {
            gi.display_clients = atoi(nv->value);
            f = 1;
        } else if (strcmp(nv->name, "node") == 0) {
            gi.node = strdup(nv->value);
            f = 1;
        } else if (strcmp(nv->name, "port") == 0) {
            gi.port = strdup(nv->value);
            f = 1;
        } else if (strcmp(nv->name, "echo") == 0) {
            gi.doecho = atoi(nv->value);
            f = 1;
        }
        if (f) {
            if (nv == gi.nv) {
                gi.nv = nv->next;
                NVLIST_FREE(nv);
                prev = gi.nv;
                nv = gi.nv;
            } else {
                prev->next = nv->next;
                NVLIST_FREE(nv);
                nv = prev->next;
            }
        } else {
            prev = nv;
            nv = nv->next;
        }
    }
}

void gshell_set(char *line, int prompt)
{
    char *p = line;
    char *name, *value;
    struct nvlist *nv;

    p = strtok(p, " =\n");
    if (!p) {
        gshell_output(stderr, "%sInvalid command\n", PROMPT);
        return;
    }
    if (strcmp(p, "set") != 0) {
        gshell_output(stderr, "%sInvalid command '%s'\n", PROMPT, p);
        return;
    }
    p = strtok(NULL, " =\n");
    if (!p) {
        gshell_output(stderr, "%sInvalid set name\n", PROMPT);
        return;
    }
    name = p;
    p = strtok(NULL, " =\n");
    if (!p) {
        gshell_output(stderr, "%sInvalid set value\n", PROMPT);
        return;
    }
    value = p;
    nv = NVLIST_ALLOC();
    nv->name = strdup(name);
    nv->value = strdup(value);
    nv->next = gi.nv;
    gi.nv = nv;
    gshell_scan_nvlist();
}

void gshell_commit(char *line, int prompt)
{
    struct hostent *host;
    
    gi.sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (gi.sock < 0) {
        gshell_output(stderr, "create socket failed with %d\n", errno);
        return;
    }
    host = gethostbyname(gi.node);
    if (!host) {
        gshell_output(stderr, "gethostbyname %s failed with %d\n",
                      gi.node, errno);
        return;
    }
    memset(&gi.addr, 0, sizeof(struct sockaddr_in));
    gi.addr.sin_family = AF_INET;
    gi.addr.sin_addr.s_addr = *(unsigned long *)host->h_addr_list[0];
    gi.addr.sin_port = htons(atoi(gi.port));

    /* add stdin to the polling set */
    {
        struct epoll_event ev;
        int res;
        if ((res = fcntl(gi.sock, F_GETFL)) < 0) {
            LAGENT_ERR(stderr, "fcntl get flags failed with %d\n", errno);
            return;
        }
        if (fcntl(gi.sock, F_SETFL, res | O_NONBLOCK)) {
            LAGENT_ERR(stderr, "fcntl set O_NONBLOCK failed with %d\n", errno);
            return;
        }
        ev.events = EPOLLIN | EPOLLERR;
        ev.data.fd = gi.sock;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, 0, &ev) == -1) {
            LAGENT_ERR(stderr, "epoll add fd %d failed with %d\n", 0, errno);
            return;
        }
    }
}

void gshell_chkpt(char *line, int prompt)
{
    if (gi.sock <= 0)
        return;
}

int gshell_parse_cmd(char *line, int len, int prompt)
{
    int quit = 0;
    
    if (gi.doecho)
        gshell_output(stdout, "%sECHO %s", PROMPT, line);
    if (strncmp(line, "help", 4) == 0) {
        gshell_help(prompt);
    } else if (strncmp(line, "list", 4) == 0) {
        gshell_output(stdout, "%s", PROMPT);
        gshell_list(line, prompt);
    } else if (strncmp(line, "set", 3) == 0) {
        gshell_set(line, prompt);
    } else if (strncmp(line, "commit", 6) == 0) {
        gshell_commit(line, prompt);
    } else if (strncmp(line, "chkpt", 5) == 0) {
    } else if (strncmp(line, "quit", 4) == 0) {
        quit = 1;
    } else {
        gshell_invalid_cmd(prompt);
    }
    return quit;
}

int gshell()
{
    int res = 0;
    int usock = -1, quit = 0;

    epfd = epoll_create(GCTRL_EPOLL_QUEUE_LEN);
    if (epfd == -1) {
        LAGENT_ERR(stderr, "create gshell epfd failed with %d\n", errno);
        return errno;
    }
    /* add stdin to the polling set */
    {
        struct epoll_event ev;
        if ((res = fcntl(0, F_GETFL)) < 0) {
            LAGENT_ERR(stderr, "fcntl get flags failed with %d\n", errno);
            res = errno;
            goto out;
        }
        if (fcntl(0, F_SETFL, res | O_NONBLOCK)) {
            LAGENT_ERR(stderr, "fcntl set O_NONBLOCK failed with %d\n", errno);
            res = errno;
            goto out;
        }
        ev.events = EPOLLIN | EPOLLERR;
        ev.data.fd = 0;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, 0, &ev) == -1) {
            LAGENT_ERR(stderr, "epoll add fd %d failed with %d\n", 0, errno);
            res = errno;
            goto out;
        }
    }
    gshell_output(stdout, "Welcome to the GSHELL *.*\n"
                  "Type 'help' if you need help.\n");
    while (!quit) {
        struct epoll_event ev[GCTRL_EPOLL_QUEUE_LEN];
        char line[256];
        size_t br;
        int i, nfds, prompt = 0;

        /* waiting for input */
        gshell_output(stdout, "> ");
        prompt = 1;
        nfds = epoll_wait(epfd, ev, GCTRL_EPOLL_QUEUE_LEN, -1);
        if (nfds < 0) {
            LAGENT_ERR(stderr, "%spoll wait failed with %d\n", 
                       (prompt ? "\n" : (prompt = 0, "")),
                       errno);
            res = errno;
            goto out;
        }
        res = 0;
        for (i = 0; i < nfds; i++) {
            int fd = ev[i].data.fd;
            if (fd == usock) {
                /* udp socket */
            } else if (!fd) {
                /* stdin */
                prompt = 0;
                do {
                    memset(line, 0, sizeof(line));
                    br = read(fd, line, 255);
                    if (br) {
                        quit = gshell_parse_cmd(line, 255, prompt);
                    }
                } while (br == 255);
            }
        }
    }
out:
    close(epfd);
    epfd = -1;
    return res;
}

int main(int argc, char *argv[])
{
    struct lister *lister = NULL;
    char *lister_name = NULL;
    char *file_name = NULL;
    char *shortflags = "l:f:vh?";
    struct option longflags[] = {
        {"lister", required_argument, 0, 'l'},
        {"file", required_argument, 0, 'f'},
        {"version", no_argument, 0, 'v'},
        {"help", no_argument, 0, '?'}
    };

    lister_list = &mpd_lister;
    mpd_lister.next = &debug_lister;
    
    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch(opt) {
        case 'l':
            lister_name = strdup(optarg);
            if (!lister_name) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(ENOMEM);
            }
            lister = select_lister(lister_name);
            if (!lister) {
                LAGENT_ERR(stderr, "select_lister failed with '%s'\n", lister_name);
                exit(EINVAL);
            }
            free(lister_name);  /* do not user lister_name hereafter */
            break;
        case 'f':
            file_name = strdup(optarg);
            if (!file_name) {
                LAGENT_ERR(stderr, "strdup failed on string '%s'\n", optarg);
                exit(ENOMEM);
            }
            break;
        case 'v':
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
    /* check file and load in it */
    if (file_name) {
        /* open the file and get the file size */
        struct stat buf;
        off_t br, bl;
        int fd = open(file_name, O_RDONLY);
        if (fd == -1) {
            perror("open()");
        }
        if (fstat(fd, &buf) == -1) {
            perror("fstat()");
        }
        input = malloc(buf.st_size + 1);
        if (!input) {
            perror("malloc()");
        }
        memset(input, 0, buf.st_size + 1);
        bl = buf.st_size;
        br = 0;
        while (bl) {
            br = read(fd, input + (buf.st_size - bl), bl);
            if (br == -1) {
                perror("read()");
                return EIO;
            }
            bl -= br;
        }
    }
    /* set default lister */
    if (!lister)
        lister = lister_list;

    if (lister->ops.is_data_valid(input)) {
        LAGENT_ERR(stderr, "is valid\n");
        lister->ops.scan_jobs(input, job_list);
        lister->ops.scan_clients(input, job_list);
    } else
        LAGENT_ERR(stderr, "is not valid\n");

    /* FIXME: find the root node, then create the local UDP socket. Create one
     * shell and waiting for the commands from the user. Construct message and
     * send it through UPD socket. USE persistent_block_t! */
    if (gshell()) {
        LAGENT_ERR(stderr, "gshell() failed with %d\n", errno);
    }

    if (file_name)
        free(file_name);
    return 0;
}
