/**
 * Copyright (c) 2008 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * net.c is the core network code
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

int lsock = -1;
unsigned short lport = 0;
int epfd = 0;

void inet_lookup_name(struct sockaddr *sa, char *name)
{
    if (getnameinfo(sa, sizeof(struct sockaddr_in), name, NI_MAXHOST, NULL, 0,
                    NI_NOFQDN | NI_NAMEREQD)) {
        LAGENT_ERR(stderr, "getnameinfo failed with %d\n", errno);
    }
}

struct sockaddr *dup_addr(struct sockaddr *from)
{
    struct sockaddr *new = malloc(sizeof(struct sockaddr));
    if (!new) {
        LAGENT_LOG(logfp, "malloc sockaddr failed\n");
    }
    memcpy(new, from, sizeof(struct sockaddr));
    return new;
}

int start_listening(int protocol, unsigned short port) 
{
    int flag_on = 1;
    int res = 0;
    struct sockaddr_in server_addr;

    /* create the socket */
    lsock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (lsock < 0) {
        perror("socket() failed");
        LAGENT_ERR(stderr, "create socket failed %d\n", lsock);
        return lsock;
    }
    res = setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &flag_on,
                     sizeof(flag_on));
    if (res < 0) {
        perror("setsockopt() failed");
        LAGENT_ERR(stderr, "setsockopt failed %d\n", lsock);
        return res;
    }
     
    memset(&server_addr, 0, sizeof(struct sockaddr_in));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    res = bind(lsock, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (res < 0) {
        perror("bind() failed\n");
        LAGENT_ERR(stderr, "bind address failed\n");
        return res;
    }
    
    return 0;
}

int send_msg(int protocol, unsigned short port, fw_addr_t *addr, msg_header_t *msg)
{
    int res;
    cmd_msg_t *m = (cmd_msg_t *)msg;
    
    addr->addr.sin_family = protocol;
    addr->addr.sin_port = htons(port);
    
    res = sendto(lsock, msg, msg->len, 0, 
                 (struct sockaddr *)&(addr->addr), sizeof(addr->addr));
    LAGENT_LOG(logfp, "send msg to %s:%d type %d, cmd %d\n", 
               inet_ntoa(addr->addr.sin_addr),
               ntohs(addr->addr.sin_port), m->msg_type, m->cmd);
    
    if (res < 0) {
        LAGENT_LOG(logfp, "send msg to %s:%d failed\n", inet_ntoa(addr->addr.sin_addr),
                   ntohs(addr->addr.sin_port));
    }
    return res;
}

int recv_msg(int fd, int protocol, void *buf, struct sockaddr *from, socklen_t *addrlen)
{
    int res;

    res = recvfrom(fd, buf, LAGENT_MAX_MSG_LEN, 0, from, addrlen);
    if (res < 0) {
        LAGENT_LOG(logfp, "recv msg failed %d\n", res);
    }
    
    return res;
}

int lagent_bcast(phy_topo_t *topo, cmd_msg_t *msg)
{
    int i;
    
    if (!topo)
        return 0;
    if (topo->type == phy_topo_flat) {
        for (i = 0; i < topo->flat.elem_num; i++) {
            send_msg(AF_INET, lport, &topo->flat.elem[i], (msg_header_t *)msg);
        }
    } else if (topo->type == phy_topo_tree) {
    }

    return 0;
}

int lagent_rbcast(int fd, phy_topo_t *topo, cmd_msg_t *msg)
{
    int i;

    if (!topo)
        return 0;
    if (topo->type == phy_topo_flat) {
        for (i = 0; i < topo->flat.elem_num; i++) {
            lagent_rsend(fd, msg, &topo->flat.elem[i], lport);
        }
    } else if (topo->type == phy_topo_tree) {
    }

    return 0;
}

int lagent_send(int fd, void *msg, fw_addr_t *addr, unsigned short port)
{
    int res;
    cmd_msg_t *m = (cmd_msg_t *)msg;

    addr->addr.sin_family = AF_INET;
    addr->addr.sin_port = htons(port);
    
    res = sendto(fd, m, m->len, 0,
                 (struct sockaddr *)&(addr->addr), sizeof(addr->addr));
    LAGENT_LOG(logfp, "send msg to %s:%d type %d, cmd %d\n",
               inet_ntoa(addr->addr.sin_addr),
               ntohs(addr->addr.sin_port), m->msg_type, m->cmd);
    if (res < 0) {
        LAGENT_LOG(logfp, "send msg to %s:%d failed\n", inet_ntoa(addr->addr.sin_addr),
                   ntohs(addr->addr.sin_port));
    }
    return res;
}

/**
 * Reliable Send: addr must be a persistent(will not be freed) and valid address
 */
int lagent_rsend(int fd, void *msg, fw_addr_t *addr, unsigned short port)
{
    int res;
    cmd_msg_t *m = (cmd_msg_t *)msg;
    persistent_block_t *p;
    struct timeval tv;

    p = alloc_pb();
    if (!p) {
        LAGENT_LOG(logfp, "alloc_pb failed\n");
        return ENOMEM;
    }
    m->bref = p->bref;

    addr->addr.sin_family = AF_INET;
    addr->addr.sin_port = htons(port);
    
    LAGENT_LOG(logfp, "send msg to %s:%d type %d, cmd %d\n",
               inet_ntoa(addr->addr.sin_addr),
               ntohs(addr->addr.sin_port), m->msg_type, m->cmd);
    res = sendto(fd, m, m->len, 0,
                 (struct sockaddr *)&(addr->addr), sizeof(addr->addr));
    if (res < 0) {
        LAGENT_LOG(logfp, "send msg to %s:%d failed\n", inet_ntoa(addr->addr.sin_addr),
                   ntohs(addr->addr.sin_port));
        free(p);
    }

    if (gettimeofday(&tv, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    p->flags |= PB_ONE_TARGET;
    p->private = addr;
    p->arg0 = get_ack_cmd(m->msg_type);
    p->arg1 = (void *)(long)fd;
    p->arg3 = dup_msg(msg);
    p->func = default_resend_cb;
    p->expire = tv.tv_sec + 5;
    insert_pb(p);
    /* when receives the ack, we should set done to 1, and release the pb */
    /* FIXME: need ONE default resend routine */
    /* FIXME: should set p->arg3 (for msg) */

    return res;
}

int lagent_recv(int fd, void *msg, struct sockaddr *from, socklen_t *len)
{
    cmd_msg_t *m = (cmd_msg_t *)msg;
    struct sockaddr_in *in = (struct sockaddr_in *)from;
    
    recv_msg(fd, AF_INET, msg, from, len);
    LAGENT_LOG(logfp, "Recv msg from %s:%d type %d, cmd %d\n",
               inet_ntoa(in->sin_addr), ntohs(in->sin_port),
               m->msg_type, m->cmd);
    return 0;
}

int lagent_send_ack(int cmd, int bref, struct sockaddr *to)
{
    struct sockaddr_in *in = (struct sockaddr_in *)to;
    cmd_msg_t msg;
    int res = 0;
    
    msg.msg_type = ACK_MSG;
    msg.len = LAGENT_MAX_MSG_LEN;
    msg.cmd = cmd;
    msg.bref = bref;

    LAGENT_LOG(logfp, "send ack %d to %s:%d\n", cmd,
               inet_ntoa(in->sin_addr), ntohs(in->sin_port));

    res = sendto(lsock, &msg, msg.len, 0, to, sizeof(*to));
    if (res < 0) {
        LAGENT_LOG(logfp, "send ack %d to %s:%d failed\n", cmd,
                   inet_ntoa(in->sin_addr), ntohs(in->sin_port));
    }
    return res;
}
    
int default_handle_io_cb(cmd_msg_t *msg, int type, int cmd, struct sockaddr *from)
{
    int res = 0;
    
    switch (msg->msg_type) {
    case TOPO_MSG:
        /* ack it */
        lagent_send_ack(MSG_CMD_TOPO, msg->bref, from);
        if (type == TOPO_MSG)
            res = 1;
        break;
    case ACK_MSG:
        if (type == ACK_MSG && msg->cmd == cmd)
            res = 1;
        search_pb_list(msg, from);
        break;
    case HB_MSG:
        /* ack it */
        lagent_send_ack(MSG_CMD_HB, msg->bref, from);
        if (type == HB_MSG)
            res = 1;
        break;
    case CHKPT_MSG:
    {
        phy_topo_t *topo;
        persistent_block_t *p;
        struct timeval tv;
        /* when receiving a chkpt request from the root/parent */
        /* Step 1: ack it */
        lagent_send_ack(MSG_CMD_CHKPT, msg->bref, from);
        if (!check_chkpt(msg, from)) {
            LAGENT_LOG(logfp, "invalid CHKPT_MSG from %s\n", 
                       inet_ntoa(((struct sockaddr_in *)(from))->sin_addr));
            break;
        }
        LAGENT_LOG(logfp, "create a new chkp requst %d\n", msg->cmd);
        /*
         * Step 2: construct a control struct and distribute the request to
         * the children
         */
        topo = select_children(isroot, root, &phy_topo);
        /* The following Steps should be done in pb_callback */
        p = alloc_pb();
        if (!p) {
            LAGENT_LOG(logfp, "alloc_pb failed\n");
            res = ENOMEM;
            if (topo && topo != &phy_topo)
                free(topo);
            break;
        }
        p->flags |= PB_MUL_TARGET;
        p->private = dup_addr(from); /* must free it later */
        if (!p->private) {
            LAGENT_LOG(logfp, "dup_addr failed, ignore this message now\n");
            res = ENOMEM;
            if (topo && topo != &phy_topo)
                free(topo);
            break;
        }
        p->match = msg->cmd;
        p->arg0 = get_ack_cmd(msg->msg_type); /* used for ACK CHKPT_MSG */
        p->arg2 = topo;                       /* the topo should not be freed
                                               * before rbcast succeed */
        p->arg3 = dup_msg(msg); /* must free it later */
        p->func = chkpt_cb;
        /* FIXME */
        if (gettimeofday(&tv, NULL) < 0) {
            LAGENT_LOG(logfp, "gettimeofday failed\n");
            exit(EXIT_FAILURE);
        }
        p->expire = tv.tv_sec + 10; /* this is the CHKPT rechecking timeout */
        insert_pb(p);
        /* Step 3: gather the relevant acks and resend the lost messages */
        /* Because of the rbcast(), step 3 is obsoleted */
        p->target = do_local_chkpt(topo, msg, p);
        /* Step 4: gather the results from all the children */
        /* Step 5: feedback to parent */
        if (type == CHKPT_MSG)
            res = 1;
        break;
    }
    case RSTRT_MSG:
        lagent_send_ack(MSG_CMD_RSTRT, msg->bref, from);
        if (type == RSTRT_MSG)
            res = 1;
        break;
    case CHKPT_REPLY_MSG:
        lagent_send_ack(MSG_CMD_CHKPT_REPLY, msg->bref, from);
        if (type == CHKPT_REPLY_MSG)
            res = 1;
        search_pb_list(msg, from);
        break;
    default:
        LAGENT_LOG(logfp, "invalid msg type %d\n", msg->msg_type);
    }

    if (type == ANY_MSG)
        res = 1;
    
    return res;
}

int handle_io_input(cmd_msg_t *msg, int type, int cmd, struct sockaddr *from,
                    handle_io_callback *cb)
{
    int res = 0, match = 0;

    if (cb) {
        switch (msg->msg_type) {
        case TOPO_MSG:
            if (type == TOPO_MSG)
                match = 1;
            break;
        case ACK_MSG:
            if (type == ACK_MSG && msg->cmd == cmd)
                match =1;
            break;
        case HB_MSG:
            if (type == HB_MSG)
                match = 1;
        case CHKPT_MSG:
            if (type == CHKPT_MSG)
                match = 1;
            break;
        case RSTRT_MSG:
            if (type == RSTRT_MSG)
                match = 1;
        default:
            LAGENT_LOG(logfp, "invalid msg type %d\n", msg->msg_type);
        }
        if (match || type == ANY_MSG)
            res = (*cb)(msg, type, cmd, from);
        else
            goto default_action;
    } else {
        /* default callback function */
    default_action:
        res = default_handle_io_cb(msg, type, cmd, from);
    }

    return res;
}

int lagent_gather(phy_topo_t *topo, int type, int cmd, time_t expire)
{
    struct sockaddr_in from;
    socklen_t addrlen = sizeof(from);
    struct timeval end;
    struct epoll_event ev[LAGENT_EPOLL_CHECK];
    char p[LAGENT_MAX_MSG_LEN];
    cmd_msg_t *ack = (cmd_msg_t *)p;
    int size, i, j, z;
    int nfds;

    switch (topo->type) {
    case phy_topo_flat:
        size = topo->flat.elem_num;
        for (i = 0; i < topo->flat.elem_num; i++) {
            topo->flat.elem[i].cbi[0] = 0;
        }
        break;
    case phy_topo_tree:
        size = topo->tree.elem_num;
        break;
    default:
        LAGENT_LOG(logfp, "invalid topo type\n");
        return 0;
    }
    if (gettimeofday(&end, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    do {
        z = 0;
        nfds = lagent_poll_wait(ev, 50);
        if (nfds < 0) {
            LAGENT_LOG(logfp, "poll wait failed\n");
            exit(EXIT_FAILURE);
        }
        LAGENT_DEBUG(logfp, "nfds is %d, z(%d) vs. size(%d), time leave %d\n", 
                     nfds, z, size, (int)(expire - end.tv_sec));
        for (i = 0; i < nfds; i++) {
            int fd = ev[i].data.fd;
            if (fd == lsock) {
                lagent_recv(fd, ack, (struct sockaddr *)&from, &addrlen);
                if (handle_io_input(ack, type, cmd, (struct sockaddr *)&from, NULL)) {
                    /* valid ack w/ FLAT */
                    for (j = 0; j < topo->flat.elem_num; j++) {
                        if (((struct sockaddr_in *)&from)->sin_addr.s_addr ==
                            topo->flat.elem[j].addr.sin_addr.s_addr) {
                            topo->flat.elem[j].cbi[0] = 1;
                            LAGENT_LOG(logfp, "find it at %d\n", j);
                            break;
                        }
                    }
                }
            } else {
                /* other sockets, such as unix sockets */
                int res;
                res = lagent_unix_read(fd);
                if (res < 0) {
                    LAGENT_LOG(logfp, "lagent_unix_read failed, ignore this message\n");
                }
            }
        }
        for (i = 0; i < size; i++) {
            z += topo->flat.elem[i].cbi[0];
        }
        if (gettimeofday(&end, NULL) < 0) {
            LAGENT_LOG(logfp, "gettimeofday failed\n");
            exit(EXIT_FAILURE);
        }
    } while (z < size && expire - end.tv_sec > 0);

    LAGENT_LOG(logfp, "total polled fds is %d, total is %d\n", z, size);
    return (z == size);
}

int lagent_poll_create(void)
{
    epfd = epoll_create(LAGENT_EPOLL_QUEUE_LEN);
    return epfd;
}

void lagent_poll_close(int epfd)
{
    close(epfd);
}

int lagent_poll_add(int fd)
{
    struct epoll_event ev;
    int res;

    if (fd < 0)
        return EINVAL;
    
    res = fcntl(fd, F_GETFL);
    if (res < 0) {
        LAGENT_LOG(logfp, "fcntl get flags failed %d\n", errno);
        goto out;
    }
    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);
    if (res < 0) {
        LAGENT_LOG(logfp, "fcntl set NONBLOCK failed %d\n", errno);
        goto out;
    }
    ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;
    
    res = epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
    if (res < 0) {
        LAGENT_LOG(logfp, "epoll add fd %d failed %d\n", fd, errno);
    }

out:
    return res;
}

int lagent_poll_del(int fd)
{
    struct epoll_event ev;
    int res;

    ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;

    res = epoll_ctl(epfd, EPOLL_CTL_DEL, fd, &ev);
    if (res < 0) {
        LAGENT_LOG(logfp, "epoll del fd %d failed %d\n", fd, errno);
    }

    return res;
}

int lagent_poll_wait(struct epoll_event *ev, int timeout)
{
    return epoll_wait(epfd, ev, LAGENT_EPOLL_CHECK, timeout);
}

