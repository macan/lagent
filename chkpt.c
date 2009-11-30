#include "lagent.h"

/**
 * check whether this CHKPT_MSG is valid
 */
int check_chkpt(cmd_msg_t *msg, struct sockaddr *from)
{
    persistent_block_t *p;

    if (msg->msg_type != CHKPT_MSG)
        return 0;
    for (p = pbl; p != NULL; p = p->next) {
        if (msg->cmd == p->match) {
            /* this CHKPT_MSG is confilct with one pending CHKPT_MSG */
            return 0;
        }
    }
    return 1;
}

/* NOTE that, topo could be NULL */
void chkpt_cb(persistent_block_t *pb, int isroot, char *root, phy_topo_t *topo,
              cmd_msg_t *msg)
{
    struct timeval tv;
    LAGENT_LOG(logfp, "CHKPT %d rechecking %d current %d ...\n", 
               pb->match, pb->target, pb->current);
    if (gettimeofday(&tv, NULL) < 0) {
        LAGENT_LOG(logfp, "gettimeofday failed\n");
        exit(EXIT_FAILURE);
    }
    if (pb->target <= pb->current) {
        /* task is done, we should return the message to the parent */
        struct sockaddr_in *from = (struct sockaddr_in *)pb->private;
        char __data[LAGENT_MAX_MSG_LEN];
        cmd_msg_t *reply = (cmd_msg_t *)__data;
        fw_addr_t *fw_addr = NULL;

        /* send the reply message */
        reply->msg_type = CHKPT_REPLY_MSG;
        reply->len = LAGENT_MAX_MSG_LEN;
        reply->cmd = msg->cmd;
        reply->bref = msg->bref;

        if (!is_my_child(inet_ntoa(from->sin_addr), &phy_topo, &fw_addr)) {
            LAGENT_LOG(logfp, "parent %s is NOT in my physical VIEW?\n",
                       inet_ntoa(from->sin_addr));
        } else {
            lagent_rsend(lsock, reply, fw_addr, lport);
        }
        
        pb->flags |= PB_DONE;
        LAGENT_LOG(logfp, "OK, checkpoint request %d has been done locally\n", 
                   pb->match);
    }
    pb->expire = tv.tv_sec + 10;
}

int __do_local_chkpt(int pid, int bref)
{
    /* Fork one process to do the execv */
    int res;
    char pid_str[16] = {0, };
    char other_args[256] = {0, };
    char *args[] = {"cr_checkpoint",
                    "-p",
                    pid_str,
                    other_args,
                    (char *)0
    };
    sprintf(pid_str, "%d", pid);
    sprintf(other_args, "-n %s -b %d", LAGENT_CHKPT_UNIX_PATH, bref);
    
    if ((res = fork()) < 0) {
        res = errno;
        LAGENT_LOG(logfp, "fork 'cr_restart' failed %d\n", errno);
    } else if (res == 0) {
        /* child process */

        /* FIXME: do execv here */
        exit(1);
    } else {
        /* parent process*/
        res = 0;
        LAGENT_LOG(logfp, "do execv '%s %s %s %s' now\n", 
                   args[0], args[1], args[2], args[3]);
    }

    return res;
}

/* do local checkpoint, the string format is "n:<node>,p:<pid>;..." */
int do_local_chkpt(phy_topo_t *topo, cmd_msg_t *msg, persistent_block_t *pb)
{
    /* copy the string first */
    char *node = NULL, *p, *str = msg->data;
    int pid, is_local = 0, all_chkpt = 0, res;
    char data[LAGENT_MAX_MSG_LEN] = {0,};

    strncpy(data, str, LAGENT_MAX_MSG_LEN);
    p = strtok(data, ":;,");
    if (!p || *p != 'C') {
        LAGENT_LOG(logfp, "match MAGIC failed\n");
        return -EINVAL;
    }
    while (1) {
        res = parse_chkpt_item(&node, &pid, &is_local);
        if (res) {
            LAGENT_LOG(logfp, "get one chkpt item failed or done, %d\n", all_chkpt);
            break;
        }
        if (is_local) {
            /* do the local checkpoint now */
            all_chkpt++;
            LAGENT_LOG(logfp, "do the local checkpoint pid %d ...\n", pid);
            res = __do_local_chkpt(pid, pb->bref);
            if (res) {
                LAGENT_LOG(logfp, "local_chkpt process %d failed\n", pid);
                /* we should tell the SENDER that the checkpoint failed */
                /* FIXME: send message to UNIX socket here */
            }
        } else {
            /* send to the child */
            fw_addr_t fw, *item = NULL;

            memset(&fw, 0, sizeof(fw));
            if (!inet_aton(node, &fw.addr.sin_addr)) {
                LAGENT_LOG(logfp, "inet_aton %s failed %d\n", node, errno);
                goto free_node;
            }
            if (is_my_child(node, topo, &item)) {
                all_chkpt++;
                lagent_rsend(lsock, msg, item, lport);
                if (pb->completes.alloc <= pb->completes.free) {
                    pb->completes.addr = realloc(pb->completes.addr, 
                                                 (pb->completes.free + 
                                                  DEFAULT_COMP_ITEM) * 
                                                 sizeof(fw_addr_t));
                    if (!pb->completes.addr) {
                        LAGENT_LOG(logfp, "realloc fw_addr failed\n");
                    } else
                        pb->completes.alloc += DEFAULT_COMP_ITEM;
                }
                if (pb->completes.alloc > pb->completes.free) {
                    /* save the fw_addr now */
                    pb->completes.addr[pb->completes.free] = *item;
                    pb->completes.addr[pb->completes.free++].cbi[1] = 0;
                }
            }
        }
    free_node:
        if (node) {
            free(node);
            node = NULL;
        }
    }
    return all_chkpt;
}
