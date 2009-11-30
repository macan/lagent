#include "lagent.h"

void tb_cb(persistent_block_t *pb, int isroot, char *root, phy_topo_t *topo, 
           cmd_msg_t *msg)
{
    /* rsend a CHKPT message; until receive the ack, the
     * persistent_block can be released */
    int i;

    if (isroot) {
        LAGENT_LOG(logfp, "test send some messages: start\n");
        msg->msg_type = CHKPT_MSG;
        msg->len = LAGENT_MAX_MSG_LEN;
        msg->cmd = 1;           /* should greater than 0 */
        sprintf(msg->data, "C;n:glnode082,p:1234;n:glnode083,p:2345;"
                "n:glnode084,p:3456;n:glnode085,p:4567");
        if (topo->type == phy_topo_flat) {
            for (i = 0; i < topo->flat.elem_num; i++) {
                lagent_rsend(lsock, msg, &topo->flat.elem[i], lport);
            }
        } else if (topo->type == phy_topo_tree) {
        }
        LAGENT_LOG(logfp, "test send some messages: done\n");
    }
    if (pb->arg2 != &phy_topo)
        free(pb->arg2);
    pb->flags |= PB_DONE;
}
