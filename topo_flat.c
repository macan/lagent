/**
 * Copyright (c) 2008 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * topo_flat.c defines the FLAT topology
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

int parse_topo_flat(FILE *file, phy_topo_t *topo)
{
    int res = 0;
    int i, j;
    block_t *b, *n, *bhead;
    struct hostent *host;
    
    if (topo->type != phy_topo_interim) {
        LAGENT_ERR(stderr, "you need to call parse_topo_block first\n");
        res = EINVAL;
        goto out;
    }

    bhead = topo->bhead;
    memset(&topo->flat, 0, sizeof(struct topo_flat));
    for (b = bhead; b != NULL; b = b->next) {
        topo->flat.elem_num += b->elem_num;
        LAGENT_DEBUG(stdout, "B%4d\talloc %4d\telem %4d\n", 
                     b->bn, b->alloc_num, b->elem_num);
        for (i = 0; i < b->elem_num; i++) {
            LAGENT_DEBUG(stdout, "%s\n", b->elem[i].node);
        }
    }

    if (topo->flat.elem_num) {
        topo->flat.elem = malloc(topo->flat.elem_num * sizeof(fw_addr_t));
        memset(topo->flat.elem, 0, topo->flat.elem_num * sizeof(fw_addr_t));
        if (!topo->flat.elem) {
            LAGENT_ERR(stderr, "alloc memory failed\n");
            res = ENOMEM;
            goto clean_out;
        }
    }

    int ex_cnt = 0, ft_cnt = 0;
    for (i = 0, b = bhead; b != NULL; b = b->next) {
        for (j = 0; j < b->elem_num; j++) {
            host = gethostbyname(b->elem[j].node);
            if (!host) {
                LAGENT_ERR(stderr, "gethostbyname (%s) failed\n",
                           b->elem[j].node);
                res = EINVAL;
                goto clean_out;
            }
            topo->flat.elem[i].addr.sin_family = AF_INET;
            topo->flat.elem[i].addr.sin_addr.s_addr = 
                *((unsigned long *)host->h_addr_list[0]);
            if (lagent_checking_excludes(&(topo->flat.elem[i]))) {
                ex_cnt++;
            } else if (lagent_filter(global_filter)) {
                ft_cnt++;
            } else {
                topo->flat.elem[i].name = strdup(b->elem[j].node);
                i++;
            }
#if 0
            LAGENT_DEBUG(stdout, "ip address is : %s\n", 
                         inet_ntoa(topo->flat.elem[i].addr.sin_addr));
#endif
        }
    }
    
    topo->flat.elem_num -= ex_cnt;
    topo->flat.elem_num -= ft_cnt;
    topo->type = phy_topo_flat;
    
clean_out:
    for (b = bhead; b != NULL; b = n) {
        n = b->next;
        if (b->elem)
            free(b->elem);
        free(b);
    }
    
out:
    return res;
}    

void dump_nodes_from_topo_flat(FILE *file, phy_topo_t *topo)
{
    int i;
    for (i = 0; i < topo->flat.elem_num; i++) {
        fprintf(file, "%s\n", topo->flat.elem[i].name);
    }
}

