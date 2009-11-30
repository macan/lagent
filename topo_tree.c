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

int parse_topo_tree(FILE *file, phy_topo_t *topo)
{
    int res = 0;
    block_t *b;
    int i;

    if (topo->type != phy_topo_interim) {
        LAGENT_ERR(stderr, "you need to call parse_topo_block first\n");
        res = EINVAL;
        goto out;
    }

    for (b = topo->bhead; b != NULL; b = b->next) {
        topo->flat.elem_num += b->elem_num;
        LAGENT_DEBUG(stdout, "B%4d\talloc %4d\telem %4d\n", 
                     b->bn, b->alloc_num, b->elem_num);
        for (i = 0; i < b->elem_num; i++) {
            LAGENT_DEBUG(stdout, "%s\n", b->elem[i].node);
        }
    }
    
out:
    return res;
}
