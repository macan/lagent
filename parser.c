/**
 * Copyright (c) 2008 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * parser.c deal with the configration file
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

int match_root(parser_state_t *ps, char *line)
{
    int res = PARSER_OK;
    
    if (*ps != PARSER_INIT) {
        fprintf(stderr, "invalid parser state, %s need the %s state\n",
                __FUNCTION__, "PRSER_INIT");
        res = PARSER_FAILED;
        goto out;
    }

    if (line[0] == '\n' || (line[0] == '#' && line[1] != '!'))
        return res;

    if (line[0] != '#') {
        fprintf(stderr, "invalid configration file, need '#!root' leading\n");
        res = PARSER_FAILED;
        goto out;
    }
    if (line[1] == '!') {
        /* this maybe a command, test it */
        char *p = line + 2;
        char *cmd = strtok(p, " \t\n");
        if (!cmd)
            goto cmd_err;
        if (strcmp(cmd, "root") != 0)
            goto cmd_err;
        *ps = PARSER_EXPECT_ROOT;
        goto out;
    cmd_err:
        fprintf(stderr, "invalid command detected [%s]\n", cmd);
        res = PARSER_FAILED;
    }

    /* ignore there headers */
out:
    return res;
}

int get_root(parser_state_t *ps, char *line, char *out)
{
    int res = PARSER_OK;
    char *p = line;
    char *str = NULL;

    if (*ps != PARSER_EXPECT_ROOT) {
        fprintf(stderr, "invalid parser state, %s need the %s state\n",
                __FUNCTION__, "PRSER_EXPECT_ROOT");
        res = PARSER_FAILED;
        goto out;
    }

    if (line[0] == '\n' || (line[0] == '#' && line[1] != '!')) {
        res = PARSER_CONTINUE;
        return res;
    }
    if (line[0] == '#' && line[1] == '!') {
        fprintf(stderr, "invalid configration file, need 'node_name' here\n");
        res = PARSER_FAILED;
        goto out;
    }
    
    str = strtok(p, " \t\n");
    if (!str) {
        res = PARSER_FAILED;
        goto out;
    }
    if (isdigit(str[0])) {
        /* ip address */
        LAGENT_DEBUG(stdout, "> get root ip address: %s\n", str);
    } else if (isalpha(str[0])) {
        /* host name */
        LAGENT_DEBUG(stdout, "> get root host name : %s\n", str);
    } else {
        res = PARSER_FAILED;
        goto out;
    }
    
    *ps = PARSER_EXPECT_BLOCK;
    memcpy(out, str, strlen(str));
out:
    return res;
}

int match_block(parser_state_t *ps, char *line, block_t **block)
{
    int res = PARSER_OK;
    block_t *prev = NULL;

    if (*ps != PARSER_EXPECT_BLOCK) {
        fprintf(stderr, "invalid parser state, %s need the %s state\n",
                __FUNCTION__, "PARSER_EXPECT_BLOCK");
        res = PARSER_FAILED;
        goto out;
    }

    if (line[0] == '\n' || (line[0] == '#' && line[1] != '!'))
        return res;

    if (line[0] != '#') {
        fprintf(stderr, "invalid configration file, need '#!block' here\n");
        res = PARSER_FAILED;
        goto out;
    }
    
    if (line[1] == '!') {
        /* this maybe a valid command, test it */
        char *p = line + 2;
        char *cmd = strtok(p, " \t\n");
        res = PARSER_FAILED;
        if (!cmd)
            goto cmd_err;
        if (strcmp(cmd, "block") != 0)
            goto cmd_err;
        /* parse the block struct */
        cmd = strtok(NULL, " \t\n");
        if (!cmd)
            goto arg_err;
        if (!isdigit(cmd[0])) {
            fprintf(stderr, "invalid block number\n");
            goto cmd_err;
        }
        
        if (*block)
            prev = *block;
        *block = malloc(sizeof(block_t));
        if (!(*block)) {
            res = PARSER_FAILED;
            goto out;
        }
        memset((*block), 0, sizeof(block_t));
        if (prev)
            prev->next = *block;

        (*block)->bn = atoi(cmd);
        if (prev && (*block)->bn != prev->bn + 1) {
            printf("Warning, unsequential block number (%d) should be (%d) "
                   "at line %d\n",
                   (*block)->bn, prev->bn + 1, pln);
            (*block)->bn = prev->bn + 1;
        }
        LAGENT_DEBUG(stdout, "> get block index %d\n", (*block)->bn);
        
        *ps = PARSER_READING_BLOCK;
        res = PARSER_OK;
        goto out;
    cmd_err:
        fprintf(stderr, "invalid command detect [%s]\n", cmd);
        goto out;
    arg_err:
        fprintf(stderr, "invalid argument detect [%s]\n", cmd);
    }

out:
    return res;
}

int get_node(parser_state_t *ps, char *line, char *out)
{
    int res = PARSER_OK;
    char *p = line;
    char *str = NULL;

    if (*ps != PARSER_READING_BLOCK) {
        fprintf(stderr, "invalid parser state, %s need the %s state\n",
                __FUNCTION__, "PARSER_READING_BLOCK");
        res = PARSER_FAILED;
        goto out;
    }

    if (line[0] == '\n' || (line[0] == '#' && line[1] != '!')) {
        res = PARSER_CONTINUE;
        return res;
    }
    if (line[0] == '#' && line[1] == '!') {
        char *pt = strdup(line + 2);
        char *cmd = strtok(pt, " \t\n");
        if (!cmd)
            goto cmd_err;
        if (strcmp(cmd, "block") != 0)
            goto cmd_err;
        /* it is a new block cmd */
        *ps = PARSER_EXPECT_BLOCK;
        res = PARSER_NEED_RETRY;
        if (pt)
            free(pt);
        goto out;
    cmd_err:
        fprintf(stderr, "invalid configration file, need 'node_name' here\n");
        res = PARSER_FAILED;
        if (pt)
            free(pt);
        goto out;
    }
    
    str = strtok(p, " \t\n");
    if (!str) {
        res = PARSER_FAILED;
        goto out;
    }
    if (isdigit(str[0])) {
        /* ip address */
        LAGENT_DEBUG(stdout, "> get node ip address: %s\n", str);
    } else if (isalpha(str[0])) {
        /* host name */
        LAGENT_DEBUG(stdout, "> get node host name : %s\n", str);
    } else {
        res = PARSER_FAILED;
        goto out;
    }
    
    strncpy(out, str, MAX_LINE_LENGTH);
out:
    return res;
}

int parse_chkpt_item(char **node, int *pid, int *is_local)
{
    char *token;
    struct sockaddr_in addr;
    struct hostent *host;
    int res = EINVAL;

    token = strtok(NULL, ":");
    if (!token) {
        LAGENT_LOG(logfp, "try to match token 'n:' failed\n");
        goto out;
    }
    if (*token != 'n') {
        LAGENT_LOG(logfp, "invalid CHKPT item founded.\n");
        goto out;
    }
    token = strtok(NULL, ",");
    if (!token) {
        LAGENT_LOG(logfp, "strtok tokens failed\n");
        goto out;
    }
    host = gethostbyname(token);
    if (!host) {
        LAGENT_LOG(logfp, "gethostbyname (%s) failed\n", token);
        goto out;
    }
    addr.sin_addr.s_addr = *((unsigned long *)host->h_addr_list[0]);
    *node = strdup(inet_ntoa(addr.sin_addr));
    if (!(*node)) {
        LAGENT_LOG(logfp, "get item failed\n");
        goto out;
    }
    if (strcmp(self, *node) == 0) {
        /* match self, should do the local checkpoint */
        *is_local = 1;
    } else
        *is_local = 0;
    token = strtok(NULL, ":");
    if (!token) {
        LAGENT_LOG(logfp, "try to match token 'p:' failed\n");
        goto out_clean;
    }
    if (*token != 'p') {
        LAGENT_LOG(logfp, "try to match token 'p' failed\n");
        goto out_clean;
    }
    token = strtok(NULL, "; ");
    if (!token) {
        LAGENT_LOG(logfp, "try to match token 'pid' failed\n");
        goto out_clean;
    }
    if (!isdigit(*token)) {
        LAGENT_LOG(logfp, "invalid pid founded\n");
        goto out_clean;
    }
    *pid = atoi(token);
    res = 0;
out:
    return res;
out_clean:
    if (*node)
        free(*node);
    *node = NULL;
    goto out;
}

int parse_message_unix(char *str, int *pid, int *state, int *bref)
{
    char *p;

    p = strtok(str, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'pid' failed\n");
        return -EINVAL;
    }
    if (strcmp(p, "pid") != 0) {
        LAGENT_LOG(logfp, "expect 'pid' here\n");
        return -EINVAL;
    }
    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'int' failed\n");
        return -EINVAL;
    }
    /* this is the pid */
    if (!isdigit(*p)) {
        LAGENT_LOG(logfp, "expect 'int' here\n");
        return -EINVAL;
    }
    *pid = atoi(p);

    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'links' failed\n");
        return -EINVAL;
    }
    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'int' failed\n");
        return -EINVAL;
    }

    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'state' failed\n");
        return -EINVAL;
    }
    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'int' failed\n");
        return -EINVAL;
    }
    /* this is the state */
    if (!isdigit(*p)) {
        LAGENT_LOG(logfp, "expect 'int' here\n");
        return -EINVAL;
    }
    *state = atoi(p);
    
    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'bref' failed\n");
        return -EINVAL;
    }
    if (strcmp(p, "bref") != 0) {
        LAGENT_LOG(logfp, "expect 'bref' here\n");
        return -EINVAL;
    }
    p = strtok(NULL, ":;");
    if (!p) {
        LAGENT_LOG(logfp, "try to match token 'int' failed\n");
        return -EINVAL;
    }
    if (!isdigit(*p)) {
        LAGENT_LOG(logfp, "expect 'int' here\n");
        return -EINVAL;
    }
    *bref = atoi(p);

    return 0;
}
