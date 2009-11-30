#include "lagent.h"

int main(int argc, char *argv[])
{
    struct sockaddr_un addr = {.sun_family = AF_UNIX,};
    int chkpt_unix, rstrt_unix;

    chkpt_unix = socket(AF_UNIX, SOCK_DGRAM, AF_UNIX);
    if (chkpt_unix == -1) {
        fprintf(stderr, "create unix socket failed, %d\n", errno);
        goto out;
    }
    sprintf(addr.sun_path, "%s", LAGENT_CHKPT_UNIX_PATH);
    if (connect(chkpt_unix, &addr, sizeof(addr)) == -1) {
        fprintf(stderr, "connect to unix socket %s failed, %d\n", addr.sun_path, errno);
        goto out;
    }
    rstrt_unix = socket(AF_UNIX, SOCK_DGRAM, AF_UNIX);
    if (rstrt_unix == -1) {
        fprintf(stderr, "create unix socket failed, %d\n", errno);
        goto out;
    }
    sprintf(addr.sun_path, "%s", LAGENT_RSTRT_UNIX_PATH);
    if (connect(rstrt_unix, &addr, sizeof(addr)) == -1) {
        fprintf(stderr, "connect to unix socket %s failed, %d\n", addr.sun_path, errno);
        goto out;
    }

    while (1) {
        int bs, pid, links, state, bref;
        char data[256] = {0,};

        /* send ACK */
        fprintf(stdout, "INPUT pid > ");
        fscanf(stdin, "%d", &pid);

        fprintf(stdout, "\nINPUT bref > ");
        fscanf(stdin, "%d", &bref);
        fprintf(stdout, "\n");

        links = state = 0;
        memset(data, 0, 256);
        sprintf(data, "pid:%d;links:%d;state:%d;bref:%d", pid, links, state, bref);
        fprintf(stdout, "SEND data:\n%s\n", data);
        bs = send(chkpt_unix, data, strlen(data), 0);
        if (bs == -1) {
            fprintf(stderr, "send error with %d\n", errno);
        }
    }

out:
    return 0;
}
