###########################################################################
# Copyright (c) 2007 National Research Center for Intelligent Computing
# Systems. ALL RIGHTS RESERVED. See "COPYRIGHT" for detail
#
# Release Under GPL v2
#
# Author: Ma Can <macan@ncic.ac.cn>
###########################################################################

CC = gcc
LD = gcc

# whether we should use SK mode ?
CFLAGS += -DSK=0

COMPILE_DATE = `date`
COMPILE_HOST = `hostname`
CFLAGS += -g -O2 -Wall -DCOMB_DEBUG=0 -pg -DCDATE="\"$(COMPILE_DATE)\"" \
			-DCHOST="\"$(COMPILE_HOST)\""
LFLAGS += 

HEADERS = lagent.h gctrl.h
OBJS = lagent.o topo_flat.o topo_tree.o parser.o net.o test.o chkpt.o comb.o launcher.o
TARGET = lagent
CFLAGS += -DTARGET="\"$(TARGET)\""

UOBJS = unix_sender.o
USENDER = unix_sender

GOBJS = gctrl.o
GCTRL = gctrl

LOCAL_INC_DIR += .

INSTALL_BIN = $(TARGET) $(USENDER) $(GCTRL)

all : $(TARGET) $(USENDER) $(GCTRL)

$(TARGET) : $(OBJS)
	@echo -e " " LD"\t" [$@] from [$(OBJS)]
	@$(CC) $(CFLAGS) -o $@ $(OBJS)

%.o : %.c $(HEADERS)
	@echo -e " " CC"\t" $@
	@$(CC) $(CFLAGS) -I$(LOCAL_INC_DIR) \
		-c $<

$(USENDER) : $(UOBJS)
	@echo -e " " LD"\t" [$@] from [$(UOBJS)]
	@$(CC) $(CFLAGS) -o $@ $(UOBJS)

$(GCTRL) : $(GOBJS)
	@echo -e " " LD"\t" [$@] from [$(GOBJS)]
	@$(CC) $(CFLAGS) -o $@ $(GOBJS)

run : $(TARGET)
	./$(TARGET)

install : $(TARGET)
	@cp $(TARGET) /usr/local/bin
	@cp phy_config /etc

rinstall : $(TARGET) $(USENDER) $(GCTRL)
	@scp $(INSTALL_BIN) root@glnode082:/usr/local/bin
	@scp $(INSTALL_BIN) root@glnode083:/usr/local/bin
	@scp $(INSTALL_BIN) root@glnode084:/usr/local/bin
	@scp $(INSTALL_BIN) root@glnode085:/usr/local/bin

kill:
	@ssh -x -q root@glnode082 killall $(TARGET)
	@ssh -x -q root@glnode083 killall $(TARGET)
	@ssh -x -q root@glnode084 killall $(TARGET)
	@ssh -x -q root@glnode085 killall $(TARGET)

clean : 
	@echo -e " " CLEAN"\t" $(INSTALL_BIN)
	@-rm -f *.o $(INSTALL_BIN)
