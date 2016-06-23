EXES_MY = my
EXES_TEST = Fpm_Base
EXES_TEST2 = Fpm_Online_User
EXES_TEST3 = server
EXES_CLIENT3 = myclient
EXES_CLIENT4 = memcacheClient

CFLAGS +=
CXXFLAGS +=
CPPFLAGS += -g -I./../infra-fpnn/core -I./../infra-fpnn/base -I./../infra-fpnn/proto -I./../infra-fpnn/proto/msgpack -I./../infra-fpnn/proto/rapidjson
LIBS += -L./../infra-fpnn/core -L./../infra-fpnn/base -L./../infra-fpnn/proto

OBJS_TEST2 = Fpm_Online_User.o
OBJS_TEST = Fpm_Base.o
OBJS_TEST3 = server.o
OBJS_MY = my.o

all:  $(EXES_TEST3) 

clean:
	$(RM) *.o $(EXES_TEST) $(EXES_TEST2) $(EXES_TEST3) $(EXES_CLIENT3)
include ../infra-fpnn/def.mk
