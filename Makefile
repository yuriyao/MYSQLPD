MYSQL_C.so:mysql_c.c mysql_c.h
	gcc -fpic -shared -o MYSQL_C.so mysql_c.c -lmysqlclient