#include <python2.6/Python.h>
#include <mysql/mysql.h>
#include <mysql/errmsg.h>

#ifndef MYSQL_C_H
#define MYSQL_C_H
//申明数据库连接的PyObject子类
typedef struct _connect 
{
	PyObject_HEAD
	MYSQL mysql;
}PyConnectObject;

//数据库结果集的句柄
typedef struct _cursor
{
	PyObject_HEAD
	//数据的存储集
	MYSQL_RES *res;
	//错误信息
	PyStringObject *err_info;
	
}PyCursorObject;

#ifndef NAME_MAX
#define NAME_MAX 256
#endif

//cursor缓冲池里维护的最多的空闲游标数
#ifndef CURSOR_FREE_MAX
#define CURSOR_FREE_MAX 20
#endif

//申明存储数据库连接池的PyObject子类
typedef struct _connectPool
{
	PyObject_HEAD
	//用来存储数据库的可用连接
	PyObject *cons_free;
	//被占用的数据库连接
	PyObject *cons_busy;
	//最多的空闲连接数量
	int free_max;
	//
	char host[NAME_MAX];
	//
	char user[NAME_MAX];
	//
	char passwd[NAME_MAX];
	//
	char db[NAME_MAX];
	//
	int port;
}PyConnectPoolObject;

#define FREE_MAX 100

#define BEGIN_CONN_MAX 100

//连接断开重新连接的次数
#define RETRY_NUM 3

//是否自动进行断线重连
#define AUTO_RECONNECT 1

#define PyConnectPool_CheckExact(v)\
	(v->ob_type == &PyConnectPool_Type)

#define PyConnect_CheckExact(v)\
	(v->ob_type == &PyConnect_Type)

#define PyCursor_CheckExact(v)\
	(v->ob_type == &PyCursor_Type)

extern PyTypeObject PyConnectPool_Type;
extern PyTypeObject PyConnect_Type;
extern PyTypeObject PyCursor_Type;

#endif


