#include "mysql_c.h"

static PyObject* PyConnectPool_New(int free_max, char *host, char *user, char *passwd, 
	char *db, int port);

static PyObject* PyConnect_New(char *host, char *user, char *passwd, 
	char *db, int port);

static int
_ping(PyObject *con)
{
	return mysql_ping(&((PyConnectObject*)con)->mysql);
}

/**
 *  连接中断重连
 **/
static int
reconnect(PyObject *con)
{
	int i = 0;
	for(; i < RETRY_NUM; i ++)
	{
		if(!mysql_ping(&((PyConnectObject*)con)->mysql))
			return 0;
	}
	return -1;
}

/**
 *  检测并且重连
 *  如果错误是由于连接断开，则返回0或者-1
 *  其他原因导致的将不会重连，并且返回-2
 **/
 static int
 check_and_reconnect(PyObject *con)
 {
 	int i = mysql_errno(&((PyConnectObject*)con)->mysql);
 	if(i == CR_SERVER_LOST)
 	{
 		return reconnect(con);
 	}
 	return -2;
 }

/**
 *  @free_max 最多的空闲连接数
 *  @begin_conn 开始时打开的连接数
 **/
static PyConnectPoolObject*
init(int free_max, int begin_conn, char *host, char *user, char *passwd, 
	char *db, int port)
{
	PyConnectPoolObject *pool;
	PyConnectObject *con;
	int i = 0;
	//参数检查
	begin_conn = (begin_conn < 1) ? 1 : begin_conn;
	begin_conn = (begin_conn > BEGIN_CONN_MAX) ? BEGIN_CONN_MAX : begin_conn;
	//printf("1\n");
	//
	pool = (PyConnectPoolObject*)PyConnectPool_New(free_max, host, user, passwd, db, port);
	if(!pool)
	{
		return NULL;
	}
	//printf("2\n");
	for(i = 0; i < begin_conn; i ++)
	{
		con = (PyConnectObject*)PyConnect_New(pool->host, pool->user, pool->passwd, pool->db, pool->port);
		PyList_Insert(pool->cons_free, 0, (PyObject*)con);
	}
	//printf("3\n");
	return pool;
}


static PyObject*
PyConnectPool_New(int free_max, char *host, char *user, char *passwd, 
	char *db, int port)
{
	PyConnectPoolObject *res;
	int len;
	//参数预处理
	free_max = (free_max < 1) ? 1 : free_max;
	free_max = (free_max > FREE_MAX) ? FREE_MAX : free_max;
	port = (port < 0) ? 0 : port;
	//创建PyConnectPoolObject
	res = PyObject_New(PyConnectPoolObject, &PyConnectPool_Type);
	if(!res)
	{
		return PyErr_NoMemory();
	}
	//memset(res, 0, sizeof(PyConnectPoolObject));
	res->cons_free = PyList_New(0);
	res->cons_busy = PyList_New(0);
	if(!res->cons_free || !res->cons_busy)
	{
		Py_XDECREF(res->cons_free);
		Py_XDECREF(res->cons_busy);
		Py_DECREF(res);
		return PyErr_NoMemory();
	}
	if(host)
	{
		len = strlen(host);
		if(len < NAME_MAX)
			memcpy(res->host, host, len + 1);
	}
	if(user)
	{
		len = strlen(user);
		if(len < NAME_MAX)
			memcpy(res->user, user, len + 1);
	}
	if(passwd)
	{
		len = strlen(passwd);
		if(len < NAME_MAX)
			memcpy(res->passwd, passwd, len + 1);
	}
	if(db)
	{
		len = strlen(db);
		if(len < NAME_MAX)
			memcpy(res->db, db, len + 1);
	}
	res->port = port;
	return (PyObject*)res;
}

/**
 * 释放连接池的资源
 **/
static void
connectPool_dealloc(PyConnectPoolObject *v)
{
	//printf("connectPool_dealloc\n");
	int n;
	PyConnectObject *conn;
	PyListObject *conns;
	//参数检查
	if(!v)
		return;
	//关闭所有的连接
	conns = (PyListObject*)(v->cons_free);
	n = conns->ob_size;
	while(--n >= 0)
	{
		conn = (PyConnectObject*)PySequence_GetItem((PyObject*)conns, n);
		mysql_close(&conn->mysql);
	}
	conns->ob_type->tp_dealloc((PyObject*)conns);
	conns = (PyListObject*)(v->cons_busy);
	n = conns->ob_size;
	while(--n >= 0)
	{
		conn = PySequence_GetItem(conns, n);
		mysql_close(&conn->mysql);
	}
	conns->ob_type->tp_dealloc(conns);
	//释放连接池的结构体
	PyObject_Free(v);
}

static int
connectPool_print(PyConnectPoolObject *self, FILE *fp, int flags)
{
	fprintf(fp, "connect to %s in port %d", self->db, self->port);
	return 0;
}

static PyObject*
connectPool_repr(PyConnectPoolObject *v)
{
	//printf("connectPool_repr\n");
	char buf[64 + NAME_MAX];
	PyOS_snprintf(buf, sizeof(buf), "connect to %s in port %d with %d busy and %d free", 
		v->db, v->port, PySequence_Length(v->cons_busy), PySequence_Length(v->cons_free));
	return PyString_FromString(buf);
}

static long
connectPool_hash(PyConnectPoolObject *v)
{
	//printf("connectPool_hash\n");
	long x = (long)v;
	x &= 0x7FFFFFFF;
	if(x == -1)
		return -2;
	return x;
}

PyDoc_STRVAR(connectPool_doc,
	"The connect pool module object which is added by yuri yao.");

PyTypeObject PyConnectPool_Type = 
{
	PyObject_HEAD_INIT(&PyType_Type)
	0, /*ob_size*/
	"connectPool", /*tp_name*/
	sizeof(PyConnectPoolObject), /*tp_basicsize*/
	0,/*tp_itemsize*/
	connectPool_dealloc, /*tp_dealloc*/
	connectPool_print, /*tp_print*/
	0, /*tp_getattr*/
	0, /*tp_setattr*/
	0, /*tp_compare*/
	connectPool_repr,/*tp_repr*/
	0, /*tp_as_number*/
	0, /*tp_as_sequence*/
	0, /*tp_as_mapping*/
	connectPool_hash, /*tp_hash*/
	0, /*tp_call*/
	connectPool_repr, /*tp_str*/
	0, /*tp_getattro*/
	0, /*tp_setattro*/
	0, /*tp_as_buffer*/
	0, /*tp_flags*/
	connectPool_doc, /*tp_doc*/
	0, /*tp_traverse*/
	0, /*tp_clear*/
	0, /*tp_richcompare*/
	0, /*tp_weaklistoffset*/
	0, /*tp_iter*/
	0, /*tp_iternext*/
	0, /*tp_methods*/
	0, /*tp_members*/
	0, /*tp_getset*/
	&PyBaseObject_Type, /*tp_base*/
	0, /*tp_dict*/
	0, /*tp_descr_get*/
	0, /*tp_descr_set*/
	0, /*tp_dictoffset*/
	0, /*tp_init*/
	0, /*tp_alloc*/
	0, /*tp_new*/
	/*...*/
};

/*******************************************************************/

static PyObject*
PyConnect_New(char *host, char *user, char *passwd, 
	char *db, int port)
{
	PyConnectObject *res;
	res = PyObject_New(PyConnectObject, &PyConnect_Type);
	if(!res)
	{
		return PyErr_NoMemory();
	}
	mysql_init(&res->mysql);
	mysql_real_connect(&res->mysql, host, user, passwd, db, port, (const char*)0, 0);
	return (PyObject*)res;
}

static void
connect_dealloc(PyConnectObject *v)
{
	if(v)
	{
		mysql_close(&v->mysql);
		PyObject_Free(v);
	}
}

static int
connect_print(PyConnectObject *self, FILE *fp, int flags)
{
	fprintf(fp, "connect");
	return 0;
}

static long
connect_hash(PyConnectObject *v)
{
	long x = (long)v;
	x &= 0x7FFFFFFF;
	if(x == -1)
		x = -2;
	return x;
}

PyDoc_STRVAR(connect_doc, "connect"); 

static PyObject*
connect_repr(PyConnectObject *v)
{
	return PyString_FromString(connect_doc);
}

PyTypeObject PyConnect_Type = 
{
	PyObject_HEAD_INIT(&PyType_Type)
	0, /*ob_size*/
	"connect", /*tp_name*/
	sizeof(PyConnectPoolObject), /*tp_basicsize*/
	0,/*tp_itemsize*/
	connect_dealloc, /*tp_dealloc*/
	connect_print, /*tp_print*/
	0, /*tp_getattr*/
	0, /*tp_setattr*/
	0, /*tp_compare*/
	connect_repr,/*tp_repr*/
	0, /*tp_as_number*/
	0, /*tp_as_sequence*/
	0, /*tp_as_mapping*/
	connect_hash, /*tp_hash*/
	0, /*tp_call*/
	connect_repr, /*tp_str*/
	0, /*tp_getattro*/
	0, /*tp_setattro*/
	0, /*tp_as_buffer*/
	0, /*tp_flags*/
	connect_doc /*tp_doc*/
	/*...*/
};
/******************************游标******************************************************/
//用于cursor的缓冲池
PyCursorObject *cursor_free = (PyCursorObject*)0;
static int cursor_free_num = 0;

/**
 * 申请一个空闲的cursor
 * 首先从cursor的缓冲池进行寻找
 * 如果缓冲池里不为空，则获得一个;
 * 否则申请一个新的
 **/
static PyObject*
PyCursor_New()
{
	PyCursorObject *res;
	//缓冲池不为空
	if(cursor_free)
	{
		printf("Get from cursor free pool\n");
		//这边需要添加互斥量
		res = cursor_free;
		cursor_free = cursor_free->ob_type;
		cursor_free_num --;
		res->ob_type = &PyConnect_Type;
		if(res->res)
			mysql_free_result(res->res);
		Py_XDECREF(res->err_info);
	}
	//动态申请一个新的
	else
	{
		res = PyObject_New(PyCursorObject, &PyCursor_Type);
		if(!res)
		{
			return PyErr_NoMemory();
		}
	}
	//进行初始化
	res->res = (MYSQL_RES*)0;
	res->err_info = (PyStringObject*)0;
	return res;
}

static void
cursor_dealloc(PyCursorObject *v)
{
	//首先检查缓冲池里面的空闲游标数量
	//加互斥量
	if(cursor_free_num > CURSOR_FREE_MAX)
	{
		PyObject_Free((PyObject*)v);
	}
	else
	{
		v->ob_type = cursor_free;
		cursor_free = v;
		cursor_free_num ++;
	}
}

static int
cursor_print(PyCursorObject *self, FILE *fp, int flags)
{
	fprintf(fp, "cursor object\n");
	return 0;
}

static long
cursor_hash(PyCursorObject *v)
{
	long res;
	res = (long)v;
	res &= 0x7FFFFFFF;
	if(res == -1)
		res = -2;
	return res;
}

static PyObject*
cursor_repr(PyCursorObject *v)
{
	return (PyObject*)PyString_FromString("cursor");
}

static PyObject*
cursor_str(PyCursorObject *v)
{
	return (PyObject*)PyString_FromString("cursor");
}

PyDoc_STRVAR(cursor_doc, "");

//游标类的类型
PyTypeObject PyCursor_Type = 
{
	PyObject_HEAD_INIT(&PyType_Type)
	0, /*ob_size*/
	"cursor", /*tp_name*/
	sizeof(PyCursorObject), /*tp_basicsize*/
	0,/*tp_itemsize*/
	cursor_dealloc, /*tp_dealloc*/
	cursor_print, /*tp_print*/
	0, /*tp_getattr*/
	0, /*tp_setattr*/
	0, /*tp_compare*/
	cursor_repr,/*tp_repr*/
	0, /*tp_as_number*/
	0, /*tp_as_sequence*/
	0, /*tp_as_mapping*/
	cursor_hash, /*tp_hash*/
	0, /*tp_call*/
	cursor_repr, /*tp_str*/
	0, /*tp_getattro*/
	0, /*tp_setattro*/
	0, /*tp_as_buffer*/
	0, /*tp_flags*/
	cursor_doc, /*tp_doc*/
	0, /*tp_traverse*/
	0, /*tp_clear*/
	0, /*tp_richcompare*/
	0, /*tp_weaklistoffset*/
	0, /*tp_iter*/
	0, /*tp_iternext*/
	0, /*tp_methods*/
	0, /*tp_members*/
	0, /*tp_getset*/
	&PyBaseObject_Type, /*tp_base*/
	0, /*tp_dict*/
	0, /*tp_descr_get*/
	0, /*tp_descr_set*/
	0, /*tp_dictoffset*/
	0, /*tp_init*/
	0, /*tp_alloc*/
	0, /*tp_new*/
	/*...*/
};

/***********************************python封装*******************************************/
static PyObject*
wrap_init(PyObject *self, PyObject *args)
{
	int free_max;
	int begin_conn;
	char *host, *user, *passwd, *db;
	int port;
	PyObject *res;

	PyArg_ParseTuple(args, "iissssi", &free_max, &begin_conn, &host, &user, &passwd, &db, &port);
	//printf("%d %d %s %s %s %s %d", free_max, begin_conn, host, user, passwd, db, port);
	res = (PyObject*)init(free_max, begin_conn, host, user, passwd, db, port);
	//Py_DECREF(res);
	//Py_DECREF(res);
	//Py_DECREF(res);
	//Py_DECREF(res);
	//printf("%p\n", res);
	//PyObject *str = connectPool_repr(res);
	//str->ob_type->tp_print(str, stdout, 0);
	return res;
}


/**
 *  从连接池中获得一个连接
 *	args的第一个参数是一个连接池对象
 **/
static PyObject*
getconnection(PyObject *self, PyObject *args)
{
	PyConnectPoolObject *pool = PySequence_GetItem(args, 0);
	PyListObject *conns;
	PyConnectObject *con;
	if(!PyConnectPool_CheckExact(pool))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数类型不是connectPool");
		return NULL;
	}
	conns = pool->cons_free;
	//空闲缓冲池里面可以获得到空闲的连接
	if(PySequence_Length(conns))
	{
		con = (PyConnectObject*)PySequence_GetItem(conns, 0);
		//删除第一个元素
		PyList_SetSlice(conns, 0, 1, NULL);
	}
	else
	{
		con = (PyConnectObject*)PyConnect_New(pool->host, pool->user, pool->passwd, pool->db, pool->port);
	}
	if(con == NULL)
	{
		return PyErr_NoMemory();
	}
	conns = pool->cons_busy;
	PyList_Insert((PyObject*)conns, 0, (PyObject*)con);
	return (PyObject*)con;
}

/**
 *  关闭连接
 *  args[0] 连接对象
 *  args[1] 连接池对象
 **/
static PyObject*
connect_close(PyObject *self, PyObject *args)
{
	PyConnectPoolObject *pool = (PyConnectPoolObject*)PySequence_GetItem(args, 1);
	PyConnectObject *con = (PyConnectObject*)PySequence_GetItem(args, 0);
	int index;
	if(!pool || !con || !PyConnectPool_CheckExact(pool) || !PyConnect_CheckExact(con))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数错误");
		return NULL;
	}
	//从busy连接队列中删除掉
	index = PySequence_Index(pool->cons_busy, (PyObject*)con);
	if(index != -1)
		PySequence_DelItem(pool->cons_busy, index);
	//检查空闲的连接池数量
	//如果空闲的连接过多，则关闭多余的连接
	if(PySequence_Length(pool->cons_free) > pool->free_max)
	{
		connect_dealloc(con);
	}
	else
	{
		PyList_Insert((PyObject*)(pool->cons_free), 0, (PyObject*)con);
	}
	return Py_True;
}

/**
 * 进行sql的执行
 * args[0] sql语句
 * args[1] sql语句的长度
 * args[2] 连接对象
 * 返回影响的行数
 **/
static PyObject*
query(PyObject *self, PyObject *args)
{
	char *sql;
	int len = 0;
	PyConnectObject *con;
	int err;
	//PyObject_Print(args, stdout, 0);
	//解析出参数
	if(!PyArg_ParseTuple(args, "siO", &sql, &len, &con))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数不合法");
		return NULL;
	}
	//验证第三个是否是连接对象
	if(!con || !PyConnect_CheckExact(con))
	{
		PyErr_SetString(PyExc_RuntimeError, "请检查参数的合法性");
	}

	//进行查询
	if(mysql_real_query(&con->mysql, sql, len))
	{
		//抛出异常
		printf("%s\n", mysql_error(&con->mysql));
#if AUTO_RECONNECT
		err = check_and_reconnect((PyObject*)con);
		//连接中断，并重连
		if(!err)
		{
			return query(self, args);
		}
		//无法断线重连
		else if(err == -1)
		{
			return NULL;
		}
#endif
		return PyInt_FromLong(-1);
	}
	return PyInt_FromLong(mysql_affected_rows(&con->mysql));
}

/**
 *  进行sql的执行，并返回执行结果集
 *	args[0] sql语句
 *	args[1] sql语句的长度
 *	args[2] 连接对象
 *  返回一个游标对象
 **/
static PyObject*
execute(PyObject *self, PyObject *args)
{
	char *sql;
	int len;
	PyConnectObject *con;
	PyCursorObject *cursor;
	int err;
	//PyObject_Print(args, stdout, 0);
	if(!PyArg_ParseTuple(args, "siO", &sql, &len, &con))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数不合法");
		return NULL;
	}
	//验证第三个是否是连接对象
	if(!con || !PyConnect_CheckExact(con))
	{
		PyErr_SetString(PyExc_RuntimeError, "请检查参数的合法性");
		return NULL;
	}

	//进行查询
	if(mysql_real_query(&con->mysql, sql, len))
	{
		//抛出异常
		printf("%s\n", mysql_error(&con->mysql));
#if AUTO_RECONNECT
		printf("Reconnecting...\n");
		err = check_and_reconnect((PyObject*)con);
		//连接中断，并重连
		if(!err)
		{
			return execute(self, args);
		}
		//无法断线重连
		else if(err == -1)
		{
			return NULL;
		}
#endif
		return NULL;
	}
	//申请一个游标对象
	cursor = (PyCursorObject*)PyCursor_New();
	if(cursor == NULL)
	{
		printf("No memery\n");
		return PyErr_NoMemory();
	}
	//存储结果集,这是一种慢速的存储过程，但是是可以每个连接可以有多个游标对象
	cursor->res = mysql_store_result(&con->mysql);
	return (PyObject*)cursor;
}

/**
 *  进行sql的执行，并返回执行结果集
 *  (速度和效率比较高，但一个连接只能同时存在一个这样的结果集)
 *	args[0] sql语句
 *	args[1] sql语句的长度
 *	args[2] 连接对象
 *  返回一个游标对象
 **/
static PyObject*
fast_execute(PyObject *self, PyObject *args)
{
	char *sql;
	int len;
	PyConnectObject *con;
	PyCursorObject *cursor;
	int err;
	//PyObject_Print(args, stdout, 0);
	if(!PyArg_ParseTuple(args, "siO", &sql, &len, &con))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数不合法");
		return NULL;
	}
	//验证第三个是否是连接对象
	if(!con || !PyConnect_CheckExact(con))
	{
		PyErr_SetString(PyExc_RuntimeError, "请检查参数的合法性");
	}

	//进行查询
	if(mysql_real_query(&con->mysql, sql, len))
	{
		//抛出异常
		printf("%s\n", mysql_error(&con->mysql));

#if AUTO_RECONNECT
		err = check_and_reconnect((PyObject*)con);
		//连接中断，并重连
		if(!err)
		{
			return fast_execute(self, args);
		}
		//无法断线重连
		else if(err == -1)
		{
			return NULL;
		}
#endif
		return Py_None;
	}
	//申请一个游标对象
	cursor = (PyCursorObject*)PyCursor_New();
	if(cursor == NULL)
		return PyErr_NoMemory();
	//存储结果集,这是一种慢速的存储过程，但是是可以每个连接可以有多个游标对象
	cursor->res = mysql_use_result(&con->mysql);
	return (PyObject*)cursor;
}

/**
 * 关闭游标对象
 * args[0] 是一个游标对象
 * 返回一个数字，恒为0
 **/
static PyObject*
cursor_close(PyObject *self, PyObject *args)
{
	PyCursorObject *cursor;
	cursor = (PyCursorObject*)PySequence_GetItem(args, 0);
	//检测参数的合法性
	if(!cursor || !PyCursor_CheckExact(cursor))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数不合法");
		return NULL;
	}
	//关闭游标
	mysql_free_result(cursor->res);
	cursor->res = (MYSQL_RES*)0;
	cursor_dealloc(cursor);
	return PyInt_FromLong(0);
}

/**
 * 将结果集的一项转变为相应类型的对象
 **/
static PyObject*
wrap_to_Object(char* cell, int type, int len)
{
	PyObject *res;
	PyObject *v;
	switch(type)
	{
	//文本或者字符串
	case FIELD_TYPE_BLOB:
	case FIELD_TYPE_LONG_BLOB:
	case FIELD_TYPE_MEDIUM_BLOB:
	case FIELD_TYPE_TINY_BLOB:
	case FIELD_TYPE_STRING:
	case FIELD_TYPE_VAR_STRING:
		res = PyString_FromStringAndSize(cell, len);
		break;
	//整数对象
	case FIELD_TYPE_ENUM:
	case FIELD_TYPE_INT24:
	case FIELD_TYPE_LONG:
	case FIELD_TYPE_LONGLONG:
	case FIELD_TYPE_SHORT:
	case FIELD_TYPE_TINY:
		res = PyInt_FromString(cell, NULL, 10);
		break;
	//浮点数
	case FIELD_TYPE_DECIMAL:
	case FIELD_TYPE_DOUBLE:
	case FIELD_TYPE_FLOAT:
		v = PyString_FromStringAndSize(cell, len);
		res = PyFloat_FromString(v, NULL);
		Py_XDECREF(v);
		break;
	//时间类型
	case FIELD_TYPE_DATE:
	case FIELD_TYPE_DATETIME:
	case FIELD_TYPE_NEWDATE:
	case FIELD_TYPE_TIME:
	case FIELD_TYPE_TIMESTAMP:
		res = PyString_FromStringAndSize(cell, len);
		break;
	//空值
	case FIELD_TYPE_NULL:
		res = Py_None;
		Py_INCREF(res);
		break;
	//set类型
	case FIELD_TYPE_SET:
		res = Py_None;
		Py_INCREF(res);
		break;
	case FIELD_TYPE_YEAR:
		res = Py_None;
		Py_INCREF(res);
		break;
	default:
		Py_INCREF(Py_None);
		return Py_None;
	}
	
	return res;
}

/**
 * 从结果集获得一行结果
 * args[0] 游标对象
 * 返回一个列表对象
 **/
static PyObject*
fetch_one(PyObject *self, PyObject *args)
{	
	PyCursorObject* cursor;
	int len;
	unsigned long *row_lens;
	PyListObject *res;
	MYSQL_ROW row;
	MYSQL_FIELD *field;
	PyObject *v;
	int i = 0;
	//PyObject_Print(args, stdout, 0);
	cursor = (PyCursorObject*)PySequence_GetItem(args, 0);
	if(!cursor || !PyCursor_CheckExact(cursor))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数不合法");
		return NULL;
	}
	row = mysql_fetch_row(cursor->res);
	//printf("1\n");4
	if(!row)
	{
		return Py_None;
	}
	//printf("2\n");
	//获得结果集的列数
	len = (int)mysql_num_fields(cursor->res);
	//printf("3\n");
	//获得该列的每个数据项的长度
	row_lens = mysql_fetch_lengths(cursor->res);
	//获得列的所有信息
	field = mysql_fetch_fields(cursor->res);
	res = (PyListObject*)PyList_New(len);
	if(!res)
	{
		return PyErr_NoMemory();
	}
	//printf("4\n");
	for(i = 0; i < len; i ++)
	{
		//printf("%s %d %d\n", row[i], field[i].type, row_lens[i]);
		v = wrap_to_Object(row[i], field[i].type, row_lens[i]);
		if(!v)
		{
			return PyErr_NoMemory();
		}
		//PyObject_Print(v, stdout, 0);
		//printf("\n");
		PyList_SetItem((PyObject*)res, i, v);
		//printf("9%d\n", i);
	}
	//printf("5\n");
	return (PyObject*)res;
}

/**
 * 从结果集获得所有结果
 * args[0] 游标对象
 * 返回一个列表对象
 **/
static PyObject*
fetch_all(PyObject *self, PyObject *args)
{
	PyListObject *res;
	PyObject *v;
	int i = 0;
	res = (PyListObject*)PyList_New(0);
	if(res == NULL)
		return PyErr_NoMemory();
	while((v = fetch_one(self, args)) != NULL && v != Py_None)
	{
		//PyObject_Print(v, stdout, 0);
		PyList_Insert((PyObject*)res, i, v);
		i ++;
	}
	if(PySequence_Length((PyObject*)res) == 0)
	{
		Py_XDECREF(res);
		return Py_None;
	}
	return (PyObject*)res;
}

/**
 *  检查连接是否中断
 *  args[0] 连接对象
 **/
static PyObject*
ping(PyObject *self, PyObject *args)
{
	PyObject *con = PySequence_GetItem(args, 0);
	int ret;
	if(!con || !PyConnect_CheckExact(con))
	{
		PyErr_SetString(PyExc_RuntimeError, "参数错误");
		return NULL;
	}
	ret = mysql_ping(&((PyConnectObject*)con)->mysql);
	if(!ret)
		return Py_True;
	return Py_False;
}

static PyMethodDef 
methods[] = {
	{"mysql_init", wrap_init, METH_VARARGS, ""},
	{"get_connection", getconnection, METH_VARARGS, ""},
	{"connect_close", connect_close, METH_VARARGS, ""},
	{"query", query, METH_VARARGS, ""},
	{"execute", execute, METH_VARARGS, ""},
	{"fast_execute", fast_execute, METH_VARARGS, ""},
	{"cursor_close", cursor_close, METH_VARARGS, ""},
	{"fetch_one", fetch_one, METH_VARARGS, ""},
	{"fetch_all", fetch_all, METH_VARARGS, ""},
	{"ping", ping, METH_VARARGS, ""},
	{NULL, NULL, 0, NULL}

};

PyMODINIT_FUNC
initMYSQL_C(void)
{
	Py_InitModule("MYSQL_C", methods);
}