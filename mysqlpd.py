#encoding:utf8

import MYSQL_C

#mysql数据库连接池
class ConnectionPool:
	def __init__(self, free_max, begin_conn, user, passwd, db, host = "localhost", port = 0):
		self.pool = MYSQL_C.mysql_init(free_max, begin_conn, host, user, passwd, db, port)
	def connect(self):
		con = MYSQL_C.get_connection(self.pool)
		connection = Connection(con, self.pool)
		return connection

#单个mysql数据库连接
class Connection:
	def __init__(self, con, pool):
		self.con = con
		self.pool = pool
	#关闭连接
	def close(self):
		MYSQL_C.connect_close(self.con, self.pool)
	#无结果集的查询,只返回影响的行数
	def query(self, sql):
		ret = MYSQL_C.query(sql, len(sql), self.con)
		return ret
	def execute(self, sql):
		ret = Cursor(MYSQL_C.execute(sql, len(sql), self.con))
		return ret
	def fast_execute(self, sql):
		ret = Cursor(MYSQL_C.fast_execute(sql, len(sql), self.con))
		return ret
	def ping(self):
		return MYSQL_C.ping(self.con)

class Cursor:
	def __init__(self, cur):
		self.cur = cur
	def fetch_one(self):
		return MYSQL_C.fetch_one(self.cur)
	def fetch_all(self):
		return MYSQL_C.fetch_all(self.cur)
	def close(self):
		MYSQL_C.cursor_close(self.cur)