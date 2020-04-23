#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fileencoding=utf-8


#首先检查合表是否可用，如果不可用，发送邮件报警，退出程序
#如果和表可用，检测是否有新表生成，新表的engine为innodb，使用该功能进行检测
#如果没有新表生成，退出程序
#如果有新表生成开始进行一系列合表操作。

import pymysql as mysqldb
import os 
import time

#打开数据库连接
db = mysqldb.connect("127.0.0.1","root","easytech","easyweb")
#得到一个可以执行SQL语句的游标对象
cursor = db.cursor()
	

#创建检测合表可用性函数，如果和表不能查询，则发送邮件报警
def check_union_available(union_table_name,union_table_shcema):
	sql_check_available = "select * from "+union_table_shcema+"."+union_table_name+" limit 1;"
	print (sql_check_available)
	try:
		cursor.execute(sql_check_available)
		res = '1'
	except:
		#发送报警邮件/send warning email.
		com="/usr/bin/sendEmail -o tls=no -f yunweisend@jiandan100.cn -s 192.168.1.250 -u \' table "+union_table_name+" on mysql-server 181 is anavailable \' -xu yunweisend@jiandan100.cn -xp \"YWEasytech456\" -t tianzhaofeng@jiandan100.cn -m \" do the union_table manually  To troubleshoot the problem!!!\""
		print (com)
		os.system(com)
		res ='0'
	return res

#定义检测是否有新的分表生成函数
def check_new_table(union_table,union_table_shcema):
	#定义SQL语句,检测是否有新表生成，
	
	sql_get_name = "select table_name from information_schema.tables \
	where engine ='InnoDB' and table_name like '"+union_table+"%' \
	and table_schema ='"+union_table_shcema+"'"
	
	#执行SQL
	cursor.execute(sql_get_name)
	
	#使用fetchone获取一条数据
	table_name_tump = cursor.fetchone()

	#将tumple 转换为str
	global table_name
	table_name = "" 
	try:
		table_name = "".join(table_name_tump[0])
	except:
		print ("no table need to be unioned on "+union_table_shcema)

#需要将表engine改为MyISAM
def change_engine(union_table_shcema):

	#生成改engine的SQL
	sql_change_engine = "alter table "+union_table_shcema+"."+table_name+" engine = MyISAM;"

	cursor.execute(sql_change_engine)

	print(sql_change_engine)

#181中W_UserInteractionData表需要调整索引顺序，W_ListenRec需要创建一个新的索引
#根据两个表所属数据库的不同进行不同操作，
def change_table_stru(union_table_shcema):
	if union_table_shcema == "easyweb_bigdata":
		print (union_table_shcema)
		sql_drop_index = "drop index `idx_userid` on "+union_table_shcema+"."+table_name+";"
		sql_add_index = "alter table "+union_table_shcema+"."+table_name+" add index `idx_userid` (`userid`);"
	
		cursor.execute(sql_drop_index)
		cursor.execute(sql_add_index)
		print ("table structure changed!")
	else:
		print (union_table_shcema)
		#增加索引KEY `idx_starttime` (`starttime`)
		sql_add_index = "alter table "+union_table_shcema+"."+table_name+" add index `idx_starttime` (`starttime`);"
		cursor.execute(sql_add_index)
		print ("table structure changed!")

#更新合表表结构
def update_table_stru(union_table,union_table_shcema):
	#获取原表结构
	cursor.execute("show create table "+union_table_shcema+"."+union_table+";")

	table_stru = cursor.fetchone()

	print("".join(table_stru[1]))
	table_stru = "".join(table_stru[1])

	#将原合表删除SQL
	sql_delete_table = "drop table "+union_table_shcema+"."+union_table+";"
	cursor.execute(sql_delete_table)
	print(sql_delete_table)
	
	#新建表SQL
	cursor.execute("use "+union_table_shcema)
	
	sql_create_table= table_stru[:-1]+",`"+table_name+"`);"
	print (sql_create_table)
	cursor.execute(sql_create_table)

#创建合表函数对W_ListenRec的分表进行check及合表
def main_listen():
	union_table="W_ListenRec"
	union_table_schema="easyweb"
	#任务逻辑，如果有新表建立，则执行之后的操作，否则退出。
	#打开数据库连接
	db = mysqldb.connect("127.0.0.1","root","easytech","easyweb")
	#得到一个可以执行SQL语句的游标对象
	cursor = db.cursor()
	
	#检测合表是否可用：如果可用函数返回值为'1'，不可用返回值为‘0’
	avai = check_union_available(union_table,union_table_schema)
	#如果合表可用,检测是否有新表创建	
	if avai == '1':
	#方法table_name.strip()将判断table_name字符串是否为空，如果非空，则执行和表的函数	 
#	if table_name.strip() and check_union_available(union_table,union_table_schema)=='1':
		check_new_table(union_table,union_table_schema)
		if table_name.strip():
			print ("begin union W_ListenRec")
			change_engine(union_table_schema)
			change_table_stru(union_table_schema)
			update_table_stru(union_table,union_table_schema)
			res = "union_table W_ListenRec succeed."
			com="/usr/bin/sendEmail -o tls=no -f yunweisend@jiandan100.cn -s 192.168.1.250 -u \' table "+table_name+" on mysql-server 181 is uninoned \' -xu yunweisend@jiandan100.cn -xp \"YWEasytech456\" -t tianzhaofeng@jiandan100.cn -m \" union table succeed!!!\""
			os.system(com)
		else:
			res = "nothind to do for W_ListenRec."
	#如果合表不可用，直接返回合表不可用
	else:
		res = "W_ListenRec is unavailable"
	return res,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

#创建合表函数对W_UserInteractionData的分表进行check及合表
def main_interaction():
	union_table = "W_UserInteractionData"
	union_table_schema = "easyweb_bigdata"
	#打开数据库连接
	db = mysqldb.connect("127.0.0.1","root","easytech","easyweb_bigdata")
	#得到一个可以执行SQL语句的游标对象
	cursor = db.cursor()

	#检测合表是否可用：如果可用函数返回值为'1'，不可用返回值为‘0’
	avai = check_union_available(union_table,union_table_schema)
	#如果合表可用,检测是否有新表创建	
	if avai == '1':
	#方法table_name.strip()将判断table_name字符串是否为空，如果非空，则执行和表的函数	 
#	if table_name.strip() and check_union_available(union_table,union_table_schema)=='1':
		check_new_table(union_table,union_table_schema)
		if table_name.strip():
			print ("begin union W_UserInteractionData")
			change_engine(union_table_schema)
			change_table_stru(union_table_schema)
			update_table_stru(union_table,union_table_schema)
			res = "union_table W_UserInteractionData succeed."

			#发送报警邮件/send warning email.
			com="/usr/bin/sendEmail -o tls=no -f yunweisend@jiandan100.cn -s 192.168.1.250 -u \' table "+table_name+" on mysql-server 181 is uninoned \' -xu yunweisend@jiandan100.cn -xp \"YWEasytech456\" -t tianzhaofeng@jiandan100.cn -m \" union table succeed!!!\""
			os.system(com)
		else:
			res = "nothind to do for W_UserInteractionData."
	#如果合表不可用，直接返回合表不可用
	else:
		res = "W_UserInteractionData is unavailable"

	return res,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


#打印checking log，脚本每小时执行一次，每次执行结果均进行记录，包括时间和
def print_log():

	#检测W_ListenRec是否有需要合表的分表生成,如果有分表生成，进行合表，如果没有新表生成，不进行任何操作
	log_listen = main_listen()	
	
	#检测W_UserInteractionData是否有需要合表的分表生成,如果有分表生成，进行合表，如果没有新表生成，不进行任何操作
	log_interaction = main_interaction()
	
	
	print (log_listen)
	print (log_interaction)
	command = "echo \'"+log_listen[0]+" time:"+log_listen[1]+"\' >> /etc/sh/checking.log"
	os.system(command)
	command = "echo \'"+log_interaction[0]+" time:"+log_interaction[1]+"\' >> /etc/sh/checking.log"
	os.system(command)
	
if __name__ == '__main__':
	#main_listen()
	print_log()


