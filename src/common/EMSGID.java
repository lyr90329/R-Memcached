package common;
public enum EMSGID 
{
	nr_read,          //WebApp send read request to R-Memcached
	nr_read_res,      //R-Memcached  respond read request from WebServer
	nr_write,         //WebApp send write request to R-Memcached
	nr_write_copy,
	nr_write_res,     //R-Memcached  respond write request from WebServer
	nr_connected_mem, //Init connection channel between WebApp and R-Memcached
	nr_connected_mem_back,
	
	nm_connected,     //Init connection channel between R-Memcached and R-Memcached
	nm_connected_mem_back,
	nm_connected_web_back,
	nm_read,          //R-Memcached send read request to another R-Memcached
	nm_read_recovery, //data recovery
	nm_write_1,       //write phase 1
	nm_write_1_res,
	nm_write_2        //write phase 2
}
