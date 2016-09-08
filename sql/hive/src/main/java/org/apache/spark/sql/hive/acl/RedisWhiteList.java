package org.apache.spark.sql.hive.acl;

/**
 * used to check whether permitted to access specified table.
 * Created by hufuwang on 16/8/11.
 */
public class RedisWhiteList {
    public static String host = HiveACLConstants.DefaultHiveAclRedisHost;
    public static int port = HiveACLConstants.DefaultHiveAclRedisPort;
    public static int db = HiveACLConstants.DefaultHiveACLRedisDB;
    public static String keyPrefix = HiveACLConstants.DefaultHiveAclWhiteListKeyPrefix;
    public static RedisClient client;

    public static Boolean accessPermitted(String dbName,String tableName){
        String fullTableName = dbName + "." + tableName;
        client = new RedisClient(host,port,db);
        return client.sismember(keyPrefix+"db",dbName) || client.sismember(keyPrefix+"table",fullTableName);
    }

    public static Boolean accessPermitted(String redisHost,int redisPort,int redisDB,String redisKeyPrefix, String dbName, String tableName){
        host = redisHost;
        port = redisPort;
        db = redisDB;
        keyPrefix = redisKeyPrefix;
        return accessPermitted(dbName,tableName);
    }

    public static void main(String[] args){
        System.out.println(accessPermitted("default","dual"));
    }
}
