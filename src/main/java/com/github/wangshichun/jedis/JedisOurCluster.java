package com.github.wangshichun.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.Set;

public class JedisOurCluster {
    private static JedisCluster jedisCluster = null;
    public synchronized static JedisCluster getJedisCluster() {
        String hostAndPorts = "192.168.10.22:7006||192.168.10.22:7007||192.168.10.33:7008||192.168.10.33:7009||192.168.10.34:7010||192.168.10.34:7011";
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        String[] hosts = hostAndPorts.split("\\|\\|");
        for (String hostport : hosts) {
            String[] ipport = hostport.split(":");
            String ip = ipport[0];
            int port = Integer.parseInt(ipport[1]);
            nodes.add(new HostAndPort(ip, port));
        }
        if (jedisCluster == null) {
            jedisCluster = new JedisCluster(nodes,
                    2000, 2000, 5, new GenericObjectPoolConfig(), "password");
        }
        return jedisCluster;
    }
}
