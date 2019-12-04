package com.github.wangshichun.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import redis.clients.jedis.HostAndPort;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wangshichun on 2016/8/9.
 */
@RunWith(MockitoJUnitRunner.class)
public class JedisClusterTest {
//    @Spy
//    private JedisCluster jedisCluster =null;
    @Test
    public void test() {
//        String hostAndPorts = "192.168.10.22:7006||192.168.10.22:7007||192.168.10.33:7008||192.168.10.33:7009||192.168.10.34:7010||192.168.10.34:7011";
//        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
//        String[] hosts = hostAndPorts.split("\\|\\|");
//        for (String hostport : hosts) {
//            String[] ipport = hostport.split(":");
//            String ip = ipport[0];
//            int port = Integer.parseInt(ipport[1]);
//            nodes.add(new HostAndPort(ip, port));
//        }
//        JedisCluster jedisCluster = new JedisCluster(nodes,
//                2000, 2000, 5, new GenericObjectPoolConfig(), "lydsj@2019");


//        jedisCluster.setReadPreference(ReadPreference.MASTER_THEN_ALL_SLAVE);
//        testReal();
//
//        jedisCluster.setReadPreference(ReadPreference.MASTER_ONLY);
//        testReal();

        JedisCluster jedisCluster = JedisOurCluster.getJedisCluster();


//        jedisCluster.setReadPreference(ReadPreference.MASTER_THEN_ONE_SLAVE);
////        testReal();
        jedisCluster.set("test", "testValue");
        String test = jedisCluster.get("test");
        System.out.println(test);
        Assert.assertTrue("testValue".equals(test));


//        jedisCluster.setReadPreference(ReadPreference.ALL_SLAVE_THEN_MASTER);
//        testReal();
//
//
//        jedisCluster.setReadPreference(ReadPreference.ONE_SLAVE_ONLY);
//        testReal();
//
//
//        jedisCluster.setReadPreference(ReadPreference.ONE_SLAVE_THEN_MASTER);
//        testReal();
    }

    private void testReal() {
//        jedisCluster.set("test", "testValue");
//        Assert.assertTrue("testValue".equals(jedisCluster.get("test")));
    }
}
