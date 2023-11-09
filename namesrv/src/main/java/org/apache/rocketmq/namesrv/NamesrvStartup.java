/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/**
 * NameServer是一个注册中心，Broker在启动时向所有的NameServer注册，生产者Producer和消费者Consumer可以从NameServer中获取所有注册的Broker列表
 * ，并从中选取Broker进行消息的发送和消费。
 */
public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    /**
     * 1. NameServer的启动类是NamesrvStartup，主要做了两件事情：
     *  1. 调用createNamesrvController方法创建NamesrvController，NamesrvController是NameServer的核心
     *  2. 调用start方法，启动NameServer
     * @param args
     * @return
     */
    public static NamesrvController main0(String[] args) {

        try {
            // 创建NamesrvController
            NamesrvController controller = createNamesrvController(args);
            // 启动nameserver
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 创建NamesrvController
     * 1. createNamesrvController方法主要是对配置信息进行处理：
     *      1. 创建NamesrvConfig，从名字可以看出是记录NameServer的相关配置信息
     *      2. 创建NettyServerConfig，与Netty服务相关的配置信息，默认设置监听端口为9876
     *      3. 从启动命令中检查是否通过- c指定了配置文件，如果指定了配置文件，从指定的路径中加载文件，并将解析文件将配置保存到NamesrvConfig和NettyServerConfig中
     *      4. 校验RocketMQ的主目录是否为空，可以在启动命令中通过-Drocketmq.home.dir=路径指定主目录，也可以在操作系统设置环境变量ROCKETMQ_HOME的方式来指定
     *      5. 处理日志相关的设置
     *      6. 创建NamesrvController并返回

     * @param args
     * @return
     * @throws IOException
     * @throws JoranException
     */
    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        // Nameserver相关配置
        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        // Netty服务器连接相关配置
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        // 设置端口
        nettyServerConfig.setListenPort(9876);
        // 如果启动命令中指定了配置文件
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                // 获取文件
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                // 解析配置文件
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        // 如果启动命令中带了-p参数,打印NameServer的相关配置信息
        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        // 将启动命令中的一些设置记录到namesrvConfig
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        // 如果RocketMQ主目录获取为空，打印异常信息
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // 日志相关设置
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        // 创建NamesrvController
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    /**
     *启动NameServer
     * 1. NameServer的启动主要通过NamesrvController进行，处理逻辑如下：
     *      1. 调用NamesrvController的initialize函数进行初始化
     *      2. 注册JVM关闭钩子函数，在JVM关闭的时候，调用NamesrvController的shutdown方法关闭相关资源
     *      3. 调用NamesrvController的start方法启动NameServer
     *
     * @param controller
     * @return
     * @throws Exception
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 初始化NamesrvController
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        // 注册JVM关闭钩子函数，在退出时进行释放资源，停止服务端，停止线城池，停止定时任务
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                controller.shutdown();
                return null;
            }
        }));

        // 启动nameserver
        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
