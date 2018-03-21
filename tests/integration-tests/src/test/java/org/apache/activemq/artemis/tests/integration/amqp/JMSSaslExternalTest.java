/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.utils.RandomUtil;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.Before;
import org.junit.Test;

public class JMSSaslExternalTest extends ActiveMQTestBase {

   static {
      String path = System.getProperty("java.security.auth.login.config");
      if (path == null) {
         URL resource = JMSSaslExternalTest.class.getClassLoader().getResource("login.config");
         if (resource != null) {
            path = resource.getFile();
            System.setProperty("java.security.auth.login.config", path);
         }
      }
   }

   private final boolean debug = true;

   @Before
   public void setUpDebug() throws Exception {

      if (debug) {
         for (java.util.logging.Logger logger : new java.util.logging.Logger[] {java.util.logging.Logger.getLogger("javax.security.sasl"), java.util.logging.Logger.getLogger("org.apache.qpid.proton")}) {
            logger.setLevel(java.util.logging.Level.FINEST);
            logger.addHandler(new java.util.logging.ConsoleHandler());
            for (java.util.logging.Handler handler : logger.getHandlers()) {
               handler.setLevel(java.util.logging.Level.FINEST);
            }
         }
      }
   }

   @Test(timeout = 600000)
   public void testConnection() throws Exception {

      ConfigurationImpl configuration = createBasicConfig(0).setJMXManagementEnabled(false);
      ActiveMQJAASSecurityManager securityManager = new ActiveMQJAASSecurityManager("CertLogin");
      ActiveMQServer server = addServer(ActiveMQServers.newActiveMQServer(configuration.setSecurityEnabled(true), ManagementFactory.getPlatformMBeanServer(), securityManager, false));

      Map<String, Object> params = new HashMap<>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "server-side-keystore.jks");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "server-side-truststore.jks");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");
      params.put(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME, true);

      Map<String, Object> extraParams = new HashMap<>();
      extraParams.put("saslMechanisms", "EXTERNAL");

      server.getConfiguration().addAcceptorConfiguration(new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(), params, "netty", extraParams));

      // role mapping via CertLogin - TextFileCertificateLoginModule
      final String roleName = "programmers";
      Role role = new Role(roleName, true, true, true, true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<>();
      roles.add(role);
      server.getSecurityRepository().addMatch("TEST", roles);

      server.start();


      final String keystore = this.getClass().getClassLoader().getResource("client-side-keystore.jks").getFile();
      final String truststore = this.getClass().getClassLoader().getResource("client-side-truststore.jks").getFile();

      String connOptions = "?amqp.saslMechanisms=EXTERNAL" + "&" +
         "transport.trustStoreLocation=" + truststore + "&" +
         "transport.trustStorePassword=secureexample" + "&" +
         "transport.keyStoreLocation=" + keystore + "&" +
         "transport.keyStorePassword=secureexample" + "&" +
         "transport.verifyHost=false";

      JmsConnectionFactory factory = new JmsConnectionFactory(new URI("amqps://localhost:" + 61616 + connOptions));
      Connection connection = factory.createConnection("client", null);
      connection.start();

      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue("TEST");
         MessageConsumer consumer = session.createConsumer(queue);
         MessageProducer producer = session.createProducer(queue);

         final String text = RandomUtil.randomString();
         producer.send(session.createTextMessage(text));

         TextMessage m = (TextMessage) consumer.receive(1000);
         assertNotNull(m);
         assertEquals(text, m.getText());

      } finally {
         connection.close();
      }
   }

}
