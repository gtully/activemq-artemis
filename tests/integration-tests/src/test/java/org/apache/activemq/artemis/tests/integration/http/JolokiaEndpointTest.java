/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JolokiaEndpointTest extends ActiveMQTestBase {

   private Configuration conf;
   private ActiveMQServer server;

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      HashMap<String, Object> params = new HashMap<>();
   //   params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
   //   params.put(TransportConstants.HTTP_UPGRADE_ENABLED_PROP_NAME, true);

      conf = createDefaultInVMConfig().clearAcceptorConfigurations().addAcceptorConfiguration(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));
      server = addServer(ActiveMQServers.newActiveMQServer(conf, false));
      server.start();
   }

   @Test
   public void testJolokiaOverNetty() throws Exception {

      HttpClient client = HttpClient.newBuilder().build();

      HttpRequest request = HttpRequest.newBuilder(new URI("http://localhost:61616/jolokia/read/org.apache.activemq.artemis:broker=artemis/Status"))
         //  .header("Authorization", "Bearer " + authToken)
         .header("Accept", "application/json; charset=utf-8").GET().build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      assertEquals(200, response.statusCode());
   }
}
