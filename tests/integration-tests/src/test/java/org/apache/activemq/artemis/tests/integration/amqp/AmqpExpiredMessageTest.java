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
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.paging.PagingStore;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.settings.impl.AddressFullMessagePolicy;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.activemq.artemis.tests.util.Wait;
import org.apache.activemq.artemis.utils.collections.LinkedListIterator;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Assert;
import org.junit.Test;

public class AmqpExpiredMessageTest extends AmqpClientTestSupport {

   protected void createAddressAndQueues(ActiveMQServer server) throws Exception {
      server.getManagementService().enableNotifications(false);
      super.createAddressAndQueues(server);
   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsAlreadyExpiredUsingAbsoluteTime() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setAbsoluteExpiryTime(System.currentTimeMillis() - 5000);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(0, queueView::getMessageCount);
      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testExpiryThroughTTL() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(1);
      message.setText("Test-Message");
      message.setDurable(true);
      message.setApplicationProperty("key1", "Value1");
      sender.send(message);
      sender.close();

      Thread.sleep(100);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();

      // This will stop and start the server
      // to make sure the message is decoded again from its binary format
      // avoiding any parsing cached at the server.
      server.stop();
      server.start();

      final Queue dlqView = getProxyToQueue(getDeadLetterAddress());
      assertNotNull(dlqView);
      Wait.assertEquals(1, dlqView::getMessageCount);

      client = createAmqpClient();
      connection = addConnection(client.connect());
      session = connection.createSession();

      AmqpReceiver receiverDLQ = session.createReceiver(getDeadLetterAddress());
      receiverDLQ.flow(1);
      received = receiverDLQ.receive(5, TimeUnit.SECONDS);

      assertNotNull("Should have read message from DLQ", received);
      assertEquals(0, received.getTimeToLive());
      assertNotNull(received);
      assertEquals("Value1", received.getApplicationProperty("key1"));

      connection.close();
   }

   @Test(timeout = 60000)
   public void testExpiryThroughTTLToQueueToExpiry() throws Exception {

      String expiryQueue = "specialExpiryQueue";
      String targetQueue = "targetOfMessageToExpire";

      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(expiryQueue), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(expiryQueue).setRoutingType(RoutingType.ANYCAST));
      server.addAddressInfo(new AddressInfo(SimpleString.toSimpleString(targetQueue), RoutingType.ANYCAST));
      server.createQueue(new QueueConfiguration(targetQueue).setRoutingType(RoutingType.ANYCAST));


      AddressSettings defaultSettings =  server.getConfiguration().getAddressesSettings().get("#");
      AddressSettings specifyExpiryQueue = new AddressSettings(defaultSettings);
      specifyExpiryQueue.setExpiryAddress(SimpleString.toSimpleString(expiryQueue));
      server.getAddressSettingsRepository().addMatch(targetQueue, specifyExpiryQueue);

      AddressSettings expiryQueueSettings = new AddressSettings(defaultSettings);
      expiryQueueSettings.setExpiryAddress(SimpleString.toSimpleString(""));
      expiryQueueSettings.setExpiryDelay(1000L);
      server.getAddressSettingsRepository().addMatch(expiryQueue, expiryQueueSettings);


      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(targetQueue);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(1000);
      message.setDurable(true);
      message.setApplicationProperty("key1", "Value1");
      sender.send(message);
      sender.close();

      Thread.sleep(100);

      PagingStore targetPagingStore = server.getPagingManager().getPageStore(SimpleString.toSimpleString(targetQueue));
      assertNotNull(targetPagingStore);

      PagingStore expiryQueuePagingStore = server.getPagingManager().getPageStore(SimpleString.toSimpleString(expiryQueue));
      assertNotNull(expiryQueuePagingStore);

      assertTrue(targetPagingStore.getAddressSize() > 0);

      Wait.assertEquals(0L, targetPagingStore::getAddressSize);

      // moves to expiry q
      assertTrue(expiryQueuePagingStore.getAddressSize() > 0);

      // expires there and dies
      Wait.assertEquals(0L, expiryQueuePagingStore::getAddressSize);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testRetryExpiry() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(1);
      message.setText("Test-Message");
      message.setDurable(true);
      message.setApplicationProperty("key1", "Value1");
      sender.send(message);

      message = new AmqpMessage();
      message.setTimeToLive(1);
      message.setBytes(new byte[500 * 1024]);
      sender.send(message);
      sender.close();

      final Queue dlqView = getProxyToQueue(getDeadLetterAddress());

      Wait.assertEquals(2, dlqView::getMessageCount);
      Assert.assertEquals(2, dlqView.retryMessages(null));
      Wait.assertEquals(0, dlqView::getMessageCount);
      Wait.assertEquals(2, queueView::getMessageCount);


      AmqpReceiver receiver = session.createReceiver(getQueueName());
      // Now try and get the message
      receiver.flow(2);
      for (int i = 0; i < 2; i++) {
         AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(received);
         received.accept();
      }
      connection.close();
      Wait.assertEquals(0, queueView::getMessageCount);
      Wait.assertEquals(0, dlqView::getMessageCount);
   }

   /** This test is validating a broker feature where the message copy through the DLQ will receive an annotation.
    *  It is also testing filter on that annotation. */
   @Test(timeout = 60000)
   public void testExpiryThroughTTLValidateAnnotation() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(1);
      message.setText("Test-Message");
      message.setDurable(true);
      message.setApplicationProperty("key1", "Value1");
      sender.send(message);
      sender.close();

      Thread.sleep(100);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();

      // This will stop and start the server
      // to make sure the message is decoded again from its binary format
      // avoiding any parsing cached at the server.
      server.stop();
      server.start();

      final Queue dlqView = getProxyToQueue(getDeadLetterAddress());
      assertNotNull(dlqView);
      Wait.assertEquals(1, dlqView::getMessageCount);

      client = createAmqpClient();
      connection = addConnection(client.connect());
      session = connection.createSession();

      AmqpReceiver receiverDLQ = session.createReceiver(getDeadLetterAddress(), "\"m.x-opt-ORIG-ADDRESS\"='" + getQueueName() + "'");
      receiverDLQ.flow(1);
      received = receiverDLQ.receive(5, TimeUnit.SECONDS);
      Assert.assertNotNull(received);
      Assert.assertEquals(getQueueName(), received.getMessageAnnotation("x-opt-ORIG-ADDRESS"));
      // close without accepting on purpose, it will issue a redelivery on the second filter
      receiverDLQ.close();

      // Redo the selection, however now using the extra-properties, since the broker will store these as extra properties on AMQP Messages
      receiverDLQ = session.createReceiver(getDeadLetterAddress(), "_AMQ_ORIG_ADDRESS='" + getQueueName() + "'");
      receiverDLQ.flow(1);
      received = receiverDLQ.receive(5, TimeUnit.SECONDS);
      Assert.assertEquals(getQueueName(), received.getMessageAnnotation("x-opt-ORIG-ADDRESS"));
      Assert.assertNotNull(received);
      received.accept();

      assertNotNull("Should have read message from DLQ", received);
      assertEquals(0, received.getTimeToLive());
      assertNotNull(received);
      assertEquals("Value1", received.getApplicationProperty("key1"));

      connection.close();
   }

   /** This test is validating a broker feature where the message copy through the DLQ will receive an annotation.
    *  It is also testing filter on that annotation. */
   @Test(timeout = 60000)
   public void testExpiryQpidJMS() throws Exception {
      ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", getBrokerAmqpConnectionURI().toString());
      Connection connection = factory.createConnection();
      try {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         javax.jms.Queue queue = session.createQueue(getQueueName());
         MessageProducer sender = session.createProducer(queue);

         // Get the Queue View early to avoid racing the delivery.
         final Queue queueView = getProxyToQueue(getQueueName());
         assertNotNull(queueView);

         sender.setTimeToLive(1);
         TextMessage message = session.createTextMessage("Test-Message");
         message.setStringProperty("key1", "Value1");
         sender.send(message);
         sender.close();

         Wait.assertEquals(1, queueView::getMessagesExpired);
         final Queue dlqView = getProxyToQueue(getDeadLetterAddress());
         assertNotNull(dlqView);
         Wait.assertEquals(1, dlqView::getMessageCount);

         connection.start();
         javax.jms.Queue queueDLQ = session.createQueue(getDeadLetterAddress());
         MessageConsumer receiverDLQ = session.createConsumer(queueDLQ, "\"m.x-opt-ORIG-ADDRESS\"='" + getQueueName() + "'");
         Message received = receiverDLQ.receive(5000);
         Assert.assertNotNull(received);
         receiverDLQ.close();
      } finally {
         connection.close();
      }

   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsNotExpiredUsingAbsoluteTime() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setAbsoluteExpiryTime(System.currentTimeMillis() + 5000);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Wait.assertEquals(1, queueView::getMessageCount);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);

      assertEquals(0, queueView.getMessagesExpired());

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsExiredUsingAbsoluteTimeWithLongTTL() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setAbsoluteExpiryTime(System.currentTimeMillis() - 5000);
      // AET should override any TTL set
      message.setTimeToLive(60000);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Wait.assertEquals(1, queueView::getMessageCount);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsExpiredUsingTTLWhenAbsoluteIsZero() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setAbsoluteExpiryTime(0);
      // AET should override any TTL set
      message.setTimeToLive(100);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Wait.assertEquals(1L, queueView::getMessagesExpired, 10000, 10);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsNotExpiredUsingAbsoluteTimeWithElspsedTTL() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setAbsoluteExpiryTime(System.currentTimeMillis() + 5000);
      // AET should override any TTL set
      message.setTimeToLive(10);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Wait.assertEquals(1, queueView::getMessageCount);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);

      Wait.assertEquals(0, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageThatIsNotExpiredUsingTimeToLive() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(5000);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Wait.assertEquals(1, queueView::getMessageCount);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receive(5, TimeUnit.SECONDS);
      assertNotNull(received);

      Wait.assertEquals(0, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testSendMessageThenAllowToExpiredUsingTimeToLive() throws Exception {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setTimeToLive(10);
      message.setText("Test-Message");
      sender.send(message);
      sender.close();

      Thread.sleep(50);

      Wait.assertEquals(0, queueView::getMessageCount);

      // Now try and get the message
      AmqpReceiver receiver = session.createReceiver(getQueueName());
      receiver.flow(1);
      AmqpMessage received = receiver.receiveNoWait();
      assertNull(received);

      Wait.assertEquals(1, queueView::getMessagesExpired);

      connection.close();
   }

   @Test(timeout = 60000)
   public void testExpiredMessageLandsInDLQ() throws Throwable {
      internalSendExpiry(false);
   }

   @Test(timeout = 60000)
   public void testExpiredMessageLandsInDLQAndExistsAfterRestart() throws Throwable {
      internalSendExpiry(true);
   }

   public void internalSendExpiry(boolean restartServer) throws Throwable {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();

      try {

         // Normal Session which won't create an TXN itself
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setText("Test-Message");
         message.setDeliveryAnnotation("shouldDisappear", 1);
         message.setAbsoluteExpiryTime(System.currentTimeMillis() + 250);
         sender.send(message);

         Queue dlq = getProxyToQueue(getDeadLetterAddress());
         assertTrue("Message not movied to DLQ", Wait.waitFor(() -> dlq.getMessageCount() > 0, 7000, 500));

         connection.close();

         if (restartServer) {
            server.stop();
            server.start();
         }

         connection = client.connect();
         session = connection.createSession();

         // Read all messages from the Queue
         AmqpReceiver receiver = session.createReceiver(getDeadLetterAddress());
         receiver.flow(20);

         message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(getQueueName(), message.getMessageAnnotation("x-opt-ORIG-QUEUE"));
         assertNull(message.getDeliveryAnnotation("shouldDisappear"));
         assertNull(receiver.receiveNoWait());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testDLQdMessageCanBeRedeliveredMultipleTimes() throws Throwable {
      AmqpClient client = createAmqpClient();
      AmqpConnection connection = client.connect();

      try {
         AmqpSession session = connection.createSession();
         AmqpSender sender = session.createSender(getQueueName());

         AmqpMessage message = new AmqpMessage();
         message.setDurable(true);
         message.setTimeToLive(250);
         message.setText("Test-Message");
         message.setMessageId(UUID.randomUUID().toString());
         message.setApplicationProperty("key", "value");

         sender.send(message);

         Queue dlqView = getProxyToQueue(getDeadLetterAddress());
         assertTrue("Message not movied to DLQ", Wait.waitFor(() -> dlqView.getMessageCount() > 0, 7000, 200));

         // Read and Modify the message for redelivery repeatedly
         AmqpReceiver receiver = session.createReceiver(getDeadLetterAddress());
         receiver.flow(20);

         message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(0, message.getWrappedMessage().getDeliveryCount());

         message.modified(true, false);

         message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(1, message.getWrappedMessage().getDeliveryCount());

         message.modified(true, false);

         message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(2, message.getWrappedMessage().getDeliveryCount());

         message.modified(true, false);

         message = receiver.receive(5, TimeUnit.SECONDS);
         assertNotNull(message);
         assertEquals(3, message.getWrappedMessage().getDeliveryCount());
      } finally {
         connection.close();
      }
   }

   @Test(timeout = 60000)
   public void testExpireThorughAddressSettings() throws Exception {
      testExpireThorughAddressSettings(false);
   }

   @Test(timeout = 60000)
   public void testExpireThorughAddressSettingsRebootServer() throws Exception {
      testExpireThorughAddressSettings(true);
   }

   private void testExpireThorughAddressSettings(boolean reboot) throws Exception {

      // Address configuration
      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setAutoCreateQueues(isAutoCreateQueues());
      addressSettings.setAutoCreateAddresses(isAutoCreateAddresses());
      addressSettings.setDeadLetterAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryDelay(1000L);

      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", addressSettings);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      message.setDurable(true);
      message.setApplicationProperty("key1", "Value1");
      sender.send(message);

      message = new AmqpMessage();
      message.setBytes(new byte[500 * 1024]);
      message.setDurable(true);
      sender.send(message);
      sender.close();
      connection.close();

      if (reboot) {
         server.stop();
         server.getConfiguration().setMessageExpiryScanPeriod(100);
         server.start();
      }

      final Queue dlqView = getProxyToQueue(getDeadLetterAddress());

      Wait.assertEquals(2, dlqView::getMessageCount);
   }

   @Test
   public void testPreserveExpirationOnTTL() throws Exception {

      // Address configuration
      AddressSettings addressSettings = new AddressSettings();

      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setAutoCreateQueues(isAutoCreateQueues());
      addressSettings.setAutoCreateAddresses(isAutoCreateAddresses());
      addressSettings.setDeadLetterAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryAddress(SimpleString.toSimpleString(getDeadLetterAddress()));
      addressSettings.setExpiryDelay(1000L);

      server.getAddressSettingsRepository().clear();
      server.getAddressSettingsRepository().addMatch("#", addressSettings);

      AmqpClient client = createAmqpClient();
      AmqpConnection connection = addConnection(client.connect());
      AmqpSession session = connection.createSession();

      AmqpSender sender = session.createSender(getQueueName());

      // Get the Queue View early to avoid racing the delivery.
      final Queue queueView = getProxyToQueue(getQueueName());
      assertNotNull(queueView);

      AmqpMessage message = new AmqpMessage();
      message.setText("Test-Message");
      message.setDurable(true);
      message.setTimeToLive(3600 * 1000);
      message.setApplicationProperty("id", "0");
      sender.send(message);

      message = new AmqpMessage();
      message.setBytes(new byte[500 * 1024]);
      message.setDurable(true);
      message.setTimeToLive(3600 * 1000);
      message.setApplicationProperty("id", "1");
      sender.send(message);

      Wait.assertEquals(2, queueView::getMessageCount);
      LinkedListIterator<MessageReference> linkedListIterator = queueView.iterator();
      HashMap<String, Long> dataSet = new HashMap<>();
      int count = 0;
      while (linkedListIterator.hasNext()) {
         count++;
         MessageReference ref = linkedListIterator.next();
         String idUsed = ref.getMessage().getStringProperty("id");
         dataSet.put(idUsed, ref.getMessage().getExpiration());
      }

      Assert.assertEquals(2, count);
      linkedListIterator.close();

      server.stop();

      Thread.sleep(500); // we need some time passing, as the TTL can't be recalculated here
      server.getConfiguration().setMessageExpiryScanPeriod(100);
      server.start();

      final Queue queueViewAfterRestart = getProxyToQueue(getQueueName());

      Wait.assertEquals(2, queueViewAfterRestart::getMessageCount);
      Wait.assertTrue(server::isActive);

      linkedListIterator = queueViewAfterRestart.iterator();
      count = 0;
      while (linkedListIterator.hasNext()) {
         count++;
         MessageReference ref = linkedListIterator.next();
         String idUsed = ref.getMessage().getStringProperty("id");
         long originalExpiration = dataSet.get(idUsed);
         System.out.println("original Expiration = " + originalExpiration + " while this expiration = " + ref.getMessage().getExpiration());
         Assert.assertEquals(originalExpiration, ref.getMessage().getExpiration());
      }
      Assert.assertEquals(2, count);
      linkedListIterator.close();


   }




}
