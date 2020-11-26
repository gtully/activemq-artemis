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
package org.apache.activemq.artemis.tests.unit.core.postoffice.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Address;
import org.apache.activemq.artemis.core.postoffice.impl.AddressImpl;
import org.apache.activemq.artemis.core.postoffice.impl.AddressMap;
import org.apache.activemq.artemis.core.postoffice.impl.AddressMapVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AddressMapUnitTest {

   AddressMap<SimpleString> underTest = new AddressMap<>("#", "*", '.');

   @Test
   public void testAddGetRemove() throws Exception {

      SimpleString a = new SimpleString("a.b.c");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatching(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   private boolean isEmpty(SimpleString match) throws Exception {
      return countMatching(match) == 0;
   }

   @Test
   public void testWildcardAddGet() throws Exception {

      SimpleString a = new SimpleString("a.*.c");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatching(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   @Test
   public void testWildcardAllAddGet() throws Exception {

      SimpleString a = new SimpleString("a.b.#");

      assertTrue(isEmpty(a));

      underTest.put(a, a);

      assertFalse(isEmpty(a));

      assertEquals(1, countMatching(a));

      underTest.remove(a, a);

      assertTrue(isEmpty(a));
   }

   @Test
   public void testNoDots() throws Exception {
      SimpleString s1 = new SimpleString("abcde");
      SimpleString s2 = new SimpleString("abcde");

      underTest.put(s1, s1);
      assertEquals(1, countMatching(s2));
   }

   @Test
   public void testDotsSameLength2() throws Exception {
      SimpleString s1 = new SimpleString("a.b");
      SimpleString s2 = new SimpleString("a.b");

      underTest.put(s1, s1);
      assertEquals(1, countMatching(s2));
   }

   @Test
   public void testA() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.c.d.e.f.g.h.i.j.k.l.m.n.*");

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s2));
   }

   @Test
   public void testB() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));
   }

   @Test
   public void testC() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.c.x");
      SimpleString s3 = new SimpleString("a.b.*.d");

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));
   }

   @Test
   public void testD() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e");
      SimpleString s2 = new SimpleString("a.b.c.x.e");
      SimpleString s3 = new SimpleString("a.b.*.d.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));
   }

   @Test
   public void testE() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.b.*.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));

   }

   @Test
   public void testF() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countMatching(s3));

   }

   @Test
   public void testG() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countMatching(s3));

   }

   @Test
   public void testH() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countMatching(s3));
   }

   @Test
   public void testI() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.b.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countMatching(s3));

   }

   @Test
   public void testJ() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.x.e.f");
      SimpleString s3 = new SimpleString("a.#.c.d.e.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));

   }

   @Test
   public void testK() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.e.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertTrue(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(2, countMatching(s3));

   }

   @Test
   public void testL() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d.e.f");
      SimpleString s2 = new SimpleString("a.b.c.d.e.x");
      SimpleString s3 = new SimpleString("a.#.c.d.*.f");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));

   }

   @Test
   public void testM() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));

   }

   @Test
   public void testN() throws Exception {
      SimpleString s1 = new SimpleString("usd.stock");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("*.stock.#");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));
   }

   @Test
   public void testO() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s2 = new SimpleString("a.b.x.e");
      SimpleString s3 = new SimpleString("a.b.c.*");
      Address a1 = new AddressImpl(s1);
      Address a2 = new AddressImpl(s2);
      Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));
      assertFalse(a2.matches(w));

      underTest.put(s1, s1);
      underTest.put(s2, s2);

      assertEquals(1, countMatching(s3));

   }

   @Test
   public void testP() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c#");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);

      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testQ() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testR() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("#*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testS() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("a.b.c*");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testT() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testU() throws Exception {
      SimpleString s1 = new SimpleString("a.b.c.d");
      SimpleString s3 = new SimpleString("*a.b.c");
      Address a1 = new AddressImpl(s1);
      Address w = new AddressImpl(s3);
      assertFalse(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(0, countMatching(s3));

   }

   @Test
   public void testV() throws Exception {
      final SimpleString s1 = new SimpleString("a.b.d");
      final SimpleString s3 = new SimpleString("a.b.#.d");
      final Address a1 = new AddressImpl(s1);
      final Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));

      underTest.put(s1, s1);
      assertEquals(1, countMatching(s3));

      final SimpleString s2 = new SimpleString("a.b.b.b.b.d");
      underTest.put(s2, s2);
      assertEquals(2, countMatching(s3));
   }

   @Test
   public void testVReverse() throws Exception {
      final SimpleString s1 = new SimpleString("a.b.d");
      final SimpleString s3 = new SimpleString("a.b.#.d");
      final Address a1 = new AddressImpl(s1);
      final Address w = new AddressImpl(s3);
      assertTrue(a1.matches(w));

      underTest.put(s3, s3);
      assertEquals(1, countMatching(s1));

   }

   @Test
   public void testHashNMatch() throws Exception {

      SimpleString addressABCF = new SimpleString("a.b.c.f");
      SimpleString addressACF = new SimpleString("a.c.f");
      SimpleString match = new SimpleString("a.#.f");

      underTest.put(addressABCF, addressABCF);
      underTest.put(addressACF, addressACF);

      assertEquals(2, countMatching(match));
   }

   @Test
   public void testEndHash()  throws Exception {

      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressACF = new SimpleString("a.c.f");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString match = new SimpleString("a.b.#");

      underTest.put(addressAB, addressAB);
      underTest.put(addressACF, addressACF);

      assertEquals(1, countMatching(match));

      underTest.put(addressABC, addressABC);
      assertEquals(2, countMatching(match));
   }


   @Test
   public void testHashEndInMap()  throws Exception {

      SimpleString addressABHash = new SimpleString("a.b.#");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString match = new SimpleString("a.b");

      underTest.put(addressABHash, addressABHash);
      underTest.put(addressABC, addressABC);

      assertEquals(1, countMatching(match));
   }

   private int countMatching(SimpleString match) throws Exception {

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatching(match, value -> count.incrementAndGet());

      return count.get();
   }

   @Test
   public void testHashEndMatchMap() throws Exception {

      SimpleString match = new SimpleString("a.b.#");
      SimpleString addressABC = new SimpleString("a.b.c");
      SimpleString addressAB = new SimpleString("a.b");

      underTest.put(addressAB, addressAB);
      underTest.put(addressABC, addressABC);

      assertEquals(2, countMatching(match));
   }


   @Test
   public void testHashAGet() throws Exception {

      SimpleString hashA = new SimpleString("#.a");
      underTest.put(hashA, hashA);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");

      assertEquals(1, countMatching(matchA));
      assertEquals(0, countMatching(matchAB));

      AddressImpl aStar = new AddressImpl(hashA);
      AddressImpl aA = new AddressImpl(matchA);
      assertTrue(aA.matches(aStar));

      AddressImpl aAB = new AddressImpl(matchAB);
      assertFalse(aAB.matches(aStar));
   }

   @Test
   public void testStarOne() throws Exception {

      SimpleString star = new SimpleString("*");
      underTest.put(star, star);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatching(matchA, value -> count.incrementAndGet());
      assertEquals(1, count.get());

      count.set(0);

      underTest.visitMatching(matchAB, value -> count.incrementAndGet());

      assertEquals(0, count.get());
   }

   @Test
   public void testHashOne() throws Exception {

      SimpleString hash = new SimpleString("#");
      underTest.put(hash, hash);

      SimpleString matchA = new SimpleString("a");
      SimpleString matchAB = new SimpleString("a.b");
      SimpleString matchABC = new SimpleString("a.b.c");

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> countCollector = value -> count.incrementAndGet();

      count.set(0);
      underTest.visitMatching(matchA, countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(matchAB, countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(matchABC, countCollector);
      assertEquals(1, count.get());
   }

   @Test
   public void testHashA() throws Exception {

      SimpleString hashA = new SimpleString("#.a");
      underTest.put(hashA, hashA);

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> countCollector = value -> count.incrementAndGet();

      count.set(0);
      underTest.visitMatching(new SimpleString("a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("d.f.c.a"), countCollector);
      assertEquals(1, count.get());


      // has to end in 'a', and not being with 'a'
      SimpleString abcaS = new SimpleString("a.b.c.a");
      AddressImpl aHashA = new AddressImpl(hashA);
      AddressImpl aABCA = new AddressImpl(abcaS);
      assertFalse(aABCA.matches(aHashA));
      assertFalse(aHashA.matches(aABCA));

      count.set(0);
      underTest.visitMatching(abcaS, countCollector);
      assertEquals(0, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("a.b"), countCollector);
      assertEquals(0, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("a.b.c"), countCollector);
      assertEquals(0, count.get());


      count.set(0);
      underTest.visitMatching(new SimpleString("a.b.c.a.d"), countCollector);
      assertEquals(0, count.get());

      // will match a.....a
      SimpleString AHashA = new SimpleString("a.#.a");
      underTest.put(AHashA, AHashA);

      count.set(0);
      underTest.visitMatching(new SimpleString("a.b.c.a"), countCollector);
      assertEquals(1, count.get());

      // only now remove the #.a
      underTest.remove(hashA, hashA);

      count.set(0);
      underTest.visitMatching(new SimpleString("a.b.c.a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("a.a"), countCollector);
      assertEquals(1, count.get());

   }

   @Test
   public void testAHashA() throws Exception {

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> countCollector = value -> count.incrementAndGet();

      // will match a.....a
      SimpleString AHashA = new SimpleString("a.#.a");
      underTest.put(AHashA, AHashA);

      count.set(0);
      underTest.visitMatching(new SimpleString("a.b.c.a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("a.a"), countCollector);
      assertEquals(1, count.get());

      count.set(0);
      underTest.visitMatching(new SimpleString("a"), countCollector);
      assertEquals(0, count.get());
   }

   @Test
   public void testStar() throws Exception {

      SimpleString star = new SimpleString("*");
      SimpleString addressA = new SimpleString("a");
      SimpleString addressAB = new SimpleString("a.b");

      underTest.put(star, star);
      underTest.put(addressAB, addressAB);

      final AtomicInteger count = new AtomicInteger();
      underTest.visitMatching(addressA, value -> count.incrementAndGet());

      assertEquals(1, count.get());
   }


   @Test
   public void testSomeAndAny() throws Exception {

      SimpleString star = new SimpleString("test.*.some.#");
      underTest.put(star, star);

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> collector = value -> count.incrementAndGet();

      underTest.visitMatching(star, collector);
      assertEquals(1, count.get());


      SimpleString addressA = new SimpleString("test.1.some.la");
      underTest.put(addressA, addressA);
      count.set(0);
      underTest.visitMatching(star, collector);
      assertEquals(2, count.get());
   }



   @Test
   public void testAnyAndSome() throws Exception {

      SimpleString star = new SimpleString("test.#.some.*");
      underTest.put(star, star);

      final AtomicInteger count = new AtomicInteger();
      AddressMapVisitor<SimpleString> collector = value -> count.incrementAndGet();

      underTest.visitMatching(star, collector);
      assertEquals(1, count.get());

      // add another match
      SimpleString addressA = new SimpleString("test.1.some.la");
      underTest.put(addressA, addressA);

      count.set(0);
      underTest.visitMatching(star, collector);
      assertEquals(2, count.get());
   }


   @Test
   public void testRealEntries() throws Exception {

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test." + i);
         underTest.put(star, star);
      }

      assertEquals(10, countMatching(new SimpleString("test.*")));

      assertEquals(10, countMatching(new SimpleString("test.#")));

      underTest.put(new SimpleString("test.#"), new SimpleString("test.#"));
      underTest.put(new SimpleString("test.*"), new SimpleString("test.*"));

      assertEquals(12, countMatching(new SimpleString("test.#")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.a." + i);
         underTest.put(star, star);
      }

      assertEquals(22, countMatching(new SimpleString("test.#")));


      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.b." + i);
         underTest.put(star, star);
      }

      assertEquals(11, countMatching(new SimpleString("test.b.*")));
      underTest.remove(new SimpleString("test.#"), new SimpleString("test.#"));
      assertEquals(10, countMatching(new SimpleString("test.b.*")));

      for (int i = 0; i < 10; i++) {
         SimpleString star = new SimpleString("test.c." + i);
         underTest.put(star, star);
      }
      assertEquals(10, countMatching(new SimpleString("test.c.*")));

      assertEquals(30, countMatching(new SimpleString("test.*.*")));

   }
}
