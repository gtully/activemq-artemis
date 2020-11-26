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
package org.apache.activemq.artemis.core.postoffice.impl;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.BindingsFactory;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.metrics.MetricsManager;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * extends the simple manager to allow wildcard addresses to be used.
 */
public class WildcardAddressManager extends SimpleAddressManager {

   private final AddressMap<Bindings> addressMap = new AddressMap<>(wildcardConfiguration.getAnyWordsString(), wildcardConfiguration.getSingleWordString(), wildcardConfiguration.getDelimiter());

   public WildcardAddressManager(final BindingsFactory bindingsFactory,
                                 final WildcardConfiguration wildcardConfiguration,
                                 final StorageManager storageManager,
                                 final MetricsManager metricsManager) {
      super(bindingsFactory, wildcardConfiguration, storageManager, metricsManager);
   }

   public WildcardAddressManager(final BindingsFactory bindingsFactory,
                                 final StorageManager storageManager,
                                 final MetricsManager metricsManager) {
      super(bindingsFactory, storageManager, metricsManager);
   }

   // publish, may be a new address that needs wildcard bindings added
   // won't contain a wildcard b/c we don't ever route to a wildcards at this time
   @Override
   public Bindings getBindingsForRoutingAddress(final SimpleString address) throws Exception {
      Bindings bindings = super.getBindingsForRoutingAddress(address);

      assert !address.containsEitherOf(wildcardConfiguration.getAnyWords(), wildcardConfiguration.getSingleWord());

      if (bindings == null && !address.containsEitherOf(wildcardConfiguration.getAnyWords(), wildcardConfiguration.getSingleWord())) {

         final WildcardAddressManager wildcardAddressManager = this;
         final Bindings[] lazyCreateResult = new Bindings[1];

         AddressMapVisitor<Bindings> bindingsVisitor = new AddressMapVisitor<Bindings>() {

            Bindings newBindings = null;
            @Override
            public void visit(Bindings matchingBindings) throws Exception {
               if (newBindings == null) {
                  newBindings = wildcardAddressManager.addMappingsInternal(address, matchingBindings.getBindings());
                  lazyCreateResult[0] = newBindings;
               } else {
                  for (Binding binding : matchingBindings.getBindings()) {
                     newBindings.addBinding(binding);
                  }
               }
            }
         };

         addressMap.visitMatching(address, bindingsVisitor);
         bindings = lazyCreateResult[0];

         if (bindings != null) {
            addressMap.put(address, lazyCreateResult[0]);
         }
      }
      return bindings;
   }

   /**
    * If the address to add the binding to contains a wildcard then a copy of the binding (with the same underlying queue)
    * will be added to the actual mappings. Otherwise the binding is added as normal.
    *
    * @param binding the binding to add
    * @return true if the address was a new mapping
    */
   @Override
   public boolean addBinding(final Binding binding) throws Exception {
      final boolean newBindings = super.addBinding(binding);
      final SimpleString address = binding.getAddress();
      final Bindings bindingsForRoutingAddress = mappings.get(binding.getAddress());

      if (address.containsEitherOf(wildcardConfiguration.getAnyWords(), wildcardConfiguration.getSingleWord())) {

         addressMap.visitNonWildcard(address, bindings -> {
            // this wildcard binding needs to be added to matching plain addresses
            bindings.addBinding(binding);
         });

      } else {

         addressMap.visitMatching(address, bindings -> {
            // apply existing bindings from matching wildcards
            for (Binding toAdd : bindings.getBindings()) {
               bindingsForRoutingAddress.addBinding(toAdd);
            }
         });
      }

      if (newBindings) {
         addressMap.put(address, bindingsForRoutingAddress);
      }
      return newBindings;
   }

   /**
    * If the address is a wild card then the binding will be removed from the actual mappings for any linked address.
    * otherwise it will be removed as normal.
    *
    * @param uniqueName the name of the binding to remove
    * @return true if this was the last mapping for a specific address
    */
   @Override
   public Binding removeBinding(final SimpleString uniqueName, Transaction tx) throws Exception {
      Binding binding = super.removeBinding(uniqueName, tx);
      if (binding != null) {
         addressMap.visitMatching(binding.getAddress(), bindings -> bindings.removeBindingByUniqueName(uniqueName));
      }
      return binding;
   }

   @Override
   public AddressInfo removeAddressInfo(SimpleString address) throws Exception {
      final AddressInfo removed = super.removeAddressInfo(address);
      if (removed != null) {
         //Remove from mappings so removeAndUpdateAddressMap processes and cleanup
         Bindings bindings = mappings.remove(address);
         addressMap.remove(address, bindings);
      }
      return removed;
   }

   @Override
   public void clear() {
      super.clear();
      addressMap.reset();
   }

   public AddressMap<Bindings> getAddressMap() {
      return addressMap;
   }
}
