/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.core.postoffice.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class AddressPartNode<T> {

   protected final String ANY_CHILD;
   protected final String ANY_DESCENDENT;

   private final AddressPartNode<T> parent;
   private final List<T> values = new CopyOnWriteArrayList<>();
   private final Map<String, AddressPartNode<T>> childNodes = new ConcurrentHashMap<>();
   private final String path;
   private final int pathLength;

   public AddressPartNode(final String path, final AddressPartNode<T> parent) {
      this.parent = parent;
      pathLength = parent.pathLength + 1;
      this.ANY_DESCENDENT = parent.ANY_DESCENDENT;
      this.ANY_CHILD = parent.ANY_CHILD;

      if (ANY_DESCENDENT.equals(path)) {
         this.path = ANY_DESCENDENT;
      } else if (ANY_CHILD.equals(path)) {
         this.path = ANY_CHILD;
      } else {
         this.path = path;
      }
   }

   public AddressPartNode(String anyDescendent, String anyChild) {
      ANY_DESCENDENT = anyDescendent;
      ANY_CHILD = anyChild;
      pathLength = 0;
      path = "Root";
      parent = null;
   }

   public AddressPartNode<T> getChild(final String path) {
      return childNodes.get(path);
   }

   public Collection<AddressPartNode<T>> getChildren() {
      return childNodes.values();
   }

   public AddressPartNode<T> getChildOrCreate(final String path) {
      AddressPartNode<T> answer = childNodes.get(path);
      if (answer == null) {
         answer = new AddressPartNode<>(path, this);
         childNodes.put(path, answer);
      }
      return answer;
   }

   public void add(final String[] paths, final int idx, final T value) {
      if (idx >= paths.length) {
         values.add(value);
      } else {
         getChildOrCreate(paths[idx]).add(paths, idx + 1, value);
      }
   }

   public void remove(final String[] paths, final int idx, final Object value) {
      if (idx >= paths.length) {
         values.remove(value);
         pruneIfEmpty();
      } else {
         getChildOrCreate(paths[idx]).remove(paths, idx + 1, value);
      }
   }

   public void visitDescendantValues(final AddressMapVisitor<T> collector) throws Exception {
      visitValues(collector);
      for (AddressPartNode<T> child : childNodes.values()) {
         child.visitDescendantValues(collector);
      }
   }

   public void visitPathTailMatch(final String[] paths,
                                  final int startIndex,
                                  final AddressMapVisitor<T> collector,
                                  boolean matchWildcardsInMap) throws Exception {

      // look for a path match after 0-N skips among children
      AddressPartNode<T> match = null;
      for (int i = startIndex; i < paths.length; i++) {
         match = getChild(paths[i]);
         if (match != null) {
            if (!matchWildcardsInMap && (ANY_CHILD == match.getPath() || ANY_DESCENDENT == match.getPath())) {
               continue;
            }
            match.visitMatching(paths, i + 1, collector, matchWildcardsInMap);
            break;
         }
      }

      // walk the rest of the sub tree to find a tail path match
      for (AddressPartNode<T> child : childNodes.values()) {
         // instance equality arranged in node creation getChildOrCreate
         if (child != match && ANY_DESCENDENT != child.getPath()) {

            if (!matchWildcardsInMap && ANY_CHILD == child.getPath()) {
               continue;
            }
            child.visitPathTailMatch(paths, startIndex, collector, matchWildcardsInMap);
         }
      }
   }

   public void visitMatching(final String[] paths, final int startIndex, final AddressMapVisitor<T> collector, final boolean matchWildcardsInMap) throws Exception {
      boolean canVisitAnyDescendent = true;
      AddressPartNode<T> node = this;
      AddressPartNode<T> wildCardNode = null;
      final int size = paths.length;
      for (int i = startIndex; i < size && node != null; i++) {

         canVisitAnyDescendent = true;  // reset for each path
         final String path = paths[i];

         // snuff out any descendant postfix in the paths ....#
         if (ANY_DESCENDENT.equals(path)) {
            if (i == size - 1) {
               node.visitDescendantValues(collector);
               canVisitAnyDescendent = false;
               break;
            }
         }

         if (matchWildcardsInMap) {
            wildCardNode = node.getChild(ANY_DESCENDENT);
            if (wildCardNode != null) {

               wildCardNode.visitValues(collector);
               // match tail
               wildCardNode.visitPathTailMatch(paths, i, collector, true);
               canVisitAnyDescendent = false;
            }

            wildCardNode = node.getChild(ANY_CHILD);
            if (wildCardNode != null) {
               wildCardNode.visitMatching(paths, i + 1, collector, true);
            }
         }

         // check the path again

         if (ANY_CHILD.equals(path)) {

            for (AddressPartNode<T> anyChild : node.getChildren()) {

               if (!matchWildcardsInMap && (ANY_CHILD == anyChild.getPath() || ANY_DESCENDENT == anyChild.getPath())) {
                  continue;
               }

               // don't visit the ANY_CHILD again
               if (anyChild != wildCardNode) {
                  anyChild.visitMatching(paths, i + 1, collector, matchWildcardsInMap);
               }
            }
            // once we have done depth first on all children we are done with our paths
            return;

         } else if (ANY_DESCENDENT.equals(path)) {

            node.visitValues(collector);
            node.visitPathTailMatch(paths, i + 1, collector, matchWildcardsInMap);
            // once we have done depth first on all children we are done with our paths
            return;

         } else {

            node = node.getChild(path);

         }
      }
      if (node != null) {

         if (canVisitAnyDescendent) {

            node.visitValues(collector);

            if (matchWildcardsInMap) {
               // allow zero node any descendant at the end of path node
               wildCardNode = node.getChild(ANY_DESCENDENT);
               if (wildCardNode != null) {
                  wildCardNode.visitValues(collector);
               }
            }
         }
      }
   }

   public void visitValues(final AddressMapVisitor<T> collector) throws Exception {
      for (T o : values) {
         collector.visit(o);
      }
   }

   public String getPath() {
      return path;
   }

   protected void pruneIfEmpty() {
      if (parent != null && childNodes.isEmpty() && values.isEmpty()) {
         parent.removeChild(this);
      }
   }

   protected void removeChild(final AddressPartNode<T> node) {
      childNodes.remove(node.getPath());
      pruneIfEmpty();
   }

   public void reset() {
      values.clear();
      childNodes.clear();
   }
}

