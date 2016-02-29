/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package io.sigpipe.sing.query;

import java.util.HashSet;
import java.util.Set;

public class PayloadFilter<T> {

    private Set<T> filterItems;
    private boolean excludesItems = false;

    public PayloadFilter() {
        filterItems = new HashSet<>();
    }

    public PayloadFilter(boolean excludesItems) {
        this();
        this.excludesItems = excludesItems;
    }

    public PayloadFilter(Set<T> items) {
        this.filterItems = items;
    }

    public PayloadFilter(boolean excludesItems, Set<T> items) {
        this(items);
        this.excludesItems = excludesItems;
    }

    public void add(T item) {
        filterItems.add(item);
    }

    public Set<T> getItems() {
        return filterItems;
    }

    /**
     * Reports whether this PayloadFilter includes or excludes the items it
     * contains.  If this method returns true, then any matching items in the
     * filter will be removed from query results.  On the other hand, if this
     * returns false, then only items in the filter will be retained in query
     * results.
     */
    public boolean excludesItems() {
        return excludesItems;
    }
}
