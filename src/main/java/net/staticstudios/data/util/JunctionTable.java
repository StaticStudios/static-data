package net.staticstudios.data.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.staticstudios.data.data.collection.CollectionEntryIdentifier;

import java.util.Collection;

public class JunctionTable {
    private final Multimap<CollectionEntryIdentifier, CollectionEntryIdentifier> entries;

    public JunctionTable() {
        this.entries = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    }

    public void add(CollectionEntryIdentifier k1, CollectionEntryIdentifier k2) {
        Preconditions.checkNotNull(k1);
        Preconditions.checkNotNull(k2);
        Preconditions.checkNotNull(k1.getId());
        Preconditions.checkNotNull(k2.getId());
        entries.put(k1, k2);
        entries.put(k2, k1);
    }

    public void remove(CollectionEntryIdentifier k1, CollectionEntryIdentifier k2) {
        entries.remove(k1, k2);
        entries.remove(k2, k1);
    }

    public Collection<CollectionEntryIdentifier> get(CollectionEntryIdentifier k) {
        return entries.get(k);
    }
}
