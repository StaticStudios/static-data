package net.staticstudios.data.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.Collection;
import java.util.UUID;
import java.util.function.BiPredicate;

public class JunctionTable {
    private final Multimap<UUID, UUID> entriesLeftToRight;
    private final Multimap<UUID, UUID> entriesRightToLeft;
    private final String left;
    private final String right;

    public JunctionTable(String left, String right) {
        this.left = left;
        this.right = right;
        this.entriesLeftToRight = Multimaps.synchronizedSetMultimap(HashMultimap.create());
        this.entriesRightToLeft = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    }

    public void add(String left, UUID leftId, UUID rightId) {
        Preconditions.checkNotNull(leftId);
        Preconditions.checkNotNull(rightId);
        if (this.left.equals(left)) {
            entriesLeftToRight.put(leftId, rightId);
            entriesRightToLeft.put(rightId, leftId);
        } else if (this.right.equals(left)) {
            entriesLeftToRight.put(rightId, leftId);
            entriesRightToLeft.put(leftId, rightId);
        } else {
            throw new IllegalArgumentException("Invalid left/right identifier! Expected " + this.left + " or " + this.right + ", got " + left);
        }
    }

    public void remove(String left, UUID leftId, UUID rightId) {
        if (this.left.equals(left)) {
            entriesLeftToRight.remove(leftId, rightId);
            entriesRightToLeft.remove(rightId, leftId);
        } else if (this.right.equals(left)) {
            entriesLeftToRight.remove(rightId, leftId);
            entriesRightToLeft.remove(leftId, rightId);
        } else {
            throw new IllegalArgumentException("Invalid left/right identifier! Expected " + this.left + " or " + this.right + ", got " + left);
        }
    }

    public void removeIf(String left, BiPredicate<UUID, UUID> predicate) {
        if (this.left.equals(left)) {
            entriesLeftToRight.entries().removeIf(e -> predicate.test(e.getKey(), e.getValue()));
        } else if (this.right.equals(left)) {
            entriesRightToLeft.entries().removeIf(e -> predicate.test(e.getKey(), e.getValue()));
        } else {
            throw new IllegalArgumentException("Invalid left/right identifier! Expected " + this.left + " or " + this.right + ", got " + left);
        }
    }

    public Collection<UUID> get(String left, UUID leftId) {
        if (this.left.equals(left)) {
            return entriesLeftToRight.get(leftId);
        } else if (this.right.equals(left)) {
            return entriesRightToLeft.get(leftId);
        } else {
            throw new IllegalArgumentException("Invalid left/right identifier! Expected " + this.left + " or " + this.right + ", got " + left);
        }
    }
}
