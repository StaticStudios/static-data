package net.staticstudios.data.util;

import net.staticstudios.data.UniqueData;
import net.staticstudios.data.utils.Link;

import java.util.List;

public record ReferenceMetadata(Class<? extends UniqueData> holderClass, Class<? extends UniqueData> referencedClass,
                                List<Link> links, boolean generateFkey) {
}
