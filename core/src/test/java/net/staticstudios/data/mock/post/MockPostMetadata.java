package net.staticstudios.data.mock.post;

import net.staticstudios.data.*;

/**
 * Used to validate referringSchema generation.
 */
@Data(schema = "${POST_SCHEMA}", table = "${POST_TABLE}_metadata")
public class MockPostMetadata extends UniqueData {
    @IdColumn(name = "metadata_id")
    public PersistentValue<Integer> id;
    @Column(name = "flag")
    public PersistentValue<Boolean> flag;
}
