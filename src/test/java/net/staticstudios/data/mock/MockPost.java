package net.staticstudios.data.mock;

import net.staticstudios.data.*;

/**
 * Used to validate schema generation.
 */
@Data(schema = "${POST_SCHEMA}", table = "${POST_TABLE}")
public class MockPost extends UniqueData {
    @IdColumn(name = "${POST_ID_COLUMN}")
    public PersistentValue<Integer> id;

    @Column(name = "text_content")
    public PersistentValue<String> textContent;
    @Column(name = "likes", defaultValue = "0")
    public PersistentValue<Integer> likes;
    @ForeignColumn(name = "interactions", table = "${POST_TABLE}_interactions", link = "${POST_ID_COLUMN}=post_id", defaultValue = "0")
    public PersistentValue<Integer> interactions;

    //todo: test relationships
}
