package net.staticstudios.data.mock.post;

import net.staticstudios.data.*;

/**
 * Used to validate schema generation.
 */
@Data(schema = "${POST_SCHEMA}", table = "${POST_TABLE}")
public class MockPost extends UniqueData {
    @IdColumn(name = "${POST_ID_COLUMN}")
    public PersistentValue<Integer> id;
    @OneToOne(link = "${POST_ID_COLUMN}=metadata_id")
    public Reference<MockPostMetadata> metadata;

    @Column(name = "text_content", index = true)
    public PersistentValue<String> textContent;
    @DefaultValue("0")
    @Column(name = "likes")
    public PersistentValue<Integer> likes;
    @DefaultValue("0")
    @ForeignColumn(name = "interactions", table = "${POST_TABLE}_interactions", link = "${POST_ID_COLUMN}=post_id")
    public PersistentValue<Integer> interactions;

    @ManyToMany(link = "${POST_ID_COLUMN}=${POST_ID_COLUMN}", joinTable = "${POST_TABLE}_related")
    public PersistentCollection<MockPost> relatedPosts;
}
