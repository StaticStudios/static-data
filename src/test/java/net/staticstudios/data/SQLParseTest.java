package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.mock.post.MockPost;
import net.staticstudios.data.parse.DDLStatement;
import net.staticstudios.data.util.EnvironmentVariableAccessor;
import net.staticstudios.data.util.ValueUtils;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SQLParseTest extends DataTest {

    @BeforeAll
    public static void setup() {
        ValueUtils.ENVIRONMENT_VARIABLE_ACCESSOR = new EnvironmentVariableAccessor() {
            @Override
            public String getEnv(String name) {
                return switch (name) {
                    case "POST_SCHEMA" -> "social_media";
                    case "POST_TABLE" -> "posts";
                    case "POST_ID_COLUMN" -> "post_id";
                    default -> null;
                };
            }
        };
    }

    @Test
    public void testParse() throws Exception {
        DataManager dm = getMockEnvironments().getFirst().dataManager();
        dm.extractMetadata(MockPost.class);
        Connection postgresConnection = getConnection();
        List<DDLStatement> ddlStatements = dm.getSQLBuilder().parse(MockPost.class);
        for (DDLStatement ddl : ddlStatements) {
            System.out.println(ddl.postgresqlStatement());
            try (Statement statement = postgresConnection.createStatement()) {
                statement.execute(ddl.postgresqlStatement());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try (Statement statement = postgresConnection.createStatement()) {
            statement.execute("DROP FUNCTION IF EXISTS public.propagate_data_update_v3");
        }

        Container.ExecResult result = postgres.execInContainer("pg_dump",
                "--schema-only",
                "--no-owner",
                "--no-privileges",
                "--no-comments",
                "--section=pre-data",
                "--section=post-data",
                "-U", postgres.getUsername(),
                postgres.getDatabaseName()
        );
        String schemaDump = result.getStdout();
        StringBuilder cleanedDump = new StringBuilder();
        for (String line : schemaDump.split("\n")) {
            if (line.startsWith("--") || line.startsWith("SET") || line.startsWith("SELECT") || line.trim().isEmpty()) {
                continue;
            }
            cleanedDump.append(line).append("\n");
        }

        @Language("SQL") String expected = """
                CREATE SCHEMA social_media;
                CREATE TABLE social_media.posts (
                    post_id integer NOT NULL,
                    likes integer DEFAULT 0 NOT NULL,
                    text_content text NOT NULL
                );
                CREATE TABLE social_media.posts_interactions (
                    post_id integer NOT NULL,
                    interactions integer DEFAULT 0 NOT NULL
                );
                CREATE TABLE social_media.posts_metadata (
                    metadata_id integer NOT NULL,
                    flag boolean NOT NULL
                );
                ALTER TABLE ONLY social_media.posts_interactions
                    ADD CONSTRAINT posts_interactions_pkey PRIMARY KEY (post_id);
                ALTER TABLE ONLY social_media.posts_metadata
                    ADD CONSTRAINT posts_metadata_pkey PRIMARY KEY (metadata_id);
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT posts_pkey PRIMARY KEY (post_id);
                CREATE INDEX idx_social_media_posts_text_content ON social_media.posts USING btree (text_content);
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT fk_social_media_posts_post_id_to_social_media_posts_interaction FOREIGN KEY (post_id) REFERENCES social_media.posts_interactions(post_id) ON UPDATE CASCADE ON DELETE CASCADE;
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT fk_social_media_posts_post_id_to_social_media_posts_metadata_me FOREIGN KEY (post_id) REFERENCES social_media.posts_metadata(metadata_id) ON UPDATE CASCADE ON DELETE SET NULL;
                """;

        assertEquals(expected.trim(), cleanedDump.toString().trim());
    }

    //todo: when a delete strategy is set to no action where it was previously set to cascade, the old trigger should be dropped. Add a test for this.
}