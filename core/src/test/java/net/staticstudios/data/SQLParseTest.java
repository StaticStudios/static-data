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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

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

    private static String normalize(String str) {
        return str.replace("\r\n", "\n").trim();
    }

    private static void assertSqlLinesEqualOrderIndependent(List<String> expectedLines, List<String> actualLines) {
        Set<String> expectedSet = new LinkedHashSet<>(expectedLines.stream().map(l -> {
            if (l.endsWith(",")) {
                l = l.substring(0, l.length() - 1);
            }
            return l.trim();
        }).toList());
        Set<String> actualSet = new LinkedHashSet<>(actualLines.stream().map(l -> {
            if (l.endsWith(",")) {
                l = l.substring(0, l.length() - 1);
            }
            return l.trim();
        }).toList());

        if (!expectedSet.equals(actualSet)) {
            Set<String> missing = new LinkedHashSet<>(expectedSet);
            missing.removeAll(actualSet);
            Set<String> unexpected = new LinkedHashSet<>(actualSet);
            unexpected.removeAll(expectedSet);

            StringBuilder msg = new StringBuilder();
            msg.append(String.format("Schema mismatch: expected %d distinct lines, actual %d distinct lines.%n", expectedSet.size(), actualSet.size()));
            if (!missing.isEmpty()) {
                msg.append(String.format("Missing (%d):%n", missing.size()));
                for (String s : missing) {
                    msg.append(String.format("  %s%n", s));
                }
            }
            if (!unexpected.isEmpty()) {
                msg.append(String.format("Unexpected (%d):%n", unexpected.size()));
                for (String s : unexpected) {
                    msg.append(String.format("  %s%n", s));
                }
            }

            msg.append("Full expected:\n");
            for (String s : expectedLines) {
                msg.append(String.format("  %s%n", s));
            }
            msg.append("Full actual:\n");
            for (String s : actualLines) {
                msg.append(String.format("  %s%n", s));
            }

            fail(msg.toString());
        }

        assertFalse(actualLines.isEmpty(), String.format("No SQL lines were produced by pg_dump after cleaning. Expected %d distinct lines but got %d distinct lines.", expectedSet.size(), actualSet.size()));
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
                CREATE TABLE social_media.posts_related (
                    posts_post_id integer NOT NULL,
                    posts_ref_post_id integer NOT NULL
                );
                ALTER TABLE ONLY social_media.posts_interactions
                    ADD CONSTRAINT posts_interactions_pkey PRIMARY KEY (post_id);
                ALTER TABLE ONLY social_media.posts_metadata
                    ADD CONSTRAINT posts_metadata_pkey PRIMARY KEY (metadata_id);
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT posts_pkey PRIMARY KEY (post_id);
                ALTER TABLE ONLY social_media.posts_related
                    ADD CONSTRAINT posts_related_pkey PRIMARY KEY (posts_post_id, posts_ref_post_id);
                CREATE INDEX idx_social_media_posts_text_content ON social_media.posts USING btree (text_content);
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT fk_post_id_to_metadata_id FOREIGN KEY (post_id) REFERENCES social_media.posts_metadata(metadata_id) ON UPDATE CASCADE ON DELETE SET NULL;
                ALTER TABLE ONLY social_media.posts
                    ADD CONSTRAINT fk_post_id_to_post_id FOREIGN KEY (post_id) REFERENCES social_media.posts_interactions(post_id) ON UPDATE CASCADE ON DELETE CASCADE;
                ALTER TABLE ONLY social_media.posts_related
                    ADD CONSTRAINT fk_posts_post_id_to_post_id FOREIGN KEY (posts_post_id) REFERENCES social_media.posts(post_id) ON UPDATE CASCADE ON DELETE CASCADE;
                ALTER TABLE ONLY social_media.posts_related
                    ADD CONSTRAINT fk_posts_ref_post_id_to_post_id FOREIGN KEY (posts_ref_post_id) REFERENCES social_media.posts(post_id) ON UPDATE CASCADE ON DELETE CASCADE;
                """;

        List<String> expectedLines = Arrays.asList(normalize(expected).split("\n"));
        List<String> actualLines = Arrays.asList(normalize(cleanedDump.toString()).split("\n"));

        assertSqlLinesEqualOrderIndependent(expectedLines, actualLines);
    }

    //todo: when a delete strategy is set to no action where it was previously set to cascade, the old trigger should be dropped. Add a test for this. moreover, what happens when we change the name of something? will the old trigger stay or what? handle this
}