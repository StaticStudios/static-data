package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.misc.SchemaAssertions;
import net.staticstudios.data.mock.post.MockPost;
import net.staticstudios.data.mock.user.MockUser;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.util.EnvironmentVariableAccessor;
import net.staticstudios.data.util.ValueUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;

import static net.staticstudios.data.misc.SchemaAssertions.DatabaseEngine.H2;
import static net.staticstudios.data.misc.SchemaAssertions.DatabaseEngine.POSTGRESQL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SQLParseTest extends DataTest {
    private static EnvironmentVariableAccessor previousEnvironmentVariableAccessor;

    @BeforeAll
    public static void setupEnvironmentVariables() {
        previousEnvironmentVariableAccessor = ValueUtils.ENVIRONMENT_VARIABLE_ACCESSOR;
        EnvironmentVariableAccessor accessor = new EnvironmentVariableAccessor();
        accessor.set("POST_SCHEMA", "social_media");
        accessor.set("POST_TABLE", "posts");
        accessor.set("POST_ID_COLUMN", "post_id");
        ValueUtils.ENVIRONMENT_VARIABLE_ACCESSOR = accessor;
    }

    @AfterAll
    public static void restoreEnvironmentVariables() {
        ValueUtils.ENVIRONMENT_VARIABLE_ACCESSOR = previousEnvironmentVariableAccessor;
    }

    @Test
    public void testEnvironmentBackedSchemaMatchesPostgresAndH2() throws SQLException {
        DataManager dataManager = load(MockPost.class);

        assertBuilderTables(dataManager, "social_media", Set.of(
                "posts",
                "posts_metadata",
                "posts_interactions",
                "posts_related"
        ));
        assertBuilderColumns(dataManager, "social_media", "posts", Set.of(
                "post_id",
                "text_content",
                "likes"
        ));

        assertDatabasesMatch(dataManager, "social_media");
    }

    @Test
    public void testLegacyPostgresObjectNamesRemainStable() {
        DataManager dataManager = load(MockPost.class);
        SQLSchema schema = dataManager.getSQLBuilder().getSchema("social_media");
        assertNotNull(schema);

        Set<String> foreignKeyNames = schema.getTables().stream()
                .flatMap(table -> table.getForeignKeys().stream())
                .map(foreignKey -> foreignKey.getName())
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of(
                "fk_o2o_post_id_to_metadata_id",
                "fk_fcol_post_id_to_post_id",
                "fk_pc_m2m_posts_post_id_to_post_id",
                "fk_pc_m2m_posts_ref_post_id_to_post_id"
        ), foreignKeyNames);

        String postgresTriggerSql = schema.getTables().stream()
                .flatMap(table -> table.getTriggers().stream())
                .map(trigger -> trigger.getPgSQL())
                .collect(java.util.stream.Collectors.joining("\n"));
        assertTrue(postgresTriggerSql.contains(
                "static_data_v3_social_media_posts_social_media_posts_metadata_delete_trigger"
        ));
        assertTrue(postgresTriggerSql.contains(
                "static_data_v3_social_media_posts_social_media_posts_interactions_delete_trigger"
        ));
        assertTrue(postgresTriggerSql.contains("static_data_v3_m2m_5120bceb_delete_trigger"));
    }

    @Test
    public void testFullUserSchemaMatchesPostgresAndH2() throws SQLException {
        DataManager dataManager = load(MockUser.class);

        assertBuilderTables(dataManager, "public", Set.of(
                "users",
                "user_settings",
                "user_sessions",
                "user_preferences",
                "user_metadata",
                "user_friends",
                "favorite_numbers"
        ));
        assertBuilderColumns(dataManager, "public", "users", Set.of(
                "id",
                "settings_id",
                "best_buddy_id",
                "age",
                "name",
                "views",
                "counter",
                "__virtual__cv_settings_updates",
                "__virtual__cv_session_additions",
                "__virtual__cv_session_removals",
                "__virtual__cv_friend_additions",
                "__virtual__cv_friend_removals",
                "__virtual__cv_favorite_number_additions",
                "__virtual__cv_favorite_number_removals",
                "__virtual__cv_cooldown_updates",
                "__virtual__cv_throttled_counter",
                "__virtual__cv_on_cooldown",
                "__virtual__cv_counter"
        ));

        assertDatabasesMatch(dataManager, "public");
    }

    @Test
    public void testBroadSchemaContractMatchesPostgresAndH2() throws SQLException {
        DataManager dataManager = load(SchemaContractParent.class);

        assertBuilderTables(dataManager, "schema_contract", Set.of(
                "contract_parents",
                "contract_profiles",
                "contract_children",
                "contract_tags",
                "contract_values"
        ));
        assertBuilderTables(dataManager, "schema_contract_links", Set.of("parent_tags"));
        assertBuilderTables(dataManager, "schema_contract_external", Set.of("parent_details"));
        assertBuilderColumns(dataManager, "schema_contract", "contract_parents", Set.of(
                "tenant_id",
                "parent_id",
                "profile_tenant_id",
                "profile_id",
                "label",
                "code",
                "active",
                "long_value",
                "real_value",
                "double_value",
                "created_at",
                "optional_count",
                "__virtual__cv_cached_score"
        ));

        assertDatabasesMatch(dataManager, "schema_contract", "schema_contract_links", "schema_contract_external");
    }

    private DataManager load(Class<? extends UniqueData> rootType) {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(rootType);
        dataManager.finishLoading();
        return dataManager;
    }

    private void assertDatabasesMatch(DataManager dataManager, String... schemaNames) throws SQLException {
        SchemaAssertions.assertMatches(dataManager, getConnection(), POSTGRESQL, schemaNames);
        SchemaAssertions.assertMatches(dataManager, getH2Connection(dataManager), H2, schemaNames);
    }

    private void assertBuilderTables(DataManager dataManager, String schemaName, Set<String> expectedTables) {
        SQLSchema schema = dataManager.getSQLBuilder().getSchema(schemaName);
        assertNotNull(schema);
        assertEquals(expectedTables, schema.getTables().stream().map(table -> table.getName()).collect(java.util.stream.Collectors.toSet()));
    }

    private void assertBuilderColumns(DataManager dataManager, String schemaName, String tableName, Set<String> expectedColumns) {
        SQLSchema schema = dataManager.getSQLBuilder().getSchema(schemaName);
        assertNotNull(schema);
        assertNotNull(schema.getTable(tableName));
        assertEquals(expectedColumns, schema.getTable(tableName).getColumns().stream().map(column -> column.getName()).collect(java.util.stream.Collectors.toSet()));
    }

    @Data(schema = "schema_contract", table = "contract_profiles")
    static class SchemaContractProfile extends UniqueData {
        @IdColumn(name = "tenant_id")
        public PersistentValue<Integer> tenantId;

        @IdColumn(name = "profile_id")
        public PersistentValue<UUID> profileId;

        @Column(name = "bio", nullable = true)
        public PersistentValue<String> bio;
    }

    @Data(schema = "schema_contract", table = "contract_children")
    static class SchemaContractChild extends UniqueData {
        @IdColumn(name = "child_id")
        public PersistentValue<UUID> childId;

        @Column(name = "owner_tenant_id", nullable = true)
        public PersistentValue<Integer> ownerTenantId;

        @Column(name = "owner_parent_id", nullable = true)
        public PersistentValue<UUID> ownerParentId;

        @Column(name = "payload")
        public PersistentValue<String> payload;
    }

    @Data(schema = "schema_contract", table = "contract_tags")
    static class SchemaContractTag extends UniqueData {
        @IdColumn(name = "tenant_id")
        public PersistentValue<Integer> tenantId;

        @IdColumn(name = "tag_id")
        public PersistentValue<UUID> tagId;

        @Column(name = "name", unique = true)
        public PersistentValue<String> name;
    }

    @Data(schema = "schema_contract", table = "contract_parents")
    static class SchemaContractParent extends UniqueData {
        @IdColumn(name = "tenant_id")
        public PersistentValue<Integer> tenantId;

        @IdColumn(name = "parent_id")
        public PersistentValue<UUID> parentId;

        @Column(name = "profile_tenant_id", nullable = true)
        public PersistentValue<Integer> profileTenantId;

        @Column(name = "profile_id", nullable = true)
        public PersistentValue<UUID> profileId;

        @DefaultValue("O'Reilly")
        @Column(name = "label", index = true)
        public PersistentValue<String> label;

        @Column(name = "code", unique = true)
        public PersistentValue<String> code;

        @DefaultValue("true")
        @Column(name = "active")
        public PersistentValue<Boolean> active;

        @Column(name = "long_value")
        public PersistentValue<Long> longValue;

        @Column(name = "real_value")
        public PersistentValue<Float> realValue;

        @Column(name = "double_value")
        public PersistentValue<Double> doubleValue;

        @Column(name = "created_at")
        public PersistentValue<Timestamp> createdAt;

        @Column(name = "optional_count", nullable = true)
        public PersistentValue<Integer> optionalCount;

        @Delete(DeleteStrategy.CASCADE)
        @ForeignColumn(
                schema = "schema_contract_external",
                table = "parent_details",
                name = "details",
                link = "tenant_id=tenant_id, parent_id=parent_id",
                nullable = true,
                index = true
        )
        public PersistentValue<String> details;

        @Delete(DeleteStrategy.CASCADE)
        @OneToOne(link = "profile_tenant_id=tenant_id, profile_id=profile_id")
        public Reference<SchemaContractProfile> profile;

        @Delete(DeleteStrategy.SET_NULL)
        @OneToMany(link = "tenant_id=owner_tenant_id, parent_id=owner_parent_id")
        public PersistentCollection<SchemaContractChild> children;

        @Delete(DeleteStrategy.SET_NULL)
        @ManyToMany(
                link = "tenant_id=tenant_id, parent_id=tag_id",
                joinTableSchema = "schema_contract_links",
                joinTable = "parent_tags"
        )
        public PersistentCollection<SchemaContractTag> tags;

        @Delete(DeleteStrategy.CASCADE)
        @OneToMany(
                link = "tenant_id=owner_tenant_id, parent_id=owner_parent_id",
                table = "contract_values",
                column = "payload",
                indexed = true,
                nullable = false
        )
        public PersistentCollection<String> values;

        @Identifier(value = "cached_score", index = true)
        public CachedValue<Integer> cachedScore = CachedValue.of(this, Integer.class).withFallback(0);
    }
}
