package net.staticstudios.data;

import net.staticstudios.data.misc.DataTest;
import net.staticstudios.data.parse.ForeignKey;
import net.staticstudios.data.parse.SQLManyToManyDeleteStrategyTrigger;
import net.staticstudios.data.parse.SQLSchema;
import net.staticstudios.data.parse.SQLTable;
import net.staticstudios.data.parse.SQLTrigger;
import net.staticstudios.data.util.OnDelete;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class DeletionTest extends DataTest {
    //todo: Similar to this test, we should create a test for update strategies.

    @Test
    public void testReferenceSetNull() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserMetadataSetNull metadata = UserMetadataSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(metadata.isDeleted());
    }

    @Test
    public void testReferenceSetNull2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserMetadataSetNull metadata = UserMetadataSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        metadata.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testReferenceCascade() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserMetadataCascade metadata = UserMetadataCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        assertSame(metadata, user.ref.get());

        user.delete();

        assertTrue(metadata.isDeleted());
    }

    @Test
    public void testReferenceCascade2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserMetadataCascade metadata = UserMetadataCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .metadataId(1)
                .insert(InsertMode.SYNC);

        assertSame(metadata, user.ref.get());

        metadata.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testOneToManySetNull() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionSetNull action1 = UserActionSetNull.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(action1.isDeleted());
        assertNull(action1.userId.get());
    }

    @Test
    public void testOneToManySetNull2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserSetNull.class);
        dataManager.finishLoading();

        UserSetNull user = UserSetNull.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionSetNull action1 = UserActionSetNull.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        action1.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testOneToManyCascade() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionCascade action1 = UserActionCascade.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        user.delete();

        assertFalse(action1.isDeleted());
        assertNull(action1.userId.get());
    }

    @Test
    public void testOneToManyCascade2() {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(UserCascade.class);
        dataManager.finishLoading();

        UserCascade user = UserCascade.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        UserActionCascade action1 = UserActionCascade.builder(dataManager)
                .id(1)
                .userId(1)
                .insert(InsertMode.SYNC);

        action1.delete();

        assertFalse(user.isDeleted());
    }

    @Test
    public void testManyToManyCascadeDeletesJoinEntriesAndChildren() throws SQLException {
        DataManager dataManager = load(ManyToManyCascadeHolder.class);

        ManyToManyCascadeHolder holder = ManyToManyCascadeHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManyCascadeChild firstChild = ManyToManyCascadeChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        ManyToManyCascadeChild secondChild = ManyToManyCascadeChild.builder(dataManager)
                .id(11)
                .insert(InsertMode.SYNC);
        holder.children.add(firstChild);
        holder.children.add(secondChild);

        holder.delete();

        assertTrue(holder.isDeleted());
        assertTrue(firstChild.isDeleted());
        assertTrue(secondChild.isDeleted());
        assertTableRowCount(dataManager, "m2m_cascade_join", 0);
        assertTableRowCount(dataManager, "m2m_cascade_children", 0);
    }

    @Test
    public void testManyToManyCascadeDoesNotDeleteUnlinkedChildren() throws SQLException {
        DataManager dataManager = load(ManyToManyCascadeHolder.class);

        ManyToManyCascadeHolder holder = ManyToManyCascadeHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManyCascadeChild linkedChild = ManyToManyCascadeChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        ManyToManyCascadeChild unlinkedChild = ManyToManyCascadeChild.builder(dataManager)
                .id(11)
                .insert(InsertMode.SYNC);
        holder.children.add(linkedChild);

        holder.delete();

        assertTrue(linkedChild.isDeleted());
        assertFalse(unlinkedChild.isDeleted());
        assertTableRowCount(dataManager, "m2m_cascade_children", 1);
    }

    @Test
    public void testManyToManySetNullDeletesOnlyJoinEntries() throws SQLException {
        DataManager dataManager = load(ManyToManySetNullHolder.class);

        ManyToManySetNullHolder holder = ManyToManySetNullHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManySetNullChild child = ManyToManySetNullChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        holder.children.add(child);

        holder.delete();

        assertTrue(holder.isDeleted());
        assertFalse(child.isDeleted());
        assertTableRowCount(dataManager, "m2m_set_null_join", 0);
        assertTableRowCount(dataManager, "m2m_set_null_children", 1);
    }

    @Test
    public void testManyToManyNoActionRejectsLinkedHolderDeletion() throws SQLException {
        DataManager dataManager = load(ManyToManyNoActionHolder.class);

        ManyToManyNoActionHolder holder = ManyToManyNoActionHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManyNoActionChild child = ManyToManyNoActionChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        holder.children.add(child);

        assertThrows(RuntimeException.class, holder::delete);

        assertFalse(holder.isDeleted());
        assertFalse(child.isDeleted());
        assertTableRowCount(dataManager, "m2m_no_action_holders", 1);
        assertTableRowCount(dataManager, "m2m_no_action_children", 1);
        assertTableRowCount(dataManager, "m2m_no_action_join", 1);
    }

    @Test
    public void testManyToManyNoActionAllowsUnlinkedHolderDeletion() throws SQLException {
        DataManager dataManager = load(ManyToManyNoActionHolder.class);

        ManyToManyNoActionHolder holder = ManyToManyNoActionHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);

        holder.delete();

        assertTrue(holder.isDeleted());
        assertTableRowCount(dataManager, "m2m_no_action_holders", 0);
    }

    @Test
    public void testDeletingManyToManyChildAlwaysCleansJoinEntry() throws SQLException {
        DataManager dataManager = load(ManyToManyNoActionHolder.class);

        ManyToManyNoActionHolder holder = ManyToManyNoActionHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManyNoActionChild child = ManyToManyNoActionChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        holder.children.add(child);

        child.delete();

        assertFalse(holder.isDeleted());
        assertTrue(child.isDeleted());
        assertTableRowCount(dataManager, "m2m_no_action_join", 0);
    }

    @Test
    public void testManyToManyCascadeSupportsCompositeLinks() throws SQLException {
        DataManager dataManager = load(ManyToManyCompositeHolder.class);

        ManyToManyCompositeHolder holder = ManyToManyCompositeHolder.builder(dataManager)
                .tenantId(1)
                .childId(2)
                .insert(InsertMode.SYNC);
        ManyToManyCompositeChild child = ManyToManyCompositeChild.builder(dataManager)
                .groupId(1)
                .id(2)
                .insert(InsertMode.SYNC);
        holder.children.add(child);

        holder.delete();

        assertTrue(child.isDeleted());
        assertTableRowCount(dataManager, "m2m_composite_join", 0);
        assertTableRowCount(dataManager, "m2m_composite_children", 0);
    }

    @Test
    public void testManyToManyDeleteStrategiesWorkWithoutForeignKeys() throws SQLException {
        DataManager dataManager = load(
                ManyToManyNoForeignKeyCascadeHolder.class,
                ManyToManyNoForeignKeySetNullHolder.class,
                ManyToManyNoForeignKeyNoActionHolder.class
        );

        ManyToManyNoForeignKeyCascadeHolder cascadeHolder = ManyToManyNoForeignKeyCascadeHolder.builder(dataManager)
                .id(1)
                .insert(InsertMode.SYNC);
        ManyToManyNoForeignKeyChild cascadeChild = ManyToManyNoForeignKeyChild.builder(dataManager)
                .id(10)
                .insert(InsertMode.SYNC);
        cascadeHolder.children.add(cascadeChild);

        ManyToManyNoForeignKeySetNullHolder setNullHolder = ManyToManyNoForeignKeySetNullHolder.builder(dataManager)
                .id(2)
                .insert(InsertMode.SYNC);
        ManyToManyNoForeignKeyChild setNullChild = ManyToManyNoForeignKeyChild.builder(dataManager)
                .id(20)
                .insert(InsertMode.SYNC);
        setNullHolder.children.add(setNullChild);

        ManyToManyNoForeignKeyNoActionHolder noActionHolder = ManyToManyNoForeignKeyNoActionHolder.builder(dataManager)
                .id(3)
                .insert(InsertMode.SYNC);
        ManyToManyNoForeignKeyChild noActionChild = ManyToManyNoForeignKeyChild.builder(dataManager)
                .id(30)
                .insert(InsertMode.SYNC);
        noActionHolder.children.add(noActionChild);

        cascadeHolder.delete();
        setNullHolder.delete();
        noActionHolder.delete();

        assertTrue(cascadeChild.isDeleted());
        assertFalse(setNullChild.isDeleted());
        assertFalse(noActionChild.isDeleted());
        assertTableRowCount(dataManager, "m2m_no_fkey_cascade_join", 0);
        assertTableRowCount(dataManager, "m2m_no_fkey_set_null_join", 0);
        assertTableRowCount(dataManager, "m2m_no_fkey_no_action_join", 1);

        SQLSchema schema = dataManager.getSQLBuilder().getSchema("test");
        assertNotNull(schema);
        assertTrue(schema.getTable("m2m_no_fkey_cascade_join").getForeignKeys().isEmpty());
        assertTrue(schema.getTable("m2m_no_fkey_set_null_join").getForeignKeys().isEmpty());
        assertTrue(schema.getTable("m2m_no_fkey_no_action_join").getForeignKeys().isEmpty());
    }

    @Test
    public void testManyToManyDeleteStrategiesConfigureForeignKeys() {
        DataManager dataManager = load(
                ManyToManyCascadeHolder.class,
                ManyToManySetNullHolder.class,
                ManyToManyNoActionHolder.class
        );

        assertForeignKeyActions(dataManager, "m2m_cascade_join", "m2m_cascade_holders", "m2m_cascade_children", OnDelete.CASCADE);
        assertForeignKeyActions(dataManager, "m2m_set_null_join", "m2m_set_null_holders", "m2m_set_null_children", OnDelete.CASCADE);
        assertForeignKeyActions(dataManager, "m2m_no_action_join", "m2m_no_action_holders", "m2m_no_action_children", OnDelete.NO_ACTION);
    }

    @Test
    public void testManyToManyDeleteStrategiesCreateAppropriateTriggers() {
        DataManager dataManager = load(
                ManyToManyCascadeHolder.class,
                ManyToManySetNullHolder.class,
                ManyToManyNoActionHolder.class
        );

        SQLManyToManyDeleteStrategyTrigger cascadeTrigger = getManyToManyTrigger(dataManager, "m2m_cascade_holders");
        assertTrue(cascadeTrigger.getPgSQL().contains("BEFORE DELETE ON \"test\".\"m2m_cascade_holders\""));
        assertTrue(cascadeTrigger.getPgSQL().contains("DELETE FROM \"test\".\"m2m_cascade_children\""));
        assertTrue(cascadeTrigger.getH2SQL().contains("BEFORE DELETE ON \"test\".\"m2m_cascade_holders\""));

        SQLManyToManyDeleteStrategyTrigger setNullTrigger = getManyToManyTrigger(dataManager, "m2m_set_null_holders");
        assertTrue(setNullTrigger.getPgSQL().contains("BEFORE DELETE ON \"test\".\"m2m_set_null_holders\""));
        assertTrue(setNullTrigger.getPgSQL().contains("DELETE FROM \"test\".\"m2m_set_null_join\""));
        assertFalse(setNullTrigger.getPgSQL().contains("DELETE FROM \"test\".\"m2m_set_null_children\""));
        assertTrue(setNullTrigger.getH2SQL().startsWith("DROP TRIGGER IF EXISTS"));
        assertTrue(setNullTrigger.getH2SQL().contains("BEFORE DELETE ON \"test\".\"m2m_set_null_holders\""));

        SQLManyToManyDeleteStrategyTrigger noActionTrigger = getManyToManyTrigger(dataManager, "m2m_no_action_holders");
        assertTrue(noActionTrigger.getPgSQL().startsWith("DROP TRIGGER IF EXISTS"));
        assertTrue(noActionTrigger.getH2SQL().startsWith("DROP TRIGGER IF EXISTS"));
    }

    @Test
    public void testCompactH2ManyToManyTriggerSurvivesAddingColumns() throws SQLException {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        Connection h2Connection = getH2Connection(dataManager);
        dataManager.load(H2TriggerInitialHolder.class);

        assertCompactManyToManyTrigger(h2Connection);

        dataManager.load(H2TriggerMigrationHolder.class);
        dataManager.finishLoading();

        try (Statement statement = h2Connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                             + "WHERE TABLE_SCHEMA = 'mig' AND TABLE_NAME = 'holders' AND COLUMN_NAME = 'new_flag'"
             )) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1));
        }

        assertCompactManyToManyTrigger(h2Connection);

        H2TriggerMigrationHolder holder = H2TriggerMigrationHolder.builder(dataManager)
                .id(UUID.randomUUID())
                .newFlag(true)
                .insert(InsertMode.SYNC);
        H2TriggerMigrationTarget child = H2TriggerMigrationTarget.builder(dataManager)
                .id(UUID.randomUUID())
                .insert(InsertMode.SYNC);
        holder.targets.add(child);

        holder.delete();

        assertTrue(child.isDeleted());
    }

    private void assertCompactManyToManyTrigger(Connection h2Connection) throws SQLException {
        try (Statement statement = h2Connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS "
                             + "WHERE TRIGGER_SCHEMA = 'mig' AND EVENT_OBJECT_TABLE = 'holders' "
                             + "AND TRIGGER_NAME LIKE 'static_data_v3_m2m_%'"
             )) {
            assertTrue(resultSet.next());
            assertTrue(resultSet.getString(1).length() < 64);
            assertFalse(resultSet.next());
        }
    }

    @SafeVarargs
    private DataManager load(Class<? extends UniqueData>... classes) {
        DataManager dataManager = getMockEnvironments().getFirst().dataManager();
        dataManager.load(classes);
        dataManager.finishLoading();
        return dataManager;
    }

    private void assertTableRowCount(DataManager dataManager, String table, int expected) throws SQLException {
        assertEquals(expected, tableRowCount(getH2Connection(dataManager), table), "Unexpected H2 row count for " + table);
        dataManager.flushTaskQueue();
        assertEquals(expected, tableRowCount(getConnection(), table), "Unexpected PostgreSQL row count for " + table);
    }

    private int tableRowCount(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM \"test\".\"" + table + "\"")) {
            assertTrue(resultSet.next());
            return resultSet.getInt(1);
        }
    }

    private void assertForeignKeyActions(DataManager dataManager, String joinTableName, String holderTableName, String targetTableName, OnDelete holderAction) {
        SQLSchema schema = dataManager.getSQLBuilder().getSchema("test");
        assertNotNull(schema);
        SQLTable joinTable = schema.getTable(joinTableName);
        assertNotNull(joinTable);

        assertEquals(holderAction, findForeignKey(joinTable, holderTableName).getOnDelete());
        assertEquals(OnDelete.CASCADE, findForeignKey(joinTable, targetTableName).getOnDelete());
    }

    private ForeignKey findForeignKey(SQLTable table, String referencedTable) {
        return table.getForeignKeys().stream()
                .filter(foreignKey -> foreignKey.getReferencedTable().equals(referencedTable))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No foreign key from " + table.getName() + " to " + referencedTable));
    }

    private SQLManyToManyDeleteStrategyTrigger getManyToManyTrigger(DataManager dataManager, String holderTableName) {
        SQLSchema schema = dataManager.getSQLBuilder().getSchema("test");
        assertNotNull(schema);
        SQLTable holderTable = schema.getTable(holderTableName);
        assertNotNull(holderTable);
        SQLTrigger trigger = holderTable.getTriggers().stream()
                .filter(SQLManyToManyDeleteStrategyTrigger.class::isInstance)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No many-to-many deletion trigger for " + holderTableName));
        return (SQLManyToManyDeleteStrategyTrigger) trigger;
    }

    @Data(schema = "test", table = "user_metadata")
    static class UserMetadataSetNull extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "user_actions")
    static class UserActionSetNull extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "user_id", nullable = true)
        public PersistentValue<Integer> userId;
    }

    @Data(schema = "test", table = "users")
    static class UserSetNull extends UniqueData {

        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "metadata_id", nullable = true)
        public PersistentValue<Integer> metadataId;

        @OneToOne(link = "metadata_id=id")
        @Delete(DeleteStrategy.SET_NULL)
        public Reference<UserMetadataSetNull> ref;

        @OneToMany(link = "id=user_id")
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<UserActionSetNull> actions;
    }

    @Data(schema = "test", table = "user_metadata")
    static class UserMetadataCascade extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "user_actions")
    static class UserActionCascade extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "user_id", nullable = true)
        public PersistentValue<Integer> userId;
    }

    @Data(schema = "test", table = "users")
    static class UserCascade extends UniqueData {

        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @Column(name = "metadata_id", nullable = true)
        public PersistentValue<Integer> metadataId;

        @OneToOne(link = "metadata_id=id")
        @Delete(DeleteStrategy.CASCADE)
        public Reference<UserMetadataCascade> ref;

        @OneToMany(link = "id=user_id")
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<UserActionCascade> actions;
    }

    @Data(schema = "test", table = "m2m_cascade_children")
    static class ManyToManyCascadeChild extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "m2m_cascade_holders")
    static class ManyToManyCascadeHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_cascade_join")
        @Delete(DeleteStrategy.CASCADE)
        public PersistentCollection<ManyToManyCascadeChild> children;
    }

    @Data(schema = "test", table = "m2m_set_null_children")
    static class ManyToManySetNullChild extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "m2m_set_null_holders")
    static class ManyToManySetNullHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_set_null_join")
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<ManyToManySetNullChild> children;
    }

    @Data(schema = "test", table = "m2m_no_action_children")
    static class ManyToManyNoActionChild extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "m2m_no_action_holders")
    static class ManyToManyNoActionHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_no_action_join")
        @Delete(DeleteStrategy.NO_ACTION)
        public PersistentCollection<ManyToManyNoActionChild> children;
    }

    @Data(schema = "test", table = "m2m_composite_children")
    static class ManyToManyCompositeChild extends UniqueData {
        @IdColumn(name = "group_id")
        public PersistentValue<Integer> groupId;

        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "m2m_composite_holders")
    static class ManyToManyCompositeHolder extends UniqueData {
        @IdColumn(name = "tenant_id")
        public PersistentValue<Integer> tenantId;

        @IdColumn(name = "child_id")
        public PersistentValue<Integer> childId;

        @ManyToMany(link = "tenant_id=group_id, child_id=id", joinTable = "m2m_composite_join")
        @Delete(DeleteStrategy.CASCADE)
        public PersistentCollection<ManyToManyCompositeChild> children;
    }

    @Data(schema = "test", table = "m2m_no_fkey_children")
    static class ManyToManyNoForeignKeyChild extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;
    }

    @Data(schema = "test", table = "m2m_no_fkey_cascade_holders")
    static class ManyToManyNoForeignKeyCascadeHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_no_fkey_cascade_join", fkey = false)
        @Delete(DeleteStrategy.CASCADE)
        public PersistentCollection<ManyToManyNoForeignKeyChild> children;
    }

    @Data(schema = "test", table = "m2m_no_fkey_set_null_holders")
    static class ManyToManyNoForeignKeySetNullHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_no_fkey_set_null_join", fkey = false)
        @Delete(DeleteStrategy.SET_NULL)
        public PersistentCollection<ManyToManyNoForeignKeyChild> children;
    }

    @Data(schema = "test", table = "m2m_no_fkey_no_action_holders")
    static class ManyToManyNoForeignKeyNoActionHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<Integer> id;

        @ManyToMany(link = "id=id", joinTable = "m2m_no_fkey_no_action_join", fkey = false)
        @Delete(DeleteStrategy.NO_ACTION)
        public PersistentCollection<ManyToManyNoForeignKeyChild> children;
    }

    @Data(schema = "mig", table = "target_records_with_extended_name")
    static class H2TriggerMigrationTarget extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<UUID> id;
    }

    @Data(schema = "mig", table = "holders")
    static class H2TriggerInitialHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<UUID> id;

        @ManyToMany(link = "id=id", joinTable = "relationship_entries")
        @Delete(DeleteStrategy.CASCADE)
        public PersistentCollection<H2TriggerMigrationTarget> targets;
    }

    @Data(schema = "mig", table = "holders")
    static class H2TriggerMigrationHolder extends UniqueData {
        @IdColumn(name = "id")
        public PersistentValue<UUID> id;

        @DefaultValue("true")
        @Column(name = "new_flag")
        public PersistentValue<Boolean> newFlag;

        @ManyToMany(link = "id=id", joinTable = "relationship_entries")
        @Delete(DeleteStrategy.CASCADE)
        public PersistentCollection<H2TriggerMigrationTarget> targets;
    }
}
