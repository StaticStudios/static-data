package net.staticstudios.data.messaging.handle;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.UniqueData;
import net.staticstudios.data.messaging.ForeignLinkUpdateMessage;
import net.staticstudios.data.meta.UniqueDataMetadata;
import net.staticstudios.data.meta.persistant.value.ForeignPersistentValueMetadata;
import net.staticstudios.data.value.ForeignPersistentValue;
import net.staticstudios.messaging.MessageHandler;
import net.staticstudios.messaging.MessageMetadata;

import java.sql.Connection;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ForeignLinkUpdateMessageHandler implements MessageHandler<ForeignLinkUpdateMessage> {

    private final DataManager dataManager;

    public ForeignLinkUpdateMessageHandler(DataManager dataManager) {
        this.dataManager = dataManager;
    }

    @Override
    public CompletableFuture<String> onMessage(MessageMetadata metadata, ForeignLinkUpdateMessage payload) {
        Collection<ForeignPersistentValueMetadata> foreignPersistentValueMetadata = dataManager.getForeignLinks(payload.linkingTable());

        if (foreignPersistentValueMetadata.isEmpty()) {
            DataManager.getLogger().warn("Received a ForeignLinkUpdateMessage for a table that does not exist: {}", payload.linkingTable());
            return null;
        }

        //todo: handle the case when values are unlinked, aka set to null


        for (ForeignPersistentValueMetadata fpvMeta : foreignPersistentValueMetadata) {
            UniqueDataMetadata uniqueDataMetadata = dataManager.getUniqueDataMetadata(fpvMeta.getParentClass());

            if (!fpvMeta.getLinkingTable().equals(payload.linkingTable())) {
                continue;
            }

            if (!fpvMeta.getThisLinkingColumn().equals(payload.column1()) && !fpvMeta.getThisLinkingColumn().equals(payload.column2())) {
                continue;
            }

            if (!fpvMeta.getForeignLinkingColumn().equals(payload.column1()) && !fpvMeta.getForeignLinkingColumn().equals(payload.column2())) {
                continue;
            }

            UUID dataId;
            UUID foreignId;

            if (payload.column1().equals(fpvMeta.getThisLinkingColumn())) {
                dataId = payload.id1();
                foreignId = payload.id2();
            } else {
                dataId = payload.id2();
                foreignId = payload.id1();
            }

            UniqueData data = uniqueDataMetadata.getProvider().get(dataId);

            if (data == null) {
                DataManager.getLogger().warn("Received a ForeignLinkUpdateMessage for a value that does not exist. FPV meta address: {}, Data id: {}, Data class: {} [{}]", fpvMeta.getMetadataAddress(), dataId, fpvMeta.getParentClass().getName(), dataManager.getServerId());
                continue;
            }

            ForeignPersistentValue<?> fpv = fpvMeta.getSharedValue(data);

            if (fpv == null) {
                DataManager.getLogger().warn("Received a ForeignLinkUpdateMessage for a value that does not exist: {} -> {} [{}]", dataId, foreignId, dataManager.getServerId());
                continue;
            }

            try (Connection connection = dataManager.getConnection()) {
                fpv.setInternalForeignObject(connection, foreignId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    @Override
    public boolean handleAsync() {
        return true;
    }

    @Override
    public boolean handleResponseAsync() {
        return true;
    }

    @Override
    public boolean shouldHandleOwnMessages() {
        return false;
    }
}
