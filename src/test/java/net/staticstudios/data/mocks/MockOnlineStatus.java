package net.staticstudios.data.mocks;

public enum MockOnlineStatus {
    ONLINE(true),
    OFFLINE(false);

    private final boolean online;

    MockOnlineStatus(boolean online) {
        this.online = online;
    }

    public static MockOnlineStatus fromBoolean(boolean online) {
        return online ? ONLINE : OFFLINE;
    }

    public boolean isOnline() {
        return online;
    }
}
