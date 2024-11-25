package net.staticstudios.data.mocks;

import net.staticstudios.data.DataManager;
import net.staticstudios.data.DataProvider;

public class MockTestDataObjectProvider extends DataProvider<MockTestDataObject> {
    public MockTestDataObjectProvider(DataManager dataManager) {
        super(dataManager, MockTestDataObject.class);
    }
}
