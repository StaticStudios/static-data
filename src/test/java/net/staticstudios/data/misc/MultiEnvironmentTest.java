package net.staticstudios.data.misc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class MultiEnvironmentTest extends DataTest {

    private MockEnvironment environment1;
    private MockEnvironment environment2;

    @BeforeAll
    public static void setup() {
        NUM_ENVIRONMENTS = 2;
    }

    @BeforeEach
    public void setEnvironments() {
        this.environment1 = getMockEnvironments().getFirst();
        this.environment2 = getMockEnvironments().get(1);
    }
}